"""
SQL generation for forecast as-at aggregation
"""

from __future__ import annotations

from typing import Any, Optional

from sqlglot import expressions
from sqlglot.expressions import select

from featurebyte.enum import InternalName, SpecialColumnName
from featurebyte.query_graph.sql.aggregator.base import LeftJoinableSubquery
from featurebyte.query_graph.sql.aggregator.base_asat import BaseAsAtAggregator
from featurebyte.query_graph.sql.aggregator.helper import (
    join_aggregated_expr_with_distinct_point_in_time,
)
from featurebyte.query_graph.sql.aggregator.request_table import RequestTablePlan
from featurebyte.query_graph.sql.aggregator.snapshots_request_table import SnapshotsRequestTablePlan
from featurebyte.query_graph.sql.asat_helper import (
    ensure_end_timestamp_column,
    get_record_validity_condition,
)
from featurebyte.query_graph.sql.common import (
    CURRENT_TIMESTAMP_PLACEHOLDER,
    get_qualified_column_identifier,
    quoted_identifier,
)
from featurebyte.query_graph.sql.groupby_helper import GroupbyColumn, GroupbyKey, get_groupby_expr
from featurebyte.query_graph.sql.offset import (
    OffsetDirection,
    adjust_point_in_time_for_offset,
)
from featurebyte.query_graph.sql.specifications.forecast_aggregate_asat import (
    ForecastAggregateAsAtSpec,
)
from featurebyte.query_graph.sql.timestamp_helper import (
    apply_snapshot_adjustment,
    convert_forecast_point_to_utc,
)


class ForecastAsAtAggregator(BaseAsAtAggregator[ForecastAggregateAsAtSpec]):
    """
    ForecastAsAtAggregation is responsible for generating SQL for as-at aggregation
    that uses FORECAST_POINT instead of POINT_IN_TIME as the reference time.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.request_table_plan = RequestTablePlan(is_time_aware=True)
        self.snapshots_request_table_plan = SnapshotsRequestTablePlan(adapter=self.adapter)

    @classmethod
    def get_offset_direction(cls) -> OffsetDirection:
        return OffsetDirection.BACKWARD

    def _get_forecast_point_expr(
        self, spec: ForecastAggregateAsAtSpec, table_alias: str = "REQ"
    ) -> expressions.Expression:
        """
        Get the FORECAST_POINT expression, converted to UTC if necessary.

        Parameters
        ----------
        spec: ForecastAggregateAsAtSpec
            Aggregation spec
        table_alias: str
            Table alias for the request table

        Returns
        -------
        expressions.Expression
            FORECAST_POINT expression, potentially converted to UTC
        """
        forecast_point_expr = get_qualified_column_identifier(
            SpecialColumnName.FORECAST_POINT, table_alias
        )
        if spec.forecast_point_schema is not None:
            forecast_point_expr = convert_forecast_point_to_utc(
                forecast_point_expr,
                spec.forecast_point_schema,
                self.adapter,
            )
        return forecast_point_expr

    def _get_aggregation_subquery_from_scd_historical(
        self, specs: list[ForecastAggregateAsAtSpec]
    ) -> LeftJoinableSubquery:
        from typing import cast

        spec = specs[0]
        end_timestamp_column, scd_expr = ensure_end_timestamp_column(
            adapter=self.adapter,
            effective_timestamp_column=spec.parameters.effective_timestamp_column,
            effective_timestamp_schema=spec.parameters.effective_timestamp_schema,
            natural_key_column=cast(str, spec.parameters.natural_key_column),
            end_timestamp_column=spec.parameters.end_timestamp_column,
            source_expr=spec.source_expr,
        )

        if spec.parameters.keys:
            join_key_condition = expressions.and_(*[
                expressions.EQ(
                    this=get_qualified_column_identifier(serving_name, "REQ"),
                    expression=get_qualified_column_identifier(key, "SCD"),
                )
                for serving_name, key in zip(spec.serving_names, spec.parameters.keys)
            ])
        else:
            join_key_condition = expressions.true()

        # Use FORECAST_POINT (converted to UTC) for the temporal join
        forecast_point_expr = self._get_forecast_point_expr(spec)

        # Apply offset adjustment if any
        forecast_point_expr = adjust_point_in_time_for_offset(
            adapter=self.adapter,
            point_in_time_expr=forecast_point_expr,
            offset=spec.parameters.offset,
            offset_direction=self.get_offset_direction(),
        )

        # Only join records from the SCD table that are valid as at forecast point
        record_validity_condition = get_record_validity_condition(
            adapter=self.adapter,
            effective_timestamp_column=spec.parameters.effective_timestamp_column,
            effective_timestamp_schema=spec.parameters.effective_timestamp_schema,
            end_timestamp_column=end_timestamp_column,
            end_timestamp_schema=spec.parameters.end_timestamp_schema,
            point_in_time_expr=forecast_point_expr,
        )
        join_condition = expressions.and_(join_key_condition, record_validity_condition)

        # Groupby keys use FORECAST_POINT instead of POINT_IN_TIME
        groupby_keys = [
            GroupbyKey(
                expr=get_qualified_column_identifier(SpecialColumnName.FORECAST_POINT, "REQ"),
                name=SpecialColumnName.FORECAST_POINT,
            )
        ] + [
            GroupbyKey(
                expr=get_qualified_column_identifier(serving_name, "REQ"),
                name=serving_name,
            )
            for serving_name in spec.serving_names
        ]
        value_by = (
            GroupbyKey(
                expr=get_qualified_column_identifier(spec.parameters.value_by, "SCD"),
                name=spec.parameters.value_by,
            )
            if spec.parameters.value_by
            else None
        )
        groupby_columns = [
            GroupbyColumn(
                agg_func=s.parameters.agg_func,
                parent_expr=(
                    get_qualified_column_identifier(s.parameters.parent, "SCD")
                    if s.parameters.parent
                    else None
                ),
                result_name=s.agg_result_name,
                parent_dtype=s.parent_dtype,
                parent_cols=(
                    [get_qualified_column_identifier(s.parameters.parent, "SCD")]
                    if s.parameters.parent
                    else []
                ),
            )
            for s in specs
        ]
        groupby_input_expr = (
            select()
            .from_(
                expressions.Table(
                    this=quoted_identifier(self.request_table_plan.get_request_table_name(spec)),
                    alias="REQ",
                )
            )
            .join(
                scd_expr.subquery(alias="SCD"),
                join_type="inner",
                on=join_condition,
            )
        )
        aggregate_asat_expr = get_groupby_expr(
            input_expr=groupby_input_expr,
            groupby_keys=groupby_keys,
            groupby_columns=groupby_columns,
            value_by=value_by,
            adapter=self.adapter,
        )
        return LeftJoinableSubquery(
            expr=aggregate_asat_expr,
            column_names=[s.agg_result_name for s in specs],
            join_keys=[SpecialColumnName.FORECAST_POINT.value] + spec.serving_names,
        )

    def _get_aggregation_subquery_from_scd_deployment(
        self, specs: list[ForecastAggregateAsAtSpec]
    ) -> LeftJoinableSubquery:
        from typing import cast

        spec = specs[0]
        end_timestamp_column, scd_expr = ensure_end_timestamp_column(
            adapter=self.adapter,
            effective_timestamp_column=spec.parameters.effective_timestamp_column,
            effective_timestamp_schema=spec.parameters.effective_timestamp_schema,
            natural_key_column=cast(str, spec.parameters.natural_key_column),
            end_timestamp_column=spec.parameters.end_timestamp_column,
            source_expr=spec.source_expr,
        )

        # Use offset adjusted forecast point (converted to UTC) for deployment
        # In deployment mode, FORECAST_POINT comes from the request, not CURRENT_TIMESTAMP
        forecast_point_expr: expressions.Expression = CURRENT_TIMESTAMP_PLACEHOLDER
        if spec.forecast_point_schema is not None:
            forecast_point_expr = convert_forecast_point_to_utc(
                forecast_point_expr,
                spec.forecast_point_schema,
                self.adapter,
            )

        forecast_point_expr = adjust_point_in_time_for_offset(
            adapter=self.adapter,
            point_in_time_expr=forecast_point_expr,
            offset=spec.parameters.offset,
            offset_direction=self.get_offset_direction(),
        )

        # Only use records from the SCD table that are valid as at forecast point
        record_validity_condition = get_record_validity_condition(
            adapter=self.adapter,
            effective_timestamp_column=spec.parameters.effective_timestamp_column,
            effective_timestamp_schema=spec.parameters.effective_timestamp_schema,
            end_timestamp_column=end_timestamp_column,
            end_timestamp_schema=spec.parameters.end_timestamp_schema,
            point_in_time_expr=forecast_point_expr,
        )

        groupby_keys = [
            GroupbyKey(
                expr=get_qualified_column_identifier(key, "SCD"),
                name=serving_name,
            )
            for key, serving_name in zip(spec.parameters.keys, spec.serving_names)
        ]
        value_by = (
            GroupbyKey(
                expr=get_qualified_column_identifier(spec.parameters.value_by, "SCD"),
                name=spec.parameters.value_by,
            )
            if spec.parameters.value_by
            else None
        )
        groupby_columns = [
            GroupbyColumn(
                agg_func=s.parameters.agg_func,
                parent_expr=(
                    get_qualified_column_identifier(s.parameters.parent, "SCD")
                    if s.parameters.parent
                    else None
                ),
                result_name=s.agg_result_name,
                parent_dtype=s.parent_dtype,
                parent_cols=(
                    [get_qualified_column_identifier(s.parameters.parent, "SCD")]
                    if s.parameters.parent
                    else []
                ),
            )
            for s in specs
        ]
        groupby_input_expr = (
            select().from_(scd_expr.subquery(alias="SCD")).where(record_validity_condition)
        )
        aggregate_asat_expr = get_groupby_expr(
            input_expr=groupby_input_expr,
            groupby_keys=groupby_keys,
            groupby_columns=groupby_columns,
            value_by=value_by,
            adapter=self.adapter,
        )
        return LeftJoinableSubquery(
            expr=aggregate_asat_expr,
            column_names=[s.agg_result_name for s in specs],
            join_keys=spec.serving_names,
        )

    def _get_aggregation_subquery_from_snapshots_historical(
        self, specs: list[ForecastAggregateAsAtSpec]
    ) -> LeftJoinableSubquery:
        spec = specs[0]
        snapshots_parameters = spec.parameters.snapshots_parameters
        assert snapshots_parameters is not None
        snapshots_req = self.snapshots_request_table_plan.get_entry(
            snapshots_parameters, spec.serving_names
        )

        # Lookup the current snapshots for each distinct adjusted forecast point and perform
        # as-at aggregation
        groupby_input_expr = (
            select()
            .from_(
                expressions.Table(
                    this=quoted_identifier(snapshots_req.distinct_adjusted_point_in_time_table),
                    alias="REQ",
                )
            )
            .join(
                spec.source_expr.subquery(alias="SNAPSHOTS"),
                join_type="inner",
                on=expressions.and_(
                    expressions.EQ(
                        this=get_qualified_column_identifier(
                            InternalName.SNAPSHOTS_ADJUSTED_POINT_IN_TIME, "REQ"
                        ),
                        expression=get_qualified_column_identifier(
                            snapshots_parameters.snapshot_datetime_column, "SNAPSHOTS"
                        ),
                    ),
                    *[
                        expressions.EQ(
                            this=get_qualified_column_identifier(serving_name, "REQ"),
                            expression=get_qualified_column_identifier(
                                key,
                                "SNAPSHOTS",
                            ),
                        )
                        for serving_name, key in zip(spec.serving_names, spec.parameters.keys)
                    ],
                ),
            )
        )
        groupby_keys = [
            GroupbyKey(
                expr=get_qualified_column_identifier(
                    InternalName.SNAPSHOTS_ADJUSTED_POINT_IN_TIME, "REQ"
                ),
                name=InternalName.SNAPSHOTS_ADJUSTED_POINT_IN_TIME,
            )
        ] + [
            GroupbyKey(
                expr=get_qualified_column_identifier(serving_name, "REQ"),
                name=serving_name,
            )
            for serving_name in spec.serving_names
        ]
        value_by = (
            GroupbyKey(
                expr=get_qualified_column_identifier(spec.parameters.value_by, "SNAPSHOTS"),
                name=spec.parameters.value_by,
            )
            if spec.parameters.value_by
            else None
        )
        groupby_columns = [
            GroupbyColumn(
                agg_func=s.parameters.agg_func,
                parent_expr=(
                    get_qualified_column_identifier(s.parameters.parent, "SNAPSHOTS")
                    if s.parameters.parent
                    else None
                ),
                result_name=s.agg_result_name,
                parent_dtype=s.parent_dtype,
                parent_cols=(
                    [get_qualified_column_identifier(s.parameters.parent, "SNAPSHOTS")]
                    if s.parameters.parent
                    else []
                ),
            )
            for s in specs
        ]
        aggregate_asat_expr = get_groupby_expr(
            input_expr=groupby_input_expr,
            groupby_keys=groupby_keys,
            groupby_columns=groupby_columns,
            value_by=value_by,
            adapter=self.adapter,
        )

        # Map the aggregated results back to the original forecast point using the distinct point in
        # time to adjusted point in time table.
        column_names = set()
        for _spec in specs:
            column_names.add(_spec.agg_result_name)
        aggregated_column_names = sorted(column_names)
        aggregated_expr = join_aggregated_expr_with_distinct_point_in_time(
            aggregated_expr=aggregate_asat_expr,
            distinct_key=InternalName.SNAPSHOTS_ADJUSTED_POINT_IN_TIME.value,
            serving_names=spec.serving_names,
            aggregated_column_names=aggregated_column_names,
            distinct_by_point_in_time_table_name=snapshots_req.distinct_point_in_time_table,
        )

        # Use FORECAST_POINT in the join keys
        return LeftJoinableSubquery(
            expr=aggregated_expr,
            column_names=aggregated_column_names,
            join_keys=[SpecialColumnName.FORECAST_POINT.value] + spec.serving_names,
        )

    def _get_aggregation_subquery_from_snapshots_deployment(
        self, specs: list[ForecastAggregateAsAtSpec]
    ) -> LeftJoinableSubquery:
        spec = specs[0]
        snapshots_parameters = spec.parameters.snapshots_parameters
        assert snapshots_parameters is not None

        # For deployment, use the current timestamp as forecast point
        # Apply timezone conversion if needed
        forecast_point_expr: expressions.Expression = CURRENT_TIMESTAMP_PLACEHOLDER
        if spec.forecast_point_schema is not None:
            forecast_point_expr = convert_forecast_point_to_utc(
                forecast_point_expr,
                spec.forecast_point_schema,
                self.adapter,
            )

        # Lookup the current snapshots for the adjusted forecast point and perform as-at aggregation
        adjusted_forecast_point_expr = apply_snapshot_adjustment(
            datetime_expr=forecast_point_expr,
            time_interval=snapshots_parameters.time_interval,
            feature_job_setting=snapshots_parameters.feature_job_setting,
            format_string=snapshots_parameters.snapshot_timestamp_format_string,
            offset_size=snapshots_parameters.offset_size,
            adapter=self.adapter,
        )
        groupby_input_expr = (
            select()
            .from_(spec.source_expr.subquery(alias="SNAPSHOTS"))
            .where(
                expressions.EQ(
                    this=adjusted_forecast_point_expr,
                    expression=get_qualified_column_identifier(
                        snapshots_parameters.snapshot_datetime_column, "SNAPSHOTS"
                    ),
                )
            )
        )
        groupby_keys = [
            GroupbyKey(
                expr=get_qualified_column_identifier(key, "SNAPSHOTS"),
                name=serving_name,
            )
            for key, serving_name in zip(spec.parameters.keys, spec.serving_names)
        ]
        value_by = (
            GroupbyKey(
                expr=get_qualified_column_identifier(spec.parameters.value_by, "SNAPSHOTS"),
                name=spec.parameters.value_by,
            )
            if spec.parameters.value_by
            else None
        )
        groupby_columns = [
            GroupbyColumn(
                agg_func=s.parameters.agg_func,
                parent_expr=(
                    get_qualified_column_identifier(s.parameters.parent, "SNAPSHOTS")
                    if s.parameters.parent
                    else None
                ),
                result_name=s.agg_result_name,
                parent_dtype=s.parent_dtype,
                parent_cols=(
                    [get_qualified_column_identifier(s.parameters.parent, "SNAPSHOTS")]
                    if s.parameters.parent
                    else []
                ),
            )
            for s in specs
        ]
        aggregated_expr = get_groupby_expr(
            input_expr=groupby_input_expr,
            groupby_keys=groupby_keys,
            groupby_columns=groupby_columns,
            value_by=value_by,
            adapter=self.adapter,
        )

        column_names = set()
        for _spec in specs:
            column_names.add(_spec.agg_result_name)
        aggregated_column_names = sorted(column_names)

        return LeftJoinableSubquery(
            expr=aggregated_expr,
            column_names=aggregated_column_names,
            join_keys=spec.serving_names,
        )

    def get_deployment_feature_subquery_from_specs(
        self, specs: list[ForecastAggregateAsAtSpec]
    ) -> Optional[LeftJoinableSubquery]:
        spec = specs[0]
        if spec.parameters.snapshots_parameters is None:
            return self._get_aggregation_subquery_from_scd_deployment(specs)
        return self._get_aggregation_subquery_from_snapshots_deployment(specs)
