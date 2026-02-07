"""
SQL generation for lookup features
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Iterable, Optional, Sequence, Tuple, TypeVar, cast

import pandas as pd
from sqlglot import expressions
from sqlglot.expressions import Select, alias_, select

from featurebyte.enum import SpecialColumnName
from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node.generic import SCDLookupParameters
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.aggregator.base import (
    AggregationResult,
    Aggregator,
    CommonTable,
    LeftJoinableSubquery,
)
from featurebyte.query_graph.sql.asat_helper import (
    ensure_end_timestamp_column,
    get_record_validity_condition,
)
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    CURRENT_TIMESTAMP_PLACEHOLDER,
    get_qualified_column_identifier,
    quoted_identifier,
)
from featurebyte.query_graph.sql.cron import get_request_table_job_datetime_column_name
from featurebyte.query_graph.sql.deduplication import get_deduplicated_expr
from featurebyte.query_graph.sql.groupby_helper import (
    GroupbyColumn,
    GroupbyKey,
    InternalAggFunc,
    get_groupby_expr,
)
from featurebyte.query_graph.sql.offset import (
    OffsetDirection,
    adjust_point_in_time_for_offset,
)
from featurebyte.query_graph.sql.scd_helper import Table
from featurebyte.query_graph.sql.specifications.base_lookup import BaseLookupSpec
from featurebyte.query_graph.sql.timestamp_helper import (
    apply_snapshot_adjustment,
    convert_forecast_point_to_utc,
)


class SubqueryWithPointInTimeCutoff(LeftJoinableSubquery):
    """
    SubqueryWithPointInTimeCutoff for lookup features
    """

    event_timestamp_column: Optional[str]
    forward_point_in_time_offset: Optional[str]
    adapter: BaseAdapter
    use_forecast_point: bool = False
    forecast_point_schema: Optional[ForecastPointSchema] = None

    def get_expression_for_column(
        self,
        main_alias: str,
        join_alias: str,
        column_name: str,
        adapter: BaseAdapter,
    ) -> expressions.Expression:
        expr = super().get_expression_for_column(
            main_alias=main_alias,
            join_alias=join_alias,
            column_name=column_name,
            adapter=adapter,
        )

        # For lookup from EventData, set the looked up value to NA if the point in time is prior to
        # the event timestamp
        if self.event_timestamp_column is not None:
            # Use FORECAST_POINT for forecast targets, POINT_IN_TIME otherwise
            datetime_column = (
                SpecialColumnName.FORECAST_POINT
                if self.use_forecast_point
                else SpecialColumnName.POINT_IN_TIME
            )
            point_in_time_expr = get_qualified_column_identifier(
                datetime_column,
                main_alias,
            )
            # Convert FORECAST_POINT to UTC if schema is available
            if self.use_forecast_point and self.forecast_point_schema is not None:
                point_in_time_expr = convert_forecast_point_to_utc(
                    point_in_time_expr,
                    self.forecast_point_schema,
                    adapter,
                )

            # Add the forward point in time offset to the point in time if it is present.
            if self.forward_point_in_time_offset is not None:
                point_in_time_expr = self.adapter.dateadd_microsecond(
                    make_literal_value(
                        pd.Timedelta(self.forward_point_in_time_offset).total_seconds() * 1e6
                    ),
                    point_in_time_expr,
                )

            point_in_time_expr = adapter.normalize_timestamp_before_comparison(point_in_time_expr)
            event_timestamp_expr = adapter.normalize_timestamp_before_comparison(
                get_qualified_column_identifier(
                    self.event_timestamp_column,
                    join_alias,
                    quote_table=True,
                )
            )
            is_point_in_time_prior_to_event_timestamp = expressions.LT(
                this=point_in_time_expr, expression=event_timestamp_expr
            )
            if_expr = expressions.If(
                this=is_point_in_time_prior_to_event_timestamp, true=expressions.null()
            )
            expr = expressions.Case(ifs=[if_expr], default=expr)

        return expr


LookupSpecT = TypeVar("LookupSpecT", bound=BaseLookupSpec)


class BaseLookupAggregator(Aggregator[LookupSpecT]):
    """
    LookupAggregator is responsible for generating SQL for lookup features
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.is_parent_lookup = False

    @property
    def lookup_specs(self) -> Iterable[LookupSpecT]:
        """
        Yields a list of LookupSpec recorded

        Yields
        ------
        LookupSpec
            Instance of LookupSpec
        """
        for specs in self.grouped_specs.values():
            yield from specs

    def additional_update(self, aggregation_spec: LookupSpecT) -> None:
        if aggregation_spec.is_parent_lookup:
            self.is_parent_lookup = True

    def iterate_grouped_lookup_specs(self, is_scd: bool) -> Iterable[list[LookupSpecT]]:
        """
        Iterate over groups of LookupSpec filtering by time awareness. All the LookupSpecs in a
        group can be looked up using the same join.

        Parameters
        ----------
        is_scd: bool
            If true, only yields LookupSpecs that require SCD join, and vice versa

        Yields
        ------
        list[LookupSpec]
            Group of LookupSpec as a list
        """
        for specs in self.grouped_specs.values():
            scd_parameters = specs[0].scd_parameters
            if scd_parameters:
                if specs[0].aggregation_source.is_scd_filtered_by_current_flag:
                    # Online serving can be simplified to an exact join
                    requires_scd_join = False
                else:
                    # Must perform SCD join when: computing historical features, current flag column
                    # is not available, or there is offset configured for the lookup feature. This
                    # is already determined at the point of creating the LookupSpec.
                    requires_scd_join = True
            else:
                requires_scd_join = False

            if is_scd and requires_scd_join:
                yield specs
            if not is_scd and not requires_scd_join:
                yield specs

    def get_forward_point_in_time_offset(self, base_lookup_spec: LookupSpecT) -> Optional[str]:
        """
        Get the forward point in time offset for the lookup if it is provided.

        Parameters
        ----------
        base_lookup_spec: LookupSpecT
            LookupSpec

        Returns
        -------
        Optional[str]
        """
        _ = base_lookup_spec
        return None

    def get_direct_lookups(self) -> Sequence[LeftJoinableSubquery]:
        """
        Get simple lookup queries without time based conditions

        This includes SCD lookups during online serving when the current flag column is available.

        Returns
        -------
        list[LeftJoinableSubquery]
        """

        out = []

        for specs in self.iterate_grouped_lookup_specs(is_scd=False):
            entity_column = specs[0].entity_column
            serving_name = specs[0].serving_names[0]
            source_expr = specs[0].source_expr

            snapshots_parameters = specs[0].snapshots_parameters
            if snapshots_parameters is None:
                join_keys = [serving_name]
                left_table_join_keys = None
            else:
                join_keys = [
                    snapshots_parameters.snapshot_datetime_column,
                    serving_name,
                ]
                if specs[0].is_target:
                    # No need to apply blind spot when adjusting for snapshots when computing target
                    feature_job_setting = None
                else:
                    feature_job_setting = snapshots_parameters.feature_job_setting
                if snapshots_parameters.feature_job_setting is None or specs[0].is_deployment_sql:
                    # Use FORECAST_POINT for forecast targets, POINT_IN_TIME otherwise
                    datetime_column = (
                        SpecialColumnName.FORECAST_POINT
                        if specs[0].use_forecast_point
                        else SpecialColumnName.POINT_IN_TIME
                    )
                    datetime_expr_to_adjust = get_qualified_column_identifier(
                        datetime_column, "REQ"
                    )
                    # Convert FORECAST_POINT to UTC if schema is available
                    if specs[0].use_forecast_point and specs[0].forecast_point_schema is not None:
                        datetime_expr_to_adjust = convert_forecast_point_to_utc(
                            datetime_expr_to_adjust,
                            specs[0].forecast_point_schema,
                            self.adapter,
                        )
                else:
                    job_datetime_column_name = get_request_table_job_datetime_column_name(
                        snapshots_parameters.feature_job_setting, self.adapter.source_type
                    )
                    datetime_expr_to_adjust = get_qualified_column_identifier(
                        job_datetime_column_name, "REQ"
                    )
                adjusted_point_in_time_expr = apply_snapshot_adjustment(
                    datetime_expr=datetime_expr_to_adjust,
                    time_interval=snapshots_parameters.time_interval,
                    feature_job_setting=feature_job_setting,
                    format_string=snapshots_parameters.snapshot_timestamp_format_string,
                    offset_size=snapshots_parameters.offset_size,
                    offset_direction=(
                        OffsetDirection.FORWARD if specs[0].is_target else OffsetDirection.BACKWARD
                    ),
                    adapter=self.adapter,
                )
                left_table_join_keys = [
                    adjusted_point_in_time_expr,
                    get_qualified_column_identifier(serving_name, "REQ"),
                ]

            agg_expr = select(
                alias_(quoted_identifier(entity_column), alias=serving_name, quoted=True),
                *(
                    [quoted_identifier(snapshots_parameters.snapshot_datetime_column)]
                    if snapshots_parameters
                    else []
                ),
                *[
                    alias_(
                        quoted_identifier(spec.input_column_name),
                        alias=spec.agg_result_name,
                        quoted=True,
                    )
                    for spec in specs
                ],
            ).from_(source_expr.subquery())

            if specs[0].event_parameters is not None:
                event_timestamp_column = specs[0].event_parameters.event_timestamp_column
                agg_expr = agg_expr.select(quoted_identifier(event_timestamp_column))
            else:
                event_timestamp_column = None

            agg_expr = get_deduplicated_expr(self.adapter, agg_expr, join_keys)
            result = SubqueryWithPointInTimeCutoff(
                expr=agg_expr,
                column_names=[spec.agg_result_name for spec in specs],
                join_keys=join_keys,
                left_table_join_keys=left_table_join_keys,
                event_timestamp_column=event_timestamp_column,
                forward_point_in_time_offset=self.get_forward_point_in_time_offset(specs[0]),
                adapter=self.adapter,
                use_forecast_point=specs[0].use_forecast_point,
                forecast_point_schema=specs[0].forecast_point_schema,
            )
            out.append(result)

        return out

    def get_scd_lookups_deployment_sql(self) -> Sequence[LeftJoinableSubquery]:
        """
        Get SCD lookup queries for deployment SQL

        Returns
        -------
        Sequence[LeftJoinableSubquery]
            List of subqueries for SCD lookups
        """
        out = []
        for specs in self.iterate_grouped_lookup_specs(is_scd=True):
            out.append(self._get_scd_lookup_subquery_deployment_sql(specs))
        return out

    def _get_scd_lookup_subquery_deployment_sql(
        self, specs: list[LookupSpecT]
    ) -> LeftJoinableSubquery:
        spec = specs[0]
        scd_parameters = spec.scd_parameters
        assert scd_parameters is not None
        end_timestamp_column, scd_expr = ensure_end_timestamp_column(
            adapter=self.adapter,
            effective_timestamp_column=scd_parameters.effective_timestamp_column,
            effective_timestamp_schema=scd_parameters.effective_timestamp_schema,
            natural_key_column=cast(str, scd_parameters.natural_key_column),
            end_timestamp_column=scd_parameters.end_timestamp_column,
            source_expr=spec.source_expr,
        )

        # Use offset adjusted point in time to join with SCD table if any
        point_in_time_expr = adjust_point_in_time_for_offset(
            adapter=self.adapter,
            point_in_time_expr=CURRENT_TIMESTAMP_PLACEHOLDER,
            offset=scd_parameters.offset,
            offset_direction=OffsetDirection.BACKWARD,
        )

        # Only use records from the SCD table that are valid as at point in time
        record_validity_condition = get_record_validity_condition(
            adapter=self.adapter,
            effective_timestamp_column=scd_parameters.effective_timestamp_column,
            effective_timestamp_schema=scd_parameters.effective_timestamp_schema,
            end_timestamp_column=end_timestamp_column,
            end_timestamp_schema=scd_parameters.end_timestamp_schema,
            point_in_time_expr=point_in_time_expr,
        )

        groupby_keys = [
            GroupbyKey(
                expr=get_qualified_column_identifier(spec.entity_column, "SCD"),
                name=spec.serving_names[0],
            )
        ]
        groupby_columns = [
            GroupbyColumn(
                agg_func=InternalAggFunc.ANY,
                parent_expr=get_qualified_column_identifier(s.input_column_name, "SCD"),
                result_name=s.agg_result_name,
                parent_dtype=None,
                parent_cols=[get_qualified_column_identifier(s.input_column_name, "SCD")],
            )
            for s in specs
        ]
        groupby_input_expr = (
            select().from_(scd_expr.subquery(alias="SCD")).where(record_validity_condition)
        )
        scd_lookup_expr = get_groupby_expr(
            input_expr=groupby_input_expr,
            groupby_keys=groupby_keys,
            groupby_columns=groupby_columns,
            value_by=None,
            adapter=self.adapter,
        )
        return LeftJoinableSubquery(
            expr=scd_lookup_expr,
            column_names=[s.agg_result_name for s in specs],
            join_keys=spec.serving_names,
        )

    def get_deployment_feature_subquery_from_specs(
        self, specs: list[LookupSpecT]
    ) -> Optional[LeftJoinableSubquery]:
        spec = specs[0]
        if spec.scd_parameters is not None:
            return self._get_scd_lookup_subquery_deployment_sql(specs)
        # Snapshots lookup is still referencing the request table. This can be improved
        # subsequently if needed.
        return None

    def update_aggregation_table_expr(
        self,
        table_expr: Select,
        point_in_time_column: str,
        current_columns: list[str],
        current_query_index: int,
    ) -> AggregationResult:
        # SCD lookup
        queries: list[LeftJoinableSubquery] = []
        if not self.is_deployment_sql:
            # Union with request table for historical / batch sql
            table_expr, scd_agg_result_names = self._update_with_scd_lookups(
                table_expr=table_expr,
                point_in_time_column=point_in_time_column,
                current_columns=current_columns,
            )
        else:
            # For deployment sql, use a different query without union with request table
            queries.extend(self.get_scd_lookups_deployment_sql())
            scd_agg_result_names = []

        # Non-time based lookup
        queries.extend(self.get_direct_lookups())
        result = self._update_with_left_joins(
            table_expr=table_expr, current_query_index=current_query_index, queries=queries
        )

        # Update result column names to account for both types of lookups
        result.column_names = scd_agg_result_names + result.column_names

        if self.is_parent_lookup and not scd_agg_result_names:
            # In case of looking up parent entities, wrap the result in a subquery so that the newly
            # joined column is available under the REQ table qualifier for subsequent joins. This is
            # already done for the SCD case.
            result.updated_table_expr = self._wrap_in_nested_query(
                table_expr=result.updated_table_expr, columns=current_columns + result.column_names
            )

        return result

    @abstractmethod
    def get_scd_join_expr_for_lookup(
        self, left_table: Table, right_table: Table, scd_parameters: SCDLookupParameters
    ) -> Select:
        """
        Returns the SQL expression for the SCD join

        Parameters
        ----------
        left_table: Table
            The left table to join
        right_table: Table
            The right table to join
        scd_parameters: SCDLookupParameters
            The SCD lookup parameters

        Returns
        -------
        Select
            The SQL expression for the SCD join
        """

    def _update_with_scd_lookups(
        self,
        table_expr: Select,
        point_in_time_column: str,
        current_columns: list[str],
    ) -> Tuple[Select, list[str]]:
        """
        Generates sql for SCD lookup and returns the updated table and added columns

        Parameters
        ----------
        table_expr: Select
            The table expression to update
        point_in_time_column: str
            Point in time column name
        current_columns: list[str]
            List of column names in the table

        Returns
        -------
        Tuple[Select, list[str]]
            First element is the updated table_expr. Second element is the new columns added
        """

        scd_agg_result_names = []

        for lookup_specs in self.iterate_grouped_lookup_specs(is_scd=True):
            # Use FORECAST_POINT for forecast targets, POINT_IN_TIME otherwise
            timestamp_column = (
                SpecialColumnName.FORECAST_POINT
                if lookup_specs[0].use_forecast_point
                else point_in_time_column
            )
            # Create timestamp schema from ForecastPointSchema for timezone conversion
            left_timestamp_schema: Optional[TimestampSchema] = None
            forecast_point_schema = lookup_specs[0].forecast_point_schema
            if lookup_specs[0].use_forecast_point and forecast_point_schema is not None:
                left_timestamp_schema = TimestampSchema(
                    format_string=forecast_point_schema.format_string,
                    is_utc_time=forecast_point_schema.is_utc_time,
                    timezone=forecast_point_schema.timezone,
                )
            left_table = Table(
                expr=table_expr,
                timestamp_column=timestamp_column,
                timestamp_schema=left_timestamp_schema,
                join_keys=[lookup_specs[0].serving_names[0]],
                input_columns=current_columns,
                output_columns=current_columns,
            )

            agg_result_names = [spec.agg_result_name for spec in lookup_specs]
            scd_parameters = lookup_specs[0].scd_parameters
            assert scd_parameters is not None
            right_table = Table(
                expr=lookup_specs[0].source_expr,
                timestamp_column=scd_parameters.effective_timestamp_column,
                timestamp_schema=scd_parameters.effective_timestamp_schema,
                join_keys=[lookup_specs[0].entity_column],
                input_columns=[spec.input_column_name for spec in lookup_specs],
                output_columns=agg_result_names,
                end_timestamp_column=scd_parameters.end_timestamp_column,
                end_timestamp_schema=(
                    scd_parameters.end_timestamp_metadata.timestamp_schema
                    if scd_parameters.end_timestamp_metadata
                    else None
                ),
            )
            table_expr = self.get_scd_join_expr_for_lookup(
                left_table,
                right_table,
                scd_parameters,
            )

            current_columns = current_columns + agg_result_names
            scd_agg_result_names.extend(agg_result_names)

        if scd_agg_result_names:
            table_expr = self._wrap_in_nested_query(table_expr=table_expr, columns=current_columns)

        return table_expr, scd_agg_result_names

    def get_common_table_expressions(self, request_table_name: str) -> list[CommonTable]:
        _ = request_table_name
        return []
