"""
SQL generation for time series window aggregation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, cast

from sqlglot import expressions
from sqlglot.expressions import Select, alias_, select

from featurebyte.enum import InternalName, SpecialColumnName
from featurebyte.query_graph.model.feature_job_setting import (
    CronFeatureJobSetting,
)
from featurebyte.query_graph.model.window import CalendarWindow
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.aggregator.base import (
    AggregationResult,
    LeftJoinableSubquery,
    NonTileBasedAggregator,
)
from featurebyte.query_graph.sql.aggregator.range_join import (
    LeftTable,
    RightTable,
    range_join_tables,
)
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    CteStatement,
    CteStatements,
    quoted_identifier,
)
from featurebyte.query_graph.sql.cron import get_request_table_with_job_schedule_name
from featurebyte.query_graph.sql.groupby_helper import GroupbyColumn, GroupbyKey, get_groupby_expr
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.specifications.time_series_window_aggregate import (
    TimeSeriesWindowAggregateSpec,
)

WindowType = CalendarWindow
OffsetType = Optional[CalendarWindow]
FeatureJobSettingsType = CronFeatureJobSetting
EntityIDsType = Tuple[str, ...]
RequestTableKeyType = Tuple[WindowType, OffsetType, FeatureJobSettingsType, EntityIDsType]


@dataclass
class ProcessedRequestTable:
    """
    Processed request table
    """

    name: str
    aggregation_spec: TimeSeriesWindowAggregateSpec


class TimeSeriesRequestTablePlan:
    """
    Responsible for processing request table to have distinct serving names and window ranges
    """

    def __init__(self, source_info: SourceInfo) -> None:
        self.processed_request_tables: Dict[RequestTableKeyType, ProcessedRequestTable] = {}
        self.adapter = get_sql_adapter(source_info)

    def add_aggregation_spec(self, aggregation_spec: TimeSeriesWindowAggregateSpec) -> None:
        """
        Update plan to account for the aggregation spec

        Parameters
        ----------
        aggregation_spec: TimeSeriesWindowAggregateSpec
            Aggregation spec
        """
        key = self._get_request_table_key(aggregation_spec)
        if key not in self.processed_request_tables:
            window_spec = f"W{aggregation_spec.window.to_string()}"
            if aggregation_spec.offset is not None:
                window_spec += f"_O{aggregation_spec.offset.to_string()}"
            feature_job_setting = aggregation_spec.parameters.feature_job_setting
            job_settings_str = feature_job_setting.get_cron_expression_with_timezone()
            table_name = (
                f"REQUEST_TABLE_TIME_SERIES"
                f"_{window_spec}"
                f"_{job_settings_str}"
                f"_{'_'.join(aggregation_spec.parameters.serving_names)}"
            )
            self.processed_request_tables[key] = ProcessedRequestTable(
                name=table_name,
                aggregation_spec=aggregation_spec,
            )

    @staticmethod
    def _get_request_table_key(
        aggregation_spec: TimeSeriesWindowAggregateSpec,
    ) -> RequestTableKeyType:
        params = aggregation_spec.parameters
        key = (
            aggregation_spec.window,
            aggregation_spec.offset,
            params.feature_job_setting,
            tuple(params.serving_names),
        )
        return key

    def get_processed_request_table(
        self, aggregation_spec: TimeSeriesWindowAggregateSpec
    ) -> ProcessedRequestTable:
        """
        Get the process request table corresponding to an aggregation spec

        Parameters
        ----------
        aggregation_spec: TimeSeriesWindowAggregateSpec
            Aggregation spec

        Returns
        -------
        ProcessedRequestTable
        """
        key = self._get_request_table_key(aggregation_spec)
        return self.processed_request_tables[key]

    def construct_request_table_ctes(self, request_table_name: str) -> CteStatements:
        """
        Get the CTEs for all the processed request tables

        Parameters
        ----------
        request_table_name: str
            Name of the input request table

        Returns
        -------
        CteStatements
        """
        request_table_ctes = []
        for processed_request_table in self.processed_request_tables.values():
            processed_table_sql = self._construct_processed_request_table_sql(
                request_table_name=request_table_name,
                aggregation_spec=processed_request_table.aggregation_spec,
            )
            request_table_ctes.append((
                quoted_identifier(processed_request_table.name),
                processed_table_sql,
            ))
        return cast(CteStatements, request_table_ctes)

    def _construct_processed_request_table_sql(
        self,
        request_table_name: str,
        aggregation_spec: TimeSeriesWindowAggregateSpec,
    ) -> Select:
        """
        Get a Select statement that applies necessary transformations to the request table to
        prepare for the aggregation.

        Parameters
        ----------
        request_table_name: str
            Request table name
        aggregation_spec: TimeSeriesWindowAggregateSpec
            Aggregation spec

        Returns
        -------
        Select
        """
        window_unit = aggregation_spec.window.unit
        job_datetime_rounded_to_window_unit = self.adapter.timestamp_truncate(
            quoted_identifier(InternalName.CRON_JOB_SCHEDULE_DATETIME),
            window_unit,
        )

        bucketed_offset_size = None
        if aggregation_spec.window.is_fixed_size():
            range_end_expr = self.adapter.to_epoch_seconds(job_datetime_rounded_to_window_unit)
            bucketed_window_size = aggregation_spec.window.to_seconds()
            if aggregation_spec.offset is not None:
                assert aggregation_spec.offset.is_fixed_size()
                bucketed_offset_size = aggregation_spec.offset.to_seconds()
        else:
            range_end_expr = self.adapter.to_epoch_months(job_datetime_rounded_to_window_unit)
            bucketed_window_size = aggregation_spec.window.to_months()
            if aggregation_spec.offset is not None:
                assert not aggregation_spec.offset.is_fixed_size()
                bucketed_offset_size = aggregation_spec.offset.to_months()

        if bucketed_offset_size is not None:
            range_end_expr = expressions.Sub(
                this=range_end_expr,
                expression=make_literal_value(bucketed_offset_size),
            )

        range_start_expr = expressions.Sub(
            this=range_end_expr,
            expression=make_literal_value(bucketed_window_size),
        )

        # Select distinct point in time values and entities
        request_table_name = get_request_table_with_job_schedule_name(
            request_table_name=request_table_name,
            feature_job_setting=aggregation_spec.parameters.feature_job_setting,
        )
        request_column_names = [
            SpecialColumnName.POINT_IN_TIME,
            *aggregation_spec.serving_names,
            InternalName.CRON_JOB_SCHEDULE_DATETIME,
        ]
        select_distinct_expr = (
            select(*[quoted_identifier(col) for col in request_column_names])
            .distinct()
            .from_(quoted_identifier(request_table_name))
        )
        processed_request_table = (
            select(*[quoted_identifier(col) for col in request_column_names]).select(
                alias_(range_start_expr, InternalName.WINDOW_START_EPOCH, quoted=True),
                alias_(range_end_expr, InternalName.WINDOW_END_EPOCH, quoted=True),
            )
        ).from_(select_distinct_expr.subquery())

        return processed_request_table


class TimeSeriesWindowAggregator(NonTileBasedAggregator[TimeSeriesWindowAggregateSpec]):
    """
    Aggregator responsible for generating SQL for time series window aggregation
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.request_table_plan = TimeSeriesRequestTablePlan(self.source_info)
        self.aggregation_source_views: Dict[str, Select] = {}

    def additional_update(self, aggregation_spec: TimeSeriesWindowAggregateSpec) -> None:
        self.request_table_plan.add_aggregation_spec(aggregation_spec)
        source_view_table_name = self.get_source_view_table_name(aggregation_spec)
        if source_view_table_name not in self.aggregation_source_views:
            self.aggregation_source_views[source_view_table_name] = (
                self.get_source_view_with_bucket_column(aggregation_spec)
            )

    @staticmethod
    def get_source_view_table_name(aggregation_spec: TimeSeriesWindowAggregateSpec) -> str:
        """
        Get the view name corresponding to the source of the aggregation

        Parameters
        ----------
        aggregation_spec: TimeSeriesWindowAggregateSpec
            Aggregation spec

        Returns
        -------
        str
        """
        return f"VIEW_{aggregation_spec.source_hash}"

    def get_source_view_with_bucket_column(
        self, aggregation_spec: TimeSeriesWindowAggregateSpec
    ) -> Select:
        """
        Get the source view augmented with timestamp converted to epoch seconds

        Parameters
        ----------
        aggregation_spec: TimeSeriesWindowAggregateSpec
            Aggregation spec

        Returns
        -------
        Select
        """
        reference_datetime_expr = quoted_identifier(
            aggregation_spec.parameters.reference_datetime_column
        )
        timestamp_schema = aggregation_spec.parameters.reference_datetime_schema
        if timestamp_schema.format_string is not None:
            reference_datetime_expr = self.adapter.to_timestamp_from_string(
                reference_datetime_expr, timestamp_schema.format_string
            )
        if aggregation_spec.window.is_fixed_size():
            bucket_expr = self.adapter.to_epoch_seconds(reference_datetime_expr)
        else:
            bucket_expr = self.adapter.to_epoch_months(reference_datetime_expr)
        return select(
            expressions.Star(),
            alias_(bucket_expr, alias=InternalName.VIEW_TIMESTAMP_EPOCH, quoted=True),
        ).from_(aggregation_spec.source_expr.subquery())

    def update_aggregation_table_expr(
        self,
        table_expr: Select,
        point_in_time_column: str,
        current_columns: list[str],
        current_query_index: int,
    ) -> AggregationResult:
        queries = []
        existing_columns: set[str] = set()
        for specs in self.grouped_specs.values():
            query = self._get_aggregation_subquery(specs, existing_columns)
            existing_columns.update(query.column_names)
            queries.append(query)

        return self._update_with_left_joins(
            table_expr=table_expr, current_query_index=current_query_index, queries=queries
        )

    def _get_aggregation_subquery(
        self, specs: list[TimeSeriesWindowAggregateSpec], existing_columns: set[str]
    ) -> LeftJoinableSubquery:
        spec = specs[0]

        # Left table: processed request table
        processed_request_table = self.request_table_plan.get_processed_request_table(spec)
        left_table = LeftTable(
            name=quoted_identifier(processed_request_table.name),
            alias="REQ",
            join_keys=spec.serving_names,
            range_start=InternalName.WINDOW_START_EPOCH,
            range_end=InternalName.WINDOW_END_EPOCH,
            columns=[SpecialColumnName.POINT_IN_TIME] + (spec.serving_names or []),
        )

        # Right table: source view
        source_view_table_name = self.get_source_view_table_name(spec)
        right_columns = set(
            agg_spec.parameters.parent
            for agg_spec in specs
            if agg_spec.parameters.parent is not None
        )
        if spec.parameters.value_by is not None:
            right_columns.add(spec.parameters.value_by)
        right_table = RightTable(
            name=quoted_identifier(source_view_table_name),
            alias="VIEW",
            join_keys=spec.parameters.keys[:],
            range_column=InternalName.VIEW_TIMESTAMP_EPOCH,
            columns=list(right_columns),
        )
        if spec.window.is_fixed_size():
            window_size = spec.window.to_seconds()
        else:
            window_size = spec.window.to_months()
        groupby_input_expr = range_join_tables(
            left_table=left_table,
            right_table=right_table,
            window_size=window_size,
        )

        # Aggregation
        groupby_keys = [
            GroupbyKey(
                expr=quoted_identifier(SpecialColumnName.POINT_IN_TIME),
                name=SpecialColumnName.POINT_IN_TIME,
            )
        ] + [
            GroupbyKey(
                expr=quoted_identifier(serving_name),
                name=serving_name,
            )
            for serving_name in spec.serving_names
        ]
        value_by = (
            GroupbyKey(
                expr=quoted_identifier(spec.parameters.value_by),
                name=spec.parameters.value_by,
            )
            if spec.parameters.value_by
            else None
        )
        groupby_columns = [
            GroupbyColumn(
                agg_func=s.parameters.agg_func,
                parent_expr=(
                    quoted_identifier(s.parameters.parent) if s.parameters.parent else None
                ),
                result_name=s.agg_result_name,
                parent_dtype=s.parent_dtype,
                parent_cols=(
                    [quoted_identifier(s.parameters.parent)] if s.parameters.parent else []
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

        # remove existing columns from the new columns
        column_names = set()
        for _spec in specs:
            if _spec.agg_result_name not in existing_columns:
                column_names.add(_spec.agg_result_name)

        return LeftJoinableSubquery(
            expr=aggregated_expr,
            column_names=list(column_names),
            join_keys=[SpecialColumnName.POINT_IN_TIME.value] + spec.serving_names,
        )

    def get_common_table_expressions(self, request_table_name: str) -> CteStatements:
        out: list[CteStatement] = []
        out.extend(self.request_table_plan.construct_request_table_ctes(request_table_name))
        for table_name, view_expr in self.aggregation_source_views.items():
            out.append((quoted_identifier(table_name), view_expr))
        return out