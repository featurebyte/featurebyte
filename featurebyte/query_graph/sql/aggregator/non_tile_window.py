"""
SQL generation for non-tile window aggregation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from sqlglot import expressions
from sqlglot.expressions import Select, alias_, select

from featurebyte.enum import InternalName, SpecialColumnName
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.aggregator.base import (
    AggregationResult,
    CommonTable,
    LeftJoinableSubquery,
    NonTileBasedAggregator,
)
from featurebyte.query_graph.sql.aggregator.helper import (
    join_aggregated_expr_with_distinct_point_in_time,
)
from featurebyte.query_graph.sql.aggregator.range_join import (
    LeftTable,
    RightTable,
    range_join_tables,
)
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.feature_job import get_previous_job_epoch_expr_from_settings
from featurebyte.query_graph.sql.groupby_helper import GroupbyColumn, GroupbyKey, get_groupby_expr
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.specifications.non_tile_window_aggregate import (
    NonTileWindowAggregateSpec,
)
from featurebyte.query_graph.sql.timestamp_helper import convert_timestamp_to_utc

WindowType = int
OffsetType = Optional[int]
FeatureJobSettingsType = FeatureJobSetting
EntityIDsType = Tuple[str, ...]
RequestTableKeyType = Tuple[WindowType, OffsetType, FeatureJobSettingsType, EntityIDsType]


@dataclass
class ProcessedRequestTable:
    """
    Processed request table
    """

    name: str


@dataclass
class ProcessedRequestTablePair:
    """
    Processed request table pair
    """

    distinct_by_point_in_time: ProcessedRequestTable
    distinct_by_scheduled_job_time: ProcessedRequestTable
    aggregation_spec: NonTileWindowAggregateSpec


class NonTileRequestTablePlan:
    """
    Responsible for processing request table to have distinct serving names and window ranges
    """

    def __init__(self, source_info: SourceInfo) -> None:
        self.processed_request_tables: Dict[RequestTableKeyType, ProcessedRequestTablePair] = {}
        self.adapter = get_sql_adapter(source_info)

    def add_aggregation_spec(self, aggregation_spec: NonTileWindowAggregateSpec) -> None:
        """
        Update plan to account for the aggregation spec

        Parameters
        ----------
        aggregation_spec: NonTileWindowAggregateSpec
            Aggregation spec
        """
        key = self._get_request_table_key(aggregation_spec)
        if key not in self.processed_request_tables:
            window_spec = f"W{aggregation_spec.window}"
            if aggregation_spec.offset is not None:
                window_spec += f"_O{aggregation_spec.offset}"
            job_settings = aggregation_spec.parameters.feature_job_setting
            table_name = (
                f"REQUEST_TABLE_NO_TILE"
                f"_{window_spec}"
                f"_F{job_settings.period_seconds}"
                f"_BS{job_settings.blind_spot_seconds}"
                f"_M{job_settings.offset_seconds}"
                f"_{'_'.join(aggregation_spec.parameters.serving_names)}"
            )
            self.processed_request_tables[key] = ProcessedRequestTablePair(
                distinct_by_point_in_time=ProcessedRequestTable(
                    name=table_name + "_DISTINCT_BY_POINT_IN_TIME",
                ),
                distinct_by_scheduled_job_time=ProcessedRequestTable(
                    name=table_name + "_DISTINCT_BY_SCHEDULED_JOB_TIME",
                ),
                aggregation_spec=aggregation_spec,
            )

    @staticmethod
    def _get_request_table_key(aggregation_spec: NonTileWindowAggregateSpec) -> RequestTableKeyType:
        params = aggregation_spec.parameters
        key = (
            aggregation_spec.window,
            aggregation_spec.offset,
            params.feature_job_setting,
            tuple(params.serving_names),
        )
        return key

    def get_processed_request_tables(
        self, aggregation_spec: NonTileWindowAggregateSpec
    ) -> ProcessedRequestTablePair:
        """
        Get the process request table corresponding to an aggregation spec

        Parameters
        ----------
        aggregation_spec: NonTileWindowAggregateSpec
            Aggregation spec

        Returns
        -------
        ProcessedRequestTable
        """
        key = self._get_request_table_key(aggregation_spec)
        return self.processed_request_tables[key]

    def construct_request_table_ctes(self, request_table_name: str) -> list[CommonTable]:
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
        for processed_request_table_pair in self.processed_request_tables.values():
            processed_tables = self._construct_processed_request_table_sql(
                request_table_name=request_table_name,
                processed_request_table_pair=processed_request_table_pair,
            )
            request_table_ctes.extend(processed_tables)
        return request_table_ctes

    def _construct_processed_request_table_sql(
        self,
        request_table_name: str,
        processed_request_table_pair: ProcessedRequestTablePair,
    ) -> list[CommonTable]:
        """
        Get a Select statement that applies necessary transformations to the request table to
        prepare for the aggregation.

        Parameters
        ----------
        request_table_name: str
            Request table name
        processed_request_table_pair: ProcessedRequestTablePair
            Processed request table pair

        Returns
        -------
        list[CteStatement]
        """
        aggregation_spec = processed_request_table_pair.aggregation_spec

        # Add window start and window end columns to prepare for join
        point_in_time_epoch_expr = self.adapter.to_epoch_seconds(
            quoted_identifier(SpecialColumnName.POINT_IN_TIME)
        )
        feature_job_settings = aggregation_spec.parameters.feature_job_setting
        job_epoch_expr = get_previous_job_epoch_expr_from_settings(
            point_in_time_epoch_expr=point_in_time_epoch_expr,
            period_seconds=feature_job_settings.period_seconds,
            offset_seconds=feature_job_settings.offset_seconds,
        )

        request_table_with_job_epoch = select(
            quoted_identifier(SpecialColumnName.POINT_IN_TIME),
            *[quoted_identifier(col) for col in aggregation_spec.serving_names],
            alias_(job_epoch_expr, alias=InternalName.JOB_SCHEDULE_EPOCH, quoted=True),
        ).from_(quoted_identifier(request_table_name))

        range_end_expr = expressions.Sub(
            this=quoted_identifier(InternalName.JOB_SCHEDULE_EPOCH),
            expression=make_literal_value(feature_job_settings.blind_spot_seconds),
        )
        range_start_expr = expressions.Sub(
            this=range_end_expr,
            expression=make_literal_value(aggregation_spec.window),
        )

        # Select distinct point in time values and entities
        request_column_names = [
            SpecialColumnName.POINT_IN_TIME,
            *aggregation_spec.serving_names,
            InternalName.JOB_SCHEDULE_EPOCH,
        ]
        point_in_time_distinct_expr = (
            select(*[quoted_identifier(col) for col in request_column_names])
            .distinct()
            .from_(request_table_with_job_epoch.subquery())
        )

        # Select distinct feature job times and entities
        column_names_without_point_in_time = [
            col for col in request_column_names if col != SpecialColumnName.POINT_IN_TIME
        ]
        scheduled_job_time_distinct_expr = (
            select(
                *[quoted_identifier(col) for col in column_names_without_point_in_time],
            )
            .distinct()
            .from_(request_table_with_job_epoch.subquery())
        )
        scheduled_job_time_distinct_expr = (
            select(
                *[quoted_identifier(col) for col in column_names_without_point_in_time],
                alias_(range_start_expr, InternalName.WINDOW_START_EPOCH, quoted=True),
                alias_(range_end_expr, InternalName.WINDOW_END_EPOCH, quoted=True),
            )
        ).from_(scheduled_job_time_distinct_expr.subquery())

        return [
            CommonTable(
                name=processed_request_table_pair.distinct_by_point_in_time.name,
                expr=point_in_time_distinct_expr,
            ),
            CommonTable(
                name=processed_request_table_pair.distinct_by_scheduled_job_time.name,
                expr=scheduled_job_time_distinct_expr,
            ),
        ]


class NonTileWindowAggregator(NonTileBasedAggregator[NonTileWindowAggregateSpec]):
    """
    Aggregator responsible for generating SQL for window aggregation without using tiles
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.request_table_plan = NonTileRequestTablePlan(self.source_info)
        self.aggregation_source_views: Dict[str, Select] = {}

    def additional_update(self, aggregation_spec: NonTileWindowAggregateSpec) -> None:
        self.request_table_plan.add_aggregation_spec(aggregation_spec)
        source_view_table_name = self.get_source_view_table_name(aggregation_spec)
        if source_view_table_name not in self.aggregation_source_views:
            self.aggregation_source_views[source_view_table_name] = (
                self.get_source_view_with_timestamp_epoch(aggregation_spec)
            )

    @staticmethod
    def get_source_view_table_name(aggregation_spec: NonTileWindowAggregateSpec) -> str:
        """
        Get the view name corresponding to the source of the aggregation

        Parameters
        ----------
        aggregation_spec: NonTileWindowAggregateSpec
            Aggregation spec

        Returns
        -------
        str
        """
        return f"VIEW_{aggregation_spec.source_hash}"

    def get_source_view_with_timestamp_epoch(
        self, aggregation_spec: NonTileWindowAggregateSpec
    ) -> Select:
        """
        Get the source view augmented with timestamp converted to epoch seconds

        Parameters
        ----------
        aggregation_spec: NonTileWindowAggregateSpec
            Aggregation spec

        Returns
        -------
        Select
        """
        timestamp_expr = quoted_identifier(aggregation_spec.parameters.timestamp)

        if aggregation_spec.parameters.timestamp_schema is not None:
            timestamp_expr = convert_timestamp_to_utc(
                column_expr=timestamp_expr,
                timestamp_schema=aggregation_spec.parameters.timestamp_schema,
                adapter=self.adapter,
            )

        timestamp_epoch_expr = self.adapter.to_epoch_seconds(timestamp_expr)

        return select(
            expressions.Star(),
            alias_(timestamp_epoch_expr, alias=InternalName.VIEW_TIMESTAMP_EPOCH, quoted=True),
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
        self, specs: list[NonTileWindowAggregateSpec], existing_columns: set[str]
    ) -> LeftJoinableSubquery:
        spec = specs[0]

        # Left table: processed request table
        processed_request_tables = self.request_table_plan.get_processed_request_tables(spec)
        left_table = LeftTable(
            name=quoted_identifier(processed_request_tables.distinct_by_scheduled_job_time.name),
            alias="REQ",
            join_keys=spec.serving_names,
            range_start=InternalName.WINDOW_START_EPOCH,
            range_end=InternalName.WINDOW_END_EPOCH,
            columns=[InternalName.JOB_SCHEDULE_EPOCH] + (spec.serving_names or []),
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
        groupby_input_expr = range_join_tables(
            left_table=left_table,
            right_table=right_table,
            window_size=spec.window,
        )

        # Aggregation
        groupby_keys = [
            GroupbyKey(
                expr=quoted_identifier(InternalName.JOB_SCHEDULE_EPOCH),
                name=InternalName.JOB_SCHEDULE_EPOCH,
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

        # Aggregated result is now distinct by scheduled feature job time and serving names. Join
        # with the distinct by point in time request table to get the final result
        aggregated_column_names = sorted(column_names)
        aggregated_expr = join_aggregated_expr_with_distinct_point_in_time(
            aggregated_expr=aggregated_expr,
            distinct_key=InternalName.JOB_SCHEDULE_EPOCH.value,
            serving_names=spec.serving_names,
            aggregated_column_names=aggregated_column_names,
            distinct_by_point_in_time_table_name=processed_request_tables.distinct_by_point_in_time.name,
        )

        return LeftJoinableSubquery(
            expr=aggregated_expr,
            column_names=list(column_names),
            join_keys=[SpecialColumnName.POINT_IN_TIME.value] + spec.serving_names,
        )

    def get_common_table_expressions(self, request_table_name: str) -> list[CommonTable]:
        out: list[CommonTable] = []
        out.extend(self.request_table_plan.construct_request_table_ctes(request_table_name))
        for table_name, view_expr in self.aggregation_source_views.items():
            out.append(CommonTable(name=table_name, expr=view_expr))
        return out
