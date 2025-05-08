"""
Target aggregator module
"""

from __future__ import annotations

from typing import Any

from sqlglot import expressions
from sqlglot.expressions import Select, select

from featurebyte.common.model_util import parse_duration_string
from featurebyte.enum import SpecialColumnName
from featurebyte.query_graph.sql.aggregator.base import (
    AggregationResult,
    CommonTable,
    LeftJoinableSubquery,
    NonTileBasedAggregator,
)
from featurebyte.query_graph.sql.aggregator.request_table import RequestTablePlan
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    get_qualified_column_identifier,
    quoted_identifier,
)
from featurebyte.query_graph.sql.groupby_helper import GroupbyColumn, GroupbyKey, get_groupby_expr
from featurebyte.query_graph.sql.specs import ForwardAggregateSpec


class ForwardAggregator(NonTileBasedAggregator[ForwardAggregateSpec]):
    """
    ForwardAggregator is responsible for generating SQL for forward aggregate targets.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.request_table_plan = RequestTablePlan(is_time_aware=True)

    def get_common_table_expressions(self, request_table_name: str) -> list[CommonTable]:
        return self.request_table_plan.construct_request_table_ctes(request_table_name)

    def additional_update(self, aggregation_spec: ForwardAggregateSpec) -> None:
        self.request_table_plan.add_aggregation_spec(aggregation_spec)

    def _get_aggregation_subquery(self, specs: list[ForwardAggregateSpec]) -> LeftJoinableSubquery:
        """
        Get aggregation subquery that performs the forward aggregation. The list of aggregation
        specifications provided can be done in a single groupby operation.

        Parameters
        ----------
        specs: list[ForwardAggregateSpec]
            Aggregation specifications

        Returns
        -------
        LeftJoinableSubquery
        """

        spec = specs[0]

        # End point expression
        point_in_time_expr = get_qualified_column_identifier(SpecialColumnName.POINT_IN_TIME, "REQ")
        point_in_time_epoch_expr = self.adapter.to_epoch_seconds(point_in_time_expr)
        window_in_seconds = 0
        if spec.parameters.window:
            window_in_seconds = parse_duration_string(spec.parameters.window)
        end_point_expr = expressions.Add(
            this=point_in_time_epoch_expr, expression=make_literal_value(window_in_seconds)
        )

        # Get valid records (timestamp column is within the point in time, and point in time + window)
        # TODO: update to range join
        table_timestamp_col = get_qualified_column_identifier(
            spec.parameters.timestamp_col, "SOURCE_TABLE"
        )
        start_point_expr: expressions.Expression
        if spec.parameters.offset is not None:
            offset_in_seconds = parse_duration_string(spec.parameters.offset)
            start_point_expr = expressions.Add(
                this=point_in_time_epoch_expr,
                expression=make_literal_value(offset_in_seconds),
            )
            end_point_expr = expressions.Add(
                this=end_point_expr,
                expression=make_literal_value(offset_in_seconds),
            )
        else:
            start_point_expr = point_in_time_epoch_expr

        # Convert to epoch seconds
        table_timestamp_col = self.adapter.to_epoch_seconds(table_timestamp_col)
        record_validity_condition = expressions.and_(
            expressions.GT(
                this=table_timestamp_col,
                expression=start_point_expr,
            ),
            expressions.LTE(
                this=table_timestamp_col,
                expression=end_point_expr,
            ),
        )

        # Join the valid records, and the request table
        groupby_keys = [
            GroupbyKey(
                expr=point_in_time_expr,
                name=SpecialColumnName.POINT_IN_TIME,
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
                expr=get_qualified_column_identifier(spec.parameters.value_by, "SOURCE_TABLE"),
                name=spec.parameters.value_by,
            )
            if spec.parameters.value_by
            else None
        )
        groupby_columns = [
            GroupbyColumn(
                agg_func=s.parameters.agg_func,
                parent_expr=(
                    get_qualified_column_identifier(s.parameters.parent, "SOURCE_TABLE")
                    if s.parameters.parent
                    else None
                ),
                result_name=s.agg_result_name,
                parent_dtype=s.parent_dtype,
                parent_cols=(
                    [get_qualified_column_identifier(s.parameters.parent, "SOURCE_TABLE")]
                    if s.parameters.parent
                    else []
                ),
            )
            for s in specs
        ]
        join_keys = []
        assert len(spec.serving_names) == len(spec.parameters.keys)
        for serving_name, key in zip(spec.serving_names, spec.parameters.keys):
            serving_name_sql = quoted_identifier(serving_name).sql()
            join_table_key_sql = quoted_identifier(key).sql()
            join_keys.append(f"REQ.{serving_name_sql} = SOURCE_TABLE.{join_table_key_sql}")
        join_conditions = [
            record_validity_condition,
            *join_keys,
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
                spec.source_expr.subquery(alias="SOURCE_TABLE"),
                join_type="inner",
                on=expressions.and_(*join_conditions) if join_conditions else None,  # type: ignore[arg-type]
            )
        )
        # Create the forward aggregation expression
        forward_agg_expr = get_groupby_expr(
            input_expr=groupby_input_expr,
            groupby_keys=groupby_keys,
            groupby_columns=groupby_columns,
            value_by=value_by,
            adapter=self.adapter,
        )
        return LeftJoinableSubquery(
            expr=forward_agg_expr,
            column_names=[s.agg_result_name for s in specs],
            join_keys=[SpecialColumnName.POINT_IN_TIME.value] + spec.serving_names,
        )

    def update_aggregation_table_expr(
        self,
        table_expr: Select,
        point_in_time_column: str,
        current_columns: list[str],
        current_query_index: int,
    ) -> AggregationResult:
        queries = []
        for specs in self.grouped_specs.values():
            query = self._get_aggregation_subquery(specs)
            queries.append(query)

        return self._update_with_left_joins(
            table_expr=table_expr, current_query_index=current_query_index, queries=queries
        )
