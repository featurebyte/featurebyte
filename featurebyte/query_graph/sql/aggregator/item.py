"""
SQL generation for aggregation without time windows from ItemView
"""

from __future__ import annotations

from typing import Any

from sqlglot import expressions
from sqlglot.expressions import Select, select

from featurebyte.query_graph.sql.aggregator.base import (
    AggregationResult,
    CommonTable,
    LeftJoinableSubquery,
    NonTileBasedAggregator,
)
from featurebyte.query_graph.sql.aggregator.request_table import RequestTablePlan
from featurebyte.query_graph.sql.common import (
    get_qualified_column_identifier,
    quoted_identifier,
)
from featurebyte.query_graph.sql.groupby_helper import GroupbyColumn, GroupbyKey, get_groupby_expr
from featurebyte.query_graph.sql.specs import ItemAggregationSpec


class ItemAggregator(NonTileBasedAggregator[ItemAggregationSpec]):
    """
    ItemAggregator is responsible for SQL generation for aggregation without time windows from
    ItemView
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.to_inner_join_with_request_table = kwargs.pop("to_inner_join_with_request_table", True)
        super().__init__(*args, **kwargs)
        self.non_time_aware_request_table_plan = RequestTablePlan(is_time_aware=False)

    def additional_update(self, aggregation_spec: ItemAggregationSpec) -> None:
        """
        Update internal state to account for the given ItemAggregationSpec

        Parameters
        ----------
        aggregation_spec: ItemAggregationSpec
            Aggregation specification
        """
        self.non_time_aware_request_table_plan.add_aggregation_spec(aggregation_spec)

    def _get_aggregation_subquery(
        self,
        agg_specs: list[ItemAggregationSpec],
    ) -> LeftJoinableSubquery:
        """
        Construct SQL for non-time aware item aggregation

        Parameters
        ----------
        agg_specs: list[ItemAggregationSpec]
            ItemAggregationSpec objects

        Returns
        -------
        LeftJoinableSubquery
        """
        spec = agg_specs[0]

        # Construct input to be aggregated using GROUP BY
        if self.to_inner_join_with_request_table:
            request_table_name = self.non_time_aware_request_table_plan.get_request_table_name(spec)
            join_condition = expressions.and_(*[
                expressions.EQ(
                    this=get_qualified_column_identifier(serving_name, "REQ"),
                    expression=get_qualified_column_identifier(key, "ITEM"),
                )
                for serving_name, key in zip(spec.serving_names, spec.parameters.keys)
            ])
            groupby_input_expr = (
                select()
                .from_(
                    expressions.Table(
                        this=quoted_identifier(request_table_name),
                        alias="REQ",
                    )
                )
                .join(
                    spec.source_expr.subquery(),
                    join_type="inner",
                    join_alias="ITEM",
                    on=join_condition,
                )
            )
            groupby_keys = [
                GroupbyKey(
                    expr=get_qualified_column_identifier(serving_name, "REQ"),
                    name=serving_name,
                )
                for serving_name in spec.serving_names
            ]
        else:
            groupby_input_expr = select().from_(spec.source_expr.subquery(alias="ITEM"))
            groupby_keys = [
                GroupbyKey(
                    expr=get_qualified_column_identifier(key, "ITEM"),
                    name=serving_name,
                )
                for (key, serving_name) in zip(spec.parameters.keys, spec.serving_names)
            ]

        # Construct GROUP BY expression using groupby_helper
        groupby_columns = [
            GroupbyColumn(
                agg_func=s.parameters.agg_func,
                parent_expr=(
                    get_qualified_column_identifier(s.parameters.parent, "ITEM")
                    if s.parameters.parent
                    else None
                ),
                result_name=s.agg_result_name,
                parent_dtype=s.parent_dtype,
                parent_cols=(
                    [get_qualified_column_identifier(s.parameters.parent, "ITEM")]
                    if s.parameters.parent
                    else []
                ),
            )
            for s in agg_specs
        ]
        value_by = (
            GroupbyKey(
                expr=get_qualified_column_identifier(spec.parameters.value_by, "ITEM"),
                name=spec.parameters.value_by,
            )
            if spec.parameters.value_by
            else None
        )
        item_aggregate_expr = get_groupby_expr(
            input_expr=groupby_input_expr,
            groupby_keys=groupby_keys,
            groupby_columns=groupby_columns,
            value_by=value_by,
            adapter=self.adapter,
        )

        return LeftJoinableSubquery(
            expr=item_aggregate_expr,
            column_names=[s.agg_result_name for s in agg_specs],
            join_keys=spec.serving_names[:],
        )

    def update_aggregation_table_expr(
        self,
        table_expr: Select,
        point_in_time_column: str,
        current_columns: list[str],
        current_query_index: int,
    ) -> AggregationResult:
        _ = point_in_time_column
        queries = []
        for specs in self.grouped_specs.values():
            query = self._get_aggregation_subquery(specs)
            queries.append(query)

        return self._update_with_left_joins(
            table_expr=table_expr, current_query_index=current_query_index, queries=queries
        )

    def get_common_table_expressions(self, request_table_name: str) -> list[CommonTable]:
        return self.non_time_aware_request_table_plan.construct_request_table_ctes(
            request_table_name
        )
