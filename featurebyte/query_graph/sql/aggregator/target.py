"""
Target aggregator module
"""
from __future__ import annotations

from sqlglot.expressions import Select

from featurebyte.query_graph.sql.aggregator.base import AggregationResult, NonTileBasedAggregator
from featurebyte.query_graph.sql.common import CteStatements
from featurebyte.query_graph.sql.specs import TargetSpec


class TargetAggregator(NonTileBasedAggregator[TargetSpec]):
    """
    TargetAggregator is responsible for generating SQL for targets.
    """

    def get_common_table_expressions(self, request_table_name: str) -> CteStatements:
        _ = request_table_name
        return []

    def additional_update(self, aggregation_spec: TargetSpec) -> None:
        return

    def update_aggregation_table_expr(
        self,
        table_expr: Select,
        point_in_time_column: str,
        current_columns: list[str],
        current_query_index: int,
    ) -> AggregationResult:
        # TODO:
        return AggregationResult(
            updated_table_expr=table_expr,
            updated_index=current_query_index + 1,
            column_names=current_columns,
        )
