"""
Distance module
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from sqlglot.expressions import Expression

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext


@dataclass
class HaversineNode(ExpressionNode):
    """Node to represent Haversine"""

    lat_node_1_expr: ExpressionNode
    lon_node_1_expr: ExpressionNode
    lat_node_2_expr: ExpressionNode
    lon_node_2_expr: ExpressionNode
    query_node_type = NodeType.HAVERSINE

    @property
    def sql(self) -> Expression:
        return self.context.adapter.haversine(
            self.lat_node_1_expr.sql,
            self.lon_node_1_expr.sql,
            self.lat_node_2_expr.sql,
            self.lon_node_2_expr.sql,
        )

    @classmethod
    def build(cls, context: SQLNodeContext) -> HaversineNode:
        lat_node_1 = cast(ExpressionNode, context.input_sql_nodes[0])
        lon_node_1 = cast(ExpressionNode, context.input_sql_nodes[1])
        lat_node_2 = cast(ExpressionNode, context.input_sql_nodes[2])
        lon_node_2 = cast(ExpressionNode, context.input_sql_nodes[3])
        table_node = lat_node_1.table_node
        return HaversineNode(
            context=context,
            table_node=table_node,
            lat_node_1_expr=lat_node_1,
            lon_node_1_expr=lon_node_1,
            lat_node_2_expr=lat_node_2,
            lon_node_2_expr=lon_node_2,
        )
