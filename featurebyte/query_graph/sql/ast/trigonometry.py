"""
Trigonometry SQL AST module
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from sqlglot import expressions

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext


@dataclass
class Atan2Node(ExpressionNode):
    """Node to represent Atan2"""

    y_expr: ExpressionNode
    x_expr: ExpressionNode
    query_node_type = NodeType.ATAN2

    @property
    def sql(self) -> expressions.Expression:
        return expressions.Anonymous(this="ATAN2", expressions=[self.y_expr.sql, self.x_expr.sql])

    @classmethod
    def build(cls, context: SQLNodeContext) -> Atan2Node:
        y_node = cast(ExpressionNode, context.input_sql_nodes[0])
        x_node = cast(ExpressionNode, context.input_sql_nodes[1])
        table_node = y_node.table_node
        return Atan2Node(
            context=context,
            table_node=table_node,
            y_expr=y_node,
            x_expr=x_node,
        )
