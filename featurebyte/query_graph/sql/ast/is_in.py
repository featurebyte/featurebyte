"""
Is in SQL node
"""
from __future__ import annotations

from dataclasses import dataclass

from sqlglot import Expression, expressions

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext
from featurebyte.query_graph.sql.ast.util import prepare_binary_op_input_nodes


@dataclass
class IsInNode(ExpressionNode):
    """Node that checks if the input series is in an array."""

    input_series_expression_node: ExpressionNode
    array_expression_node: ExpressionNode
    query_node_type = NodeType.IS_IN

    @property
    def sql(self) -> Expression:
        return self.context.adapter.in_array(
            self.input_series_expression_node.sql, self.array_expression_node.sql
        )

    @classmethod
    def build(cls, context: SQLNodeContext) -> IsInNode:
        table_node, left_node, right_node = prepare_binary_op_input_nodes(context)
        return IsInNode(
            context=context,
            table_node=table_node,
            input_series_expression_node=left_node,
            array_expression_node=right_node,
        )
