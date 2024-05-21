"""
Request data related SQLNode
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot.expressions import Expression

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext
from featurebyte.query_graph.sql.common import quoted_identifier


@dataclass
class RequestColumnNode(ExpressionNode):
    """Node that represents a column provided in the request."""

    column_name: str
    query_node_type = NodeType.REQUEST_COLUMN

    @property
    def sql(self) -> Expression:
        return quoted_identifier(self.column_name)

    @classmethod
    def build(cls, context: SQLNodeContext) -> RequestColumnNode:
        return RequestColumnNode(
            context=context,
            table_node=None,
            column_name=context.parameters["column_name"],
        )
