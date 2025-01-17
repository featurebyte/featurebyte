"""
SQL node for ZipTimestampTZTupleNode
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import Expression

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext
from featurebyte.query_graph.sql.ast.util import prepare_binary_op_input_nodes


@dataclass
class ZipTimestampTZTupleNode(ExpressionNode):
    """Node for zip_timestamp_tz_tuple operation"""

    timestamp_node: ExpressionNode
    timezone_offset_node: ExpressionNode
    query_node_type = NodeType.ZIP_TIMESTAMP_TZ_TUPLE

    @property
    def sql(self) -> Expression:
        raise

    @classmethod
    def build(cls, context: SQLNodeContext) -> ZipTimestampTZTupleNode:
        table_node, left_node, right_node = prepare_binary_op_input_nodes(context)
        return ZipTimestampTZTupleNode(
            context=context,
            table_node=table_node,
            timestamp_node=left_node,
            timezone_offset_node=right_node,
        )
