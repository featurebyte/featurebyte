"""
SQL node for ZipTimestampTZTupleNode
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import Expression

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node.binary import ZipTimestampTZTupleNodeParameters
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext
from featurebyte.query_graph.sql.ast.util import prepare_binary_op_input_nodes
from featurebyte.query_graph.sql.timestamp_helper import convert_timestamp_to_utc


@dataclass
class ZipTimestampTZTupleNode(ExpressionNode):
    """Node for zip_timestamp_tz_tuple operation"""

    timestamp_node: ExpressionNode
    timezone_offset_node: ExpressionNode
    timestamp_schema: TimestampSchema
    query_node_type = NodeType.ZIP_TIMESTAMP_TZ_TUPLE

    @property
    def sql(self) -> Expression:
        timestamp_utc_expr = convert_timestamp_to_utc(
            column_expr=self.timestamp_node.sql,
            timestamp_schema=self.timestamp_schema,
            adapter=self.context.adapter,
        )
        return self.context.adapter.zip_timestamp_and_timezone(
            timestamp_utc_expr, self.timezone_offset_node.sql
        )

    @classmethod
    def build(cls, context: SQLNodeContext) -> ZipTimestampTZTupleNode:
        table_node, left_node, right_node = prepare_binary_op_input_nodes(context)
        parameters = ZipTimestampTZTupleNodeParameters(**context.parameters)
        return ZipTimestampTZTupleNode(
            context=context,
            table_node=table_node,
            timestamp_node=left_node,
            timezone_offset_node=right_node,
            timestamp_schema=parameters.timestamp_schema,
        )
