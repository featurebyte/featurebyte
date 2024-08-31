"""
Vector SQL node module
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot.expressions import Expression

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext
from featurebyte.query_graph.sql.ast.util import prepare_binary_op_input_nodes


@dataclass
class VectorCosineSimilarityNode(ExpressionNode):
    """Node for vector cosine_similarity operation"""

    array_column_1_node: ExpressionNode
    array_column_2_node: ExpressionNode
    query_node_type = NodeType.VECTOR_COSINE_SIMILARITY

    @property
    def sql(self) -> Expression:
        return self.context.adapter.call_udf(
            "F_VECTOR_COSINE_SIMILARITY",
            [
                self.array_column_1_node.sql,
                self.array_column_2_node.sql,
            ],
        )

    @classmethod
    def build(cls, context: SQLNodeContext) -> VectorCosineSimilarityNode:
        table_node, left_node, right_node = prepare_binary_op_input_nodes(context)
        return VectorCosineSimilarityNode(
            context=context,
            table_node=table_node,
            array_column_1_node=left_node,
            array_column_2_node=right_node,
        )
