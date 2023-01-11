"""
Feature dictionary node operators
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import Expression, expressions

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext
from featurebyte.query_graph.sql.ast.util import prepare_binary_op_input_nodes


@dataclass
class IsInDictionaryNode(ExpressionNode):
    """
    Is in dictionary node
    """

    lookup_feature_node: ExpressionNode
    target_dictionary_node: ExpressionNode
    query_node_type = NodeType.IS_IN_DICT

    @property
    def sql(self) -> Expression:
        object_keys = self.context.adapter.object_keys(self.target_dictionary_node.sql)
        return expressions.In(this=self.lookup_feature_node.sql, expressions=object_keys)

    @classmethod
    def build(cls, context: SQLNodeContext) -> IsInDictionaryNode:
        table_node, left_node, right_node = prepare_binary_op_input_nodes(context)
        output_node = IsInDictionaryNode(
            context=context,
            table_node=table_node,
            lookup_feature_node=left_node,
            target_dictionary_node=right_node,
        )
        return output_node
