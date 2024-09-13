"""
Module for count dict sql generation
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import expressions
from sqlglot.expressions import Expression
from typing_extensions import Literal

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.count_dict import (
    CountDictTransformNode as CountDictTransformQueryGraphNode,
)
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.ast.util import (
    prepare_binary_op_input_nodes,
    prepare_unary_input_nodes,
)
from featurebyte.query_graph.sql.common import MISSING_VALUE_REPLACEMENT


@dataclass
class CountDictTransformNode(ExpressionNode):
    """Node for count dict transform operation (eg. entropy)"""

    expr: ExpressionNode
    transform_type: Literal["entropy", "most_frequent", "unique_count"]
    include_missing: bool
    query_node_type = NodeType.COUNT_DICT_TRANSFORM

    @property
    def sql(self) -> Expression:
        function_name = {
            "entropy": "F_COUNT_DICT_ENTROPY",
            "most_frequent": "F_COUNT_DICT_MOST_FREQUENT",
            "key_with_highest_value": "F_COUNT_DICT_MOST_FREQUENT",
            "key_with_lowest_value": "F_COUNT_DICT_LEAST_FREQUENT",
            "unique_count": "F_COUNT_DICT_NUM_UNIQUE",
        }[self.transform_type]
        if self.include_missing:
            counts_expr = self.expr.sql
        else:
            counts_expr = self.context.adapter.call_udf(
                "OBJECT_DELETE",
                [self.expr.sql, make_literal_value(MISSING_VALUE_REPLACEMENT)],
            )
        output_expr = self.context.adapter.call_udf(function_name, [counts_expr])  # type: Expression
        if (
            self.transform_type
            in CountDictTransformQueryGraphNode.transform_types_with_varchar_output
        ):
            # Some UDFs such as F_COUNT_DICT_MOST_FREQUENT produce a VARIANT type. Cast to string to
            # prevent double quoting in the feature output ('remove' vs '"remove"')
            output_expr = expressions.Cast(
                this=output_expr, to=expressions.DataType.build("VARCHAR")
            )
        return output_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> CountDictTransformNode:
        table_node, input_expr_node, parameters = prepare_unary_input_nodes(context)
        sql_node = CountDictTransformNode(
            context=context,
            table_node=table_node,
            expr=input_expr_node,
            transform_type=parameters["transform_type"],
            include_missing=parameters.get("include_missing", True),
        )
        return sql_node


@dataclass
class DictionaryKeysNode(ExpressionNode):
    """Node converting dictionary keys into an array"""

    dictionary_feature_node: ExpressionNode
    query_node_type = NodeType.DICTIONARY_KEYS

    @property
    def sql(self) -> Expression:
        return self.context.adapter.object_keys(self.dictionary_feature_node.sql)

    @classmethod
    def build(cls, context: SQLNodeContext) -> DictionaryKeysNode:
        table_node, input_expr_node, _ = prepare_unary_input_nodes(context)
        return DictionaryKeysNode(
            context=context,
            table_node=table_node,
            dictionary_feature_node=input_expr_node,
        )


@dataclass
class GetValueFromDictionaryNode(ExpressionNode):
    """Node that gets the value from a dictionary"""

    dictionary_feature_node: ExpressionNode
    lookup_feature_node: ExpressionNode
    query_node_type = NodeType.GET_VALUE

    @property
    def sql(self) -> Expression:
        return self.context.adapter.get_value_from_dictionary(
            self.dictionary_feature_node.sql,
            self.context.adapter.cast_to_string(self.lookup_feature_node.sql, None),
        )

    @classmethod
    def build(cls, context: SQLNodeContext) -> GetValueFromDictionaryNode:
        table_node, dictionary_node, lookup_node = prepare_binary_op_input_nodes(context)
        return GetValueFromDictionaryNode(
            context=context,
            table_node=table_node,
            dictionary_feature_node=dictionary_node,
            lookup_feature_node=lookup_node,
        )


@dataclass
class GetRelativeFrequencyNode(ExpressionNode):
    """Node to get relative frequency"""

    lookup_key_node: ExpressionNode
    dictionary_node: ExpressionNode
    query_node_type = NodeType.GET_RELATIVE_FREQUENCY

    @property
    def sql(self) -> Expression:
        return self.context.adapter.call_udf(
            "F_GET_RELATIVE_FREQUENCY",
            [
                self.dictionary_node.sql,
                self.context.adapter.cast_to_string(
                    self.lookup_key_node.sql,
                    None,
                ),
            ],
        )

    @classmethod
    def build(cls, context: SQLNodeContext) -> GetRelativeFrequencyNode:
        table_node, dictionary_node, lookup_key_node = prepare_binary_op_input_nodes(context)
        return GetRelativeFrequencyNode(
            context=context,
            table_node=table_node,
            dictionary_node=dictionary_node,
            lookup_key_node=lookup_key_node,
        )


@dataclass
class GetRankNode(ExpressionNode):
    """Node to get relative frequency"""

    lookup_key_node: ExpressionNode
    dictionary_node: ExpressionNode
    descending: bool = False
    query_node_type = NodeType.GET_RANK

    @property
    def sql(self) -> Expression:
        return self.context.adapter.call_udf(
            "F_GET_RANK",
            [
                self.dictionary_node.sql,
                self.context.adapter.cast_to_string(self.lookup_key_node.sql, None),
                make_literal_value(self.descending),
            ],
        )

    @classmethod
    def build(cls, context: SQLNodeContext) -> GetRankNode:
        table_node, dictionary_node, lookup_key_node = prepare_binary_op_input_nodes(context)
        return GetRankNode(
            context=context,
            table_node=table_node,
            dictionary_node=dictionary_node,
            lookup_key_node=lookup_key_node,
            descending=context.parameters["descending"],
        )
