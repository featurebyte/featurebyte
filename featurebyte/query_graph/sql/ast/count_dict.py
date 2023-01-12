"""
Module for count dict sql generation
"""
from __future__ import annotations

from typing import Literal

from dataclasses import dataclass

from sqlglot import expressions, parse_one
from sqlglot.expressions import Expression

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.ast.util import prepare_unary_input_nodes
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
            "unique_count": "F_COUNT_DICT_NUM_UNIQUE",
        }[self.transform_type]
        if self.include_missing:
            counts_expr = self.expr.sql
        else:
            counts_expr = expressions.Anonymous(
                this="OBJECT_DELETE",
                expressions=[self.expr.sql, make_literal_value(MISSING_VALUE_REPLACEMENT)],
            )
        output_expr = expressions.Anonymous(
            this=function_name, expressions=[counts_expr]
        )  # type: Expression
        if self.transform_type == "most_frequent":
            # The F_COUNT_DICT_MOST_FREQUENT UDF produces a VARIANT type. Cast to string to prevent
            # double quoting in the feature output ('remove' vs '"remove"')
            output_expr = expressions.Cast(this=output_expr, to=parse_one("VARCHAR"))
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
        output_expr = expressions.Anonymous(
            this="OBJECT_KEYS", expressions=[self.dictionary_feature_node.sql]
        )
        return output_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> DictionaryKeysNode:
        table_node, input_expr_node, _ = prepare_unary_input_nodes(context)
        return DictionaryKeysNode(
            context=context,
            table_node=table_node,
            dictionary_feature_node=input_expr_node,
        )
