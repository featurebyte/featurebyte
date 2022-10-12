"""
Module for count dict sql generation
"""
from __future__ import annotations

from typing import Literal

from dataclasses import dataclass

from sqlglot import Expression, expressions, parse_one

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext, make_literal_value
from featurebyte.query_graph.sql.ast.util import prepare_unary_input_nodes

MISSING_VALUE_REPLACEMENT = "__MISSING__"


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
        output_expr = expressions.Anonymous(this=function_name, expressions=[counts_expr])
        if self.transform_type == "most_frequent":
            # The F_COUNT_DICT_MOST_FREQUENT UDF produces a VARIANT type. Cast to string to prevent
            # double quoting in the feature output ('remove' vs '"remove"')
            output_expr = expressions.Cast(this=output_expr, to=parse_one("VARCHAR"))
        return output_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> CountDictTransformNode:
        table_node, input_expr_node, parameters = prepare_unary_input_nodes(context)
        sql_node = CountDictTransformNode(
            table_node=table_node,
            expr=input_expr_node,
            transform_type=parameters["transform_type"],
            include_missing=parameters.get("include_missing", True),
        )
        return sql_node
