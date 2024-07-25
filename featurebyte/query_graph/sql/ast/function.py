"""
Generic function related SQLNode
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, cast

from sqlglot import expressions
from sqlglot.expressions import Expression

from featurebyte.enum import DBVarType, FunctionParameterInputForm
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext
from featurebyte.query_graph.sql.ast.literal import make_literal_value


@dataclass
class GenericFunctionNode(ExpressionNode):
    """Node that represents a column after applying a generic function."""

    query_node_type = NodeType.GENERIC_FUNCTION
    function_name: str
    function_parameters: List[Any]

    @property
    def sql(self) -> Expression:
        return expressions.Anonymous(this=self.function_name, expressions=self.function_parameters)

    @classmethod
    def build(cls, context: SQLNodeContext) -> GenericFunctionNode:
        parameters = []
        input_sql_nodes = context.input_sql_nodes
        node_index = 0
        for func_param in context.parameters["function_parameters"]:
            if func_param["input_form"] == FunctionParameterInputForm.COLUMN:
                parameters.append(input_sql_nodes[node_index].sql)
                node_index += 1
            else:
                cast_as_timestamp = func_param["dtype"] == DBVarType.TIMESTAMP
                parameters.append(
                    make_literal_value(func_param["value"], cast_as_timestamp=cast_as_timestamp)
                )

        table_node = None
        if input_sql_nodes:
            table_node = cast(ExpressionNode, input_sql_nodes[0]).table_node
        return GenericFunctionNode(
            context=context,
            table_node=table_node,
            function_name=context.parameters["sql_function_name"],
            function_parameters=parameters,
        )
