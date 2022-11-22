"""
Module for join operation sql generation
"""
from __future__ import annotations

from typing import Literal, cast

from dataclasses import dataclass

from sqlglot import Expression, Select, expressions, select

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.common import quoted_identifier


@dataclass
class Join(TableNode):
    """
    Join SQLNode
    """

    left_node: TableNode
    right_node: TableNode
    left_on: str
    right_on: str
    join_type: Literal["left", "inner"]
    query_node_type = NodeType.JOIN

    def select_query_impl(self, select_expr: Select) -> Select:
        left_subquery = expressions.Subquery(this=self.left_node.sql, alias="L")
        join_conditions = expressions.EQ(
            this=self._get_qualified_column_identifier(self.left_on, "L"),
            expression=self._get_qualified_column_identifier(self.right_on, "R"),
        )
        select_expr = select_expr.from_(left_subquery).join(
            self.right_node.sql_nested(),
            on=join_conditions,
            join_type=self.join_type,
            join_alias="R",
        )
        return select_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> Join:
        parameters = context.parameters
        columns_map = {}
        for input_col, output_col in zip(
            parameters["left_input_columns"], parameters["left_output_columns"]
        ):
            columns_map[output_col] = cls._get_qualified_column_identifier(input_col, "L")
        for input_col, output_col in zip(
            parameters["right_input_columns"], parameters["right_output_columns"]
        ):
            columns_map[output_col] = cls._get_qualified_column_identifier(input_col, "R")
        node = Join(
            context=context,
            columns_map=columns_map,
            left_node=cast(TableNode, context.input_sql_nodes[0]),
            right_node=cast(TableNode, context.input_sql_nodes[1]),
            left_on=parameters["left_on"],
            right_on=parameters["right_on"],
            join_type=parameters["join_type"],
        )
        return node

    @classmethod
    def _get_qualified_column_identifier(
        cls, column_name: str, side: Literal["L", "R"]
    ) -> Expression:
        expr = expressions.Column(this=quoted_identifier(column_name), table=side)
        return expr
