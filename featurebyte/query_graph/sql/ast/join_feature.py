"""
SQL generation for JOIN_FEATURE query node type
"""
from __future__ import annotations

from typing import cast

from dataclasses import dataclass

from sqlglot import expressions, select
from sqlglot.expressions import Select, alias_

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext, TableNode
from featurebyte.query_graph.sql.common import get_qualified_column_identifier, quoted_identifier


@dataclass
class JoinFeature(TableNode):
    """
    JoinFeature SQLNode

    Responsible for generating SQL code for adding a Feature to an EventView
    """

    view_node: TableNode
    view_entity_column: str
    feature_node: ExpressionNode
    feature_entity_column: str
    name: str
    query_node_type = NodeType.JOIN_FEATURE

    # Internal names for SQL construction
    TEMP_FEATURE_NAME = "__FB_TEMP_FEATURE_NAME"

    def from_query_impl(self, select_expr: Select) -> Select:

        # Subquery for View
        left_subquery = expressions.Subquery(this=self.view_node.sql, alias="L")

        # Subquery for Feature
        feature_sql = select(
            alias_(self.feature_node.sql, alias=self.TEMP_FEATURE_NAME, quoted=True),
            quoted_identifier(self.feature_entity_column),
        ).from_(self.feature_node.table_node.sql_nested())

        # Join condition based on entity column
        join_conditions = expressions.EQ(
            this=get_qualified_column_identifier(self.view_entity_column, "L"),
            expression=get_qualified_column_identifier(self.feature_entity_column, "R"),
        )

        select_expr = select_expr.from_(left_subquery).join(
            feature_sql,
            on=join_conditions,
            join_type="left",
            join_alias="R",
        )
        return select_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> JoinFeature:
        parameters = context.parameters

        view_node = cast(TableNode, context.input_sql_nodes[0])
        columns_map = {}
        for col in view_node.columns:
            columns_map[col] = get_qualified_column_identifier(col, "L")

        feature_name = parameters["name"]
        columns_map[feature_name] = get_qualified_column_identifier(cls.TEMP_FEATURE_NAME, "R")

        feature_entity_column = parameters["feature_entity_column"]
        feature_node = cast(ExpressionNode, context.input_sql_nodes[1])

        node = JoinFeature(
            context=context,
            columns_map=columns_map,
            view_node=view_node,
            view_entity_column=parameters["view_entity_column"],
            feature_node=feature_node,
            feature_entity_column=feature_entity_column,
            name=feature_name,
        )
        return node
