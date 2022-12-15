"""
Module for join operation sql generation
"""
from __future__ import annotations

from typing import Literal, Optional, cast

from dataclasses import dataclass

from sqlglot import expressions
from sqlglot.expressions import Select, alias_, select

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext, TableNode
from featurebyte.query_graph.sql.common import get_qualified_column_identifier, quoted_identifier
from featurebyte.query_graph.sql.scd_helper import Table, get_scd_join_expr


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

    def from_query_impl(self, select_expr: Select) -> Select:
        left_subquery = expressions.Subquery(this=self.left_node.sql, alias="L")
        join_conditions = expressions.EQ(
            this=get_qualified_column_identifier(self.left_on, "L"),
            expression=get_qualified_column_identifier(self.right_on, "R"),
        )
        select_expr = select_expr.from_(left_subquery).join(
            self.right_node.sql_nested(),
            on=join_conditions,
            join_type=self.join_type,
            join_alias="R",
        )
        return select_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> Optional[Join]:
        if context.parameters.get("scd_parameters") is not None:
            return None
        parameters = context.parameters
        columns_map = {}
        for input_col, output_col in zip(
            parameters["left_input_columns"], parameters["left_output_columns"]
        ):
            columns_map[output_col] = get_qualified_column_identifier(input_col, "L")
        for input_col, output_col in zip(
            parameters["right_input_columns"], parameters["right_output_columns"]
        ):
            columns_map[output_col] = get_qualified_column_identifier(input_col, "R")
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


@dataclass
class SCDJoin(TableNode):
    """
    SCDJoin joins the latest record per natural key from the right table to the left table
    """

    # pylint: disable=too-many-instance-attributes

    left_node: TableNode
    right_node: TableNode
    left_on: str
    right_on: str
    left_timestamp_column: str
    right_timestamp_column: str
    left_input_columns: list[str]
    left_output_columns: list[str]
    right_input_columns: list[str]
    right_output_columns: list[str]
    join_type: Literal["left", "inner"]
    query_node_type = NodeType.JOIN

    def from_query_impl(self, select_expr: Select) -> Select:
        """
        Construct a query to perform SCD join

        The general idea is to first merge the left timestamps (event timestamps) and right
        timestamps (SCD record effective timestamps) along with join keys into a temporary table.
        Then apply a LAG window function on this temporary table to retrieve the latest effective
        timestamp corresponding to each event timestamp in one go.

        Parameters
        ----------
        select_expr: Select
            Partially constructed select expression

        Returns
        -------
        Select
        """
        left_table = Table(
            expr=cast(Select, self.left_node.sql),
            timestamp_column=self.left_timestamp_column,
            join_key=self.left_on,
            input_columns=self.left_input_columns,
            output_columns=self.left_output_columns,
        )
        right_table = Table(
            expr=cast(Select, self.right_node.sql),
            timestamp_column=self.right_timestamp_column,
            join_key=self.right_on,
            input_columns=self.right_input_columns,
            output_columns=self.right_output_columns,
        )
        select_expr = get_scd_join_expr(
            left_table,
            right_table,
            join_type=self.join_type,
            select_expr=select_expr,
            adapter=self.context.adapter,
        )
        return select_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> Optional[SCDJoin]:
        if context.parameters.get("scd_parameters") is None:
            return None
        parameters = context.parameters

        columns_map = {}

        # It is intended to consider only "left_output_columns" here. In the L aliased subquery
        # that will be constructed in from_query_impl(), the left side columns would have already
        # been renamed, so here they should be referred to using output column names.
        for output_col in parameters["left_output_columns"]:
            columns_map[output_col] = get_qualified_column_identifier(output_col, "L")

        for input_col, output_col in zip(
            parameters["right_input_columns"], parameters["right_output_columns"]
        ):
            columns_map[output_col] = get_qualified_column_identifier(input_col, "R")

        node = SCDJoin(
            context=context,
            columns_map=columns_map,
            left_node=cast(TableNode, context.input_sql_nodes[0]),
            right_node=cast(TableNode, context.input_sql_nodes[1]),
            left_on=parameters["left_on"],
            right_on=parameters["right_on"],
            join_type=parameters["join_type"],
            left_timestamp_column=parameters["scd_parameters"]["left_timestamp_column"],
            right_timestamp_column=parameters["scd_parameters"]["effective_timestamp_column"],
            left_input_columns=parameters["left_input_columns"],
            left_output_columns=parameters["left_output_columns"],
            right_input_columns=parameters["right_input_columns"],
            right_output_columns=parameters["right_output_columns"],
        )
        return node
