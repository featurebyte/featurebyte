"""
Module for join operation sql generation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, cast

from sqlglot import expressions
from sqlglot.expressions import Select
from typing_extensions import Literal

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.common import get_qualified_column_identifier
from featurebyte.query_graph.sql.deduplication import get_deduplicated_expr
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
        deduplicated_right_table_expr = get_deduplicated_expr(
            adapter=self.context.adapter,
            table_expr=cast(Select, self.right_node.sql),
            expected_primary_keys=[self.right_on],
        )
        select_expr = select_expr.from_(left_subquery).join(
            deduplicated_right_table_expr,
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
class SCDJoin(TableNode):
    """
    SCDJoin joins the latest record per natural key from the right table to the left table
    """

    left_node: TableNode
    right_node: TableNode
    left_on: str
    right_on: str
    left_timestamp_column: str
    right_timestamp_column: str
    left_timestamp_schema: Optional[TimestampSchema]
    right_timestamp_schema: Optional[TimestampSchema]
    left_input_columns: list[str]
    left_output_columns: list[str]
    right_input_columns: list[str]
    right_output_columns: list[str]
    end_timestamp_column: Optional[str]
    end_timestamp_schema: Optional[TimestampSchema]
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
            timestamp_schema=self.left_timestamp_schema,
            join_keys=[self.left_on],
            input_columns=self.left_input_columns,
            output_columns=self.left_output_columns,
        )
        right_table = Table(
            expr=cast(Select, self.right_node.sql),
            timestamp_column=self.right_timestamp_column,
            timestamp_schema=self.right_timestamp_schema,
            join_keys=[self.right_on],
            input_columns=self.right_input_columns,
            output_columns=self.right_output_columns,
            end_timestamp_column=self.end_timestamp_column,
            end_timestamp_schema=self.end_timestamp_schema,
        )
        select_expr = get_scd_join_expr(
            left_table,
            right_table,
            join_type=self.join_type,
            select_expr=select_expr,
            adapter=self.context.adapter,
            convert_timestamps_to_utc=True,
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

        left_timestamp_metadata_dict = parameters["scd_parameters"].get("left_timestamp_metadata")
        right_timestamp_metadata_dict = parameters["scd_parameters"].get(
            "effective_timestamp_metadata"
        )
        left_timestamp_schema = (
            TimestampSchema(**left_timestamp_metadata_dict["timestamp_schema"])
            if left_timestamp_metadata_dict is not None
            else None
        )
        right_timestamp_schema = (
            TimestampSchema(**right_timestamp_metadata_dict["timestamp_schema"])
            if right_timestamp_metadata_dict is not None
            else None
        )
        end_timestamp_metadata_dict = parameters["scd_parameters"].get("end_timestamp_metadata")
        end_timestamp_schema = (
            TimestampSchema(**end_timestamp_metadata_dict["timestamp_schema"])
            if end_timestamp_metadata_dict is not None
            else None
        )
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
            left_timestamp_schema=left_timestamp_schema,
            right_timestamp_schema=right_timestamp_schema,
            left_input_columns=parameters["left_input_columns"],
            left_output_columns=parameters["left_output_columns"],
            right_input_columns=parameters["right_input_columns"],
            right_output_columns=parameters["right_output_columns"],
            end_timestamp_column=parameters["scd_parameters"].get("end_timestamp_column"),
            end_timestamp_schema=end_timestamp_schema,
        )
        return node
