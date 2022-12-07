"""
Module for join operation sql generation
"""
from __future__ import annotations

from typing import Literal, Optional, cast

from dataclasses import dataclass

from sqlglot import expressions
from sqlglot.expressions import Expression, Select, alias_, select

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext, TableNode
from featurebyte.query_graph.sql.common import quoted_identifier


def get_qualified_column_identifier(column_name: str, table: str) -> Expression:
    """
    Get a qualified column name with a table alias prefix

    Parameters
    ----------
    column_name: str
        Column name
    table: str
        Table prefix to add to the column name

    Returns
    -------
    Expression
    """
    expr = expressions.Column(this=quoted_identifier(column_name), table=table)
    return expr


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
    feature_sql: Select
    feature_entity_column: str
    name: str
    query_node_type = NodeType.JOIN_FEATURE

    # Internal names for SQL construction
    TEMP_FEATURE_NAME = "__FB_TEMP_FEATURE_NAME"

    def from_query_impl(self, select_expr: Select) -> Select:
        left_subquery = expressions.Subquery(this=self.view_node.sql, alias="L")
        join_conditions = expressions.EQ(
            this=get_qualified_column_identifier(self.view_entity_column, "L"),
            expression=get_qualified_column_identifier(self.feature_entity_column, "R"),
        )
        select_expr = select_expr.from_(left_subquery).join(
            self.feature_sql,
            on=join_conditions,
            join_type="left",
            join_alias="R",
        )
        return select_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> JoinFeature:
        parameters = context.parameters

        columns_map = {}
        for col in parameters["view_columns"]:
            columns_map[col] = get_qualified_column_identifier(col, "L")

        feature_name = parameters["name"]
        columns_map[feature_name] = get_qualified_column_identifier(cls.TEMP_FEATURE_NAME, "R")

        feature_entity_column = parameters["feature_entity_column"]
        feature_node = cast(ExpressionNode, context.input_sql_nodes[1])
        feature_sql = select(
            alias_(feature_node.sql, alias=cls.TEMP_FEATURE_NAME, quoted=True),
            quoted_identifier(feature_entity_column),
        ).from_(feature_node.table_node.sql_nested())

        node = JoinFeature(
            context=context,
            columns_map=columns_map,
            view_node=cast(TableNode, context.input_sql_nodes[0]),
            view_entity_column=parameters["view_entity_column"],
            feature_sql=feature_sql,
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
    right_input_columns: list[str]
    right_output_columns: list[str]
    join_type: Literal["left", "inner"]
    query_node_type = NodeType.JOIN

    # Internally used identifiers when constructing SQL
    LEFT_VIEW = "LEFT_VIEW"
    RIGHT_VIEW = "RIGHT_VIEW"
    TS_COL = "TS_COL"
    EFFECTIVE_TS_COL = "EFFECTIVE_TS_COL"
    KEY_COL = "KEY_COL"
    LAST_TS = "LAST_TS"

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

        inner_scd_joined_expr = self._construct_inner_scd_joined_view()

        join_conditions = [
            expressions.EQ(
                this=get_qualified_column_identifier(self.left_timestamp_column, "L"),
                expression=get_qualified_column_identifier(self.left_timestamp_column, "R"),
            ),
            expressions.EQ(
                this=get_qualified_column_identifier(self.left_on, "L"),
                expression=get_qualified_column_identifier(self.left_on, "R"),
            ),
        ]

        select_expr = select_expr.from_(self._left_view_as_table(alias="L")).join(
            inner_scd_joined_expr.subquery(alias="R"),
            join_type=self.join_type,
            on=expressions.and_(*join_conditions),
        )

        return select_expr

    def _construct_inner_scd_joined_view(self) -> Select:
        """
        Construct a query that joins the intermediate table with the input SCD table (right table)

        Example output of the join:

        ------------------------------------------------------------------
        EVENT_TS      CUST_ID    SCD_TS        SCD_COL1    SCD_COL2    ...
        ------------------------------------------------------------------
        2022-04-10    1000       NULL          NULL        NULL
        2022-04-15    1000       2022-04-12    a           x
        2022-04-20    1000       2022-04-20    b           y
        ------------------------------------------------------------------

        The result can be joined back to the input event table (left table) using EVENT_TS and
        CUST_ID columns.

        Returns
        -------
        Select
        """

        left_view_with_effective_ts_expr = self._construct_left_view_with_effective_timestamp()

        scd_columns = []
        for input_col, output_col in zip(self.right_input_columns, self.right_output_columns):
            scd_columns.append(
                alias_(
                    get_qualified_column_identifier(input_col, "INNER_R"),
                    alias=output_col,
                    quoted=True,
                )
            )

        join_conditions = [
            expressions.EQ(
                this=get_qualified_column_identifier(self.LAST_TS, "INNER_L"),
                expression=get_qualified_column_identifier(self.right_timestamp_column, "INNER_R"),
            ),
            expressions.EQ(
                this=get_qualified_column_identifier(self.KEY_COL, "INNER_L"),
                expression=get_qualified_column_identifier(self.right_on, "INNER_R"),
            ),
        ]

        inner_scd_joined_expr = (
            select(
                alias_(
                    get_qualified_column_identifier(self.TS_COL, "INNER_L"),
                    alias=self.left_timestamp_column,
                    quoted=True,
                ),
                alias_(
                    get_qualified_column_identifier(self.KEY_COL, "INNER_L"),
                    alias=self.left_on,
                    quoted=True,
                ),
                *scd_columns,
            )
            .from_(left_view_with_effective_ts_expr.subquery(alias="INNER_L"))
            .join(
                self._right_view_as_table(),
                join_alias="INNER_R",
                join_type="inner",
                on=expressions.and_(*join_conditions),
            )
        )

        return inner_scd_joined_expr

    def _construct_left_view_with_effective_timestamp(self) -> Select:
        """
        This constructs a query that calculates the corresponding SCD effective date for each event
        timestamp. See an example below.

        Left table:

        ------------------------------
        EVENT_TS      CUST_ID    ....
        ------------------------------
        2022-04-10    1000
        2022-04-15    1000
        2022-04-20    1000
        ------------------------------

        Right table:

        -------------------------------
        SCD_TS          CUST_ID    ....
        -------------------------------
        2022-04-12      1000
        2022-04-20      1000
        -------------------------------

        Merged temporary table with LAG function applied with IGNORE NULLS option:

        --------------------------------------------------------------------
        TS            KEY     EFFECTIVE_TS    LAST_TS       ROW_OF_INTEREST
        --------------------------------------------------------------------
        2022-04-10    1000    NULL            NULL          *
        2022-04-12    1000    2022-04-12      NULL
        2022-04-15    1000    NULL            2022-04-12    *
        2022-04-20    1000    2022-04-20      2022-04-12
        2022-04-20    1000    NULL            2022-04-20    *
        --------------------------------------------------------------------

        This query extracts the above table and keeps only the TS, KEY and LAST_TS columns and rows
        of interest (rows that are from the right tables).

        Returns
        -------
        Select
        """

        distinct_left_view = (
            select(quoted_identifier(self.left_timestamp_column), quoted_identifier(self.left_on))
            .distinct()
            .from_(self._left_view_as_table())
        )

        left_ts_and_key = select(
            alias_(quoted_identifier(self.left_timestamp_column), alias=self.TS_COL, quoted=True),
            alias_(quoted_identifier(self.left_on), alias=self.KEY_COL, quoted=True),
            alias_(expressions.NULL, alias=self.EFFECTIVE_TS_COL, quoted=True),
        ).from_(distinct_left_view.subquery())

        right_ts_and_key = select(
            alias_(quoted_identifier(self.right_timestamp_column), alias=self.TS_COL, quoted=True),
            alias_(quoted_identifier(self.right_on), alias=self.KEY_COL, quoted=True),
            alias_(
                quoted_identifier(self.right_timestamp_column),
                alias=self.EFFECTIVE_TS_COL,
                quoted=True,
            ),
        ).from_(self._right_view_as_table())

        all_ts_and_key = expressions.Union(
            this=left_ts_and_key,
            expression=right_ts_and_key,
            distinct=False,
        )

        # Sorting additionally by EFFECTIVE_TS_COL allows exact matching when there are ties between
        # left timestamp and right timestamp.
        #
        # The default behaviour of this sort is NULL LAST, so extra rows from the left table (which
        # have NULL as the EFFECTIVE_TS_COL) come after SCD rows in the right table when there are
        # ties between dates. This way, LAG yields the exact matching date, instead of a previous
        # effective date.
        order = expressions.Order(
            expressions=[
                expressions.Ordered(this=quoted_identifier(self.TS_COL)),
                expressions.Ordered(this=quoted_identifier(self.EFFECTIVE_TS_COL)),
            ]
        )
        matched_effective_timestamp_expr = expressions.Window(
            this=expressions.IgnoreNulls(
                this=expressions.Anonymous(
                    this="LAG", expressions=[quoted_identifier(self.EFFECTIVE_TS_COL)]
                ),
            ),
            partition_by=[quoted_identifier(self.KEY_COL)],
            order=order,
        )

        filter_original_left_view_rows = expressions.Is(
            this=quoted_identifier(self.EFFECTIVE_TS_COL),
            expression=expressions.NULL,
        )
        left_view_with_effective_timestamp_expr = (
            select(
                quoted_identifier(self.TS_COL),
                quoted_identifier(self.KEY_COL),
                quoted_identifier(self.LAST_TS),
            )
            .from_(
                select(
                    quoted_identifier(self.TS_COL),
                    quoted_identifier(self.KEY_COL),
                    alias_(matched_effective_timestamp_expr, alias=self.LAST_TS, quoted=True),
                    quoted_identifier(self.EFFECTIVE_TS_COL),
                )
                .from_(all_ts_and_key.subquery())
                .subquery()
            )
            .where(filter_original_left_view_rows)
        )

        return left_view_with_effective_timestamp_expr

    def _left_view_as_table(self, alias: Optional[str] = None) -> Expression:
        return expressions.Table(this=self.LEFT_VIEW, alias=alias)

    def _right_view_as_table(self, alias: Optional[str] = None) -> Expression:
        return expressions.Table(this=self.RIGHT_VIEW, alias=alias)

    def with_query_impl(self, select_expr: Select) -> Select:
        select_expr = select_expr.with_(self.LEFT_VIEW, self.left_node.sql).with_(
            self.RIGHT_VIEW, self.right_node.sql
        )
        return select_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> Optional[SCDJoin]:
        if context.parameters.get("scd_parameters") is None:
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
        node = SCDJoin(
            context=context,
            columns_map=columns_map,
            left_node=cast(TableNode, context.input_sql_nodes[0]),
            right_node=cast(TableNode, context.input_sql_nodes[1]),
            left_on=parameters["left_on"],
            right_on=parameters["right_on"],
            join_type=parameters["join_type"],
            left_timestamp_column=parameters["scd_parameters"]["left_timestamp_column"],
            right_timestamp_column=parameters["scd_parameters"]["right_timestamp_column"],
            right_input_columns=parameters["right_input_columns"],
            right_output_columns=parameters["right_output_columns"],
        )
        return node
