"""
Helpers to perform range join between two tables
"""

from __future__ import annotations

from typing import Any, List, Set

from pydantic import ConfigDict, Field
from sqlglot import expressions
from sqlglot.expressions import Expression, Select, select

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import get_qualified_column_identifier


class BaseTable(FeatureByteBaseModel):
    """
    Common attributes of a Table used in range join
    """

    name: Expression  # name is an Expression to allow caller to decide on the quoting behaviour
    alias: str
    join_keys: list[str]
    columns: list[str]
    disable_quote_columns: Set[str] = Field(default_factory=set)

    # pydantic model configuration
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    def get_qualified_column(self, column_name: str) -> Expression:
        """
        Get an expression of the column name qualified by table alias. Column name is always quoted
        unless when specified in disable_quote_columns.

        Parameters
        ----------
        column_name: str
            Column name

        Returns
        -------
        Expression
        """
        return get_qualified_column_identifier(
            column_name, self.alias, quote_column=column_name not in self.disable_quote_columns
        )

    @property
    def qualified_join_keys(self) -> List[Expression]:
        """
        Get expressions for the qualified join keys

        Returns
        -------
        List[Expression]
        """
        return [self.get_qualified_column(key) for key in self.join_keys]

    @property
    def qualified_columns(self) -> List[Expression]:
        """
        Get expressions for the qualified columns

        Returns
        -------
        List[Expression]
        """
        return [self.get_qualified_column(key) for key in self.columns]


class LeftTable(BaseTable):
    """
    Representation of the left table (typically the request table) in a range join
    """

    range_start: str
    range_end: str
    columns: list[str]

    @property
    def qualified_range_start(self) -> Expression:
        """
        Get an expression for the qualified range start column

        Returns
        -------
        Expression
        """
        return self.get_qualified_column(self.range_start)

    @property
    def qualified_range_end(self) -> Expression:
        """
        Get an expression for the qualified range end column

        Returns
        -------
        Expression
        """
        return self.get_qualified_column(self.range_end)


class RightTable(BaseTable):
    """
    Representation of the right table (e.g. a tile table) in a range join
    """

    range_column: str

    @property
    def qualified_range_column(self) -> Expression:
        """
        Get an expression for the range column

        Returns
        -------
        Expression
        """
        return self.get_qualified_column(self.range_column)


def range_join_tables(
    left_table: LeftTable,
    right_table: RightTable,
    window_size: int,
    as_subquery: bool = True,
) -> Select:
    """
    Join two tables with range join.

    Each row in the left table defines a range start and range end. For each row in the left table,
    we want to join it with rows in the right table with range column is between range start and
    range end.

    Parameters
    ----------
    left_table: LeftTable
        Left table in the range join, e.g. the request table
    right_table: RightTable
        Right table in the range join, e.g. the tile table
    window_size: int
        Window size of the range join
    as_subquery: bool
        Whether to return the result as a subquery or not. If True, the result will be returned
        as a subquery. Otherwise, it will be returned as a table.

    Returns
    -------
    Select
    """

    # Ultimate condition:
    # RIGHT.RANGE_COLUMN >= LEFT.RANGE_START AND RIGHT.RANGE_COLUMN < LEFT.RANGE_END
    range_join_where_conditions = [
        expressions.GTE(
            this=right_table.qualified_range_column,
            expression=left_table.qualified_range_start,
        ),
        expressions.LT(
            this=right_table.qualified_range_column,
            expression=left_table.qualified_range_end,
        ),
    ]

    # We can narrow down matches using joins without inequality conditions to make the query
    # efficient. The desired output rows must statisfy either of these conditions:
    #
    # 1. FLOOR(LEFT.RANGE_END / WINDOW) = FLOOR(RIGHT.RANGE_COLUMN / WINDOW)
    # 2. FLOOR(LEFT.RANGE_END / WINDOW) - 1 = FLOOR(RIGHT.RANGE_COLUMN / WINDOW)
    left_range_end_div_window = expressions.Floor(
        this=expressions.Div(
            this=left_table.qualified_range_end, expression=make_literal_value(window_size)
        ),
    )
    right_range_div_window = expressions.Floor(
        this=expressions.Div(
            this=right_table.qualified_range_column, expression=make_literal_value(window_size)
        )
    )
    range_join_conditions = [
        expressions.EQ(  # Condition 1
            this=left_range_end_div_window, expression=right_range_div_window
        ),
        expressions.EQ(  # Condition 2
            this=expressions.Sub(this=left_range_end_div_window, expression=make_literal_value(1)),
            expression=right_range_div_window,
        ),
    ]

    # We will use two joins, each with one of the conditions, and then union the result
    req_joined_with_tiles = None
    for range_join_condition in range_join_conditions:
        join_conditions_lst: Any = [range_join_condition]
        for left_key, right_key in zip(
            left_table.qualified_join_keys, right_table.qualified_join_keys
        ):
            join_conditions_lst.append(
                expressions.EQ(this=left_key, expression=right_key),
            )
        joined_expr = (
            select(
                *left_table.qualified_columns,
                *right_table.qualified_columns,
            )
            .from_(
                expressions.Table(
                    this=left_table.name, alias=expressions.TableAlias(this=left_table.alias)
                )
            )
            .join(
                expressions.Table(
                    this=right_table.name, alias=expressions.TableAlias(this=right_table.alias)
                ),
                join_type="inner",
                on=expressions.and_(*join_conditions_lst),
                copy=False,
            )
            .where(*range_join_where_conditions, copy=False)
        )
        # Use UNION ALL with two separate joins to avoid non-exact join condition with OR which
        # has significant performance impact.
        if req_joined_with_tiles is None:
            req_joined_with_tiles = joined_expr
        else:
            req_joined_with_tiles = expressions.Union(  # type: ignore[unreachable]
                this=req_joined_with_tiles,
                distinct=False,
                expression=joined_expr,
            )
    assert req_joined_with_tiles is not None
    if as_subquery:
        return select().from_(req_joined_with_tiles.subquery(copy=False))
    return req_joined_with_tiles
