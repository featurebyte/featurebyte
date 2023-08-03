"""
Utilities for SCD join / lookup
"""
from __future__ import annotations

from typing import Literal, Optional, cast

from dataclasses import dataclass

import pandas as pd
from sqlglot import expressions, parse_one
from sqlglot.expressions import Expression, Select, alias_, select

from featurebyte.enum import StrEnum
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import get_qualified_column_identifier, quoted_identifier

# Internally used identifiers when constructing SQL
TS_COL = "__FB_TS_COL"
EFFECTIVE_TS_COL = "__FB_EFFECTIVE_TS_COL"
KEY_COL = "__FB_KEY_COL"
LAST_TS = "__FB_LAST_TS"
END_TS = "__FB_END_TS"

# Special column name and values used to handle ties in timestamps between main table and SCD table
TS_TIE_BREAKER_COL = "__FB_TS_TIE_BREAKER_COL"
TS_TIE_BREAKER_VALUE_SCD_TABLE = 1
TS_TIE_BREAKER_VALUE_ALLOW_EXACT_MATCH = 2
TS_TIE_BREAKER_VALUE_DISALLOW_EXACT_MATCH = 0


@dataclass
class Table:
    """
    Representation of a table to be used in SCD join
    """

    expr: str | Select
    timestamp_column: str | Expression
    join_keys: list[str]
    input_columns: list[str]
    output_columns: list[str]

    @property
    def timestamp_column_expr(self) -> Expression:
        """
        Returns an expression for the timestamp column (return as is if it's already an expression,
        else return the quoted column name)

        Returns
        -------
        Expression
        """
        if isinstance(self.timestamp_column, str):
            return quoted_identifier(self.timestamp_column)
        return self.timestamp_column

    def as_subquery(self, alias: Optional[str] = None) -> Expression:
        """
        Returns an expression that can be selected from (converted to subquery if required)

        Parameters
        ----------
        alias: Optional[str]
            Table alias, if specified.

        Returns
        -------
        Expression
        """
        if isinstance(self.expr, str):
            return expressions.Table(this=expressions.Identifier(this=self.expr), alias=alias)
        assert isinstance(self.expr, Select)
        return cast(Expression, self.expr.subquery(alias=alias))


class OffsetDirection(StrEnum):
    """
    Offset direction
    """

    FORWARD = "forward"
    BACKWARD = "backward"


def get_scd_join_expr(
    left_table: Table,
    right_table: Table,
    join_type: Literal["inner", "left"],
    adapter: BaseAdapter,
    select_expr: Optional[Select] = None,
    offset: Optional[str] = None,
    offset_direction: OffsetDirection = OffsetDirection.BACKWARD,
    allow_exact_match: bool = True,
    quote_right_input_columns: bool = True,
    convert_timestamps_to_utc: bool = True,
) -> Select:
    """
    Construct a query to perform SCD join

    The general idea is to first merge the left timestamps (event timestamps) and right
    timestamps (SCD record effective timestamps) along with join keys into a temporary table.
    Then apply a LAG window function on this temporary table to retrieve the latest effective
    timestamp corresponding to each event timestamp in one go.

    Parameters
    ----------
    left_table: Table
        Left table
    right_table: Table
        Right table, usually the SCD table
    join_type: Literal["inner", "left"]
        Join type
    adapter: BaseAdapter
        Instance of BaseAdapter for engine specific sql generation
    select_expr: Optional[Select]
        Partially constructed select expression, if any
    offset: Optional[str]
        Offset to apply when performing SCD join
    offset_direction: OffsetDirection
        Direction of offset
    allow_exact_match: bool
        Whether to allow exact matching effective timestamps to be joined
    quote_right_input_columns: bool
        Whether to quote right table's input columns. Temporary and should be removed after
        https://featurebyte.atlassian.net/browse/DEV-935
    convert_timestamps_to_utc: bool
        Whether timestamps should be converted to UTC for joins

    Returns
    -------
    Select
    """
    if select_expr is None:
        select_expr = select(
            *[
                alias_(
                    get_qualified_column_identifier(input_col, "L"), alias=output_col, quoted=True
                )
                for input_col, output_col in zip(
                    left_table.input_columns, left_table.output_columns
                )
            ],
            *[
                alias_(
                    get_qualified_column_identifier(
                        input_col, "R", quote_column=quote_right_input_columns
                    ),
                    alias=output_col,
                    quoted=True,
                )
                for input_col, output_col in zip(
                    right_table.input_columns, right_table.output_columns
                )
            ],
        )

    left_view_with_last_ts_expr = augment_table_with_effective_timestamp(
        left_table=left_table,
        right_table=right_table,
        adapter=adapter,
        offset=offset,
        offset_direction=offset_direction,
        allow_exact_match=allow_exact_match,
        convert_timestamps_to_utc=convert_timestamps_to_utc,
    )

    left_subquery = left_view_with_last_ts_expr.subquery(alias="L")
    right_subquery = right_table.as_subquery(alias="R")
    assert isinstance(right_table.timestamp_column, str)
    join_conditions = [
        expressions.EQ(
            this=get_qualified_column_identifier(LAST_TS, "L"),
            expression=get_qualified_column_identifier(right_table.timestamp_column, "R"),
        ),
    ] + _key_cols_equality_conditions(right_table.join_keys)

    select_expr = select_expr.from_(left_subquery).join(
        right_subquery,
        join_type=join_type,
        on=expressions.and_(*join_conditions),
    )
    return select_expr


def _convert_to_utc_ntz(
    col_expr: expressions.Expression, adapter: BaseAdapter
) -> expressions.Expression:
    """
    Convert timestamp expression to UTC values and NTZ timestamps

    Parameters
    ----------
    col_expr: expressions.Expression
        Timestamp expression to convert
    adapter: BaseAdapter
        Instance of BaseAdapter for engine specific sql generation

    Returns
    -------
    expressions.Expression
    """
    utc_ts_expr = adapter.convert_to_utc_timestamp(col_expr)
    return expressions.Cast(this=utc_ts_expr, to=parse_one("TIMESTAMP"))


def augment_table_with_effective_timestamp(  # pylint: disable=too-many-locals
    left_table: Table,
    right_table: Table,
    adapter: BaseAdapter,
    offset: Optional[str],
    offset_direction: OffsetDirection,
    allow_exact_match: bool = True,
    convert_timestamps_to_utc: bool = True,
) -> Select:
    """
    This constructs a query that calculates the corresponding SCD effective date for each row in the
    left table. See an example below.

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

    --------------------------------------------------------------------------
    TS            KEY     EFFECTIVE_TS    LAST_TS       ROW_OF_INTEREST    ...
    --------------------------------------------------------------------------
    2022-04-10    1000    NULL            NULL          *
    2022-04-12    1000    2022-04-12      NULL
    2022-04-15    1000    NULL            2022-04-12    *
    2022-04-20    1000    2022-04-20      2022-04-12
    2022-04-20    1000    NULL            2022-04-20    *
    --------------------------------------------------------------------------

    This query extracts the above table and keeps only the rows of interest (rows that are from
    the left table). The resulting table has the same number of rows as the original left
    table, and contains the columns specified in left_input_columns renamed as
    left_output_columns.

    Parameters
    ----------
    left_table: Table
        Left table
    right_table: Table
        Right table, usually the SCD table
    adapter: BaseAdapter
        Instance of BaseAdapter for engine specific sql generation
    offset: Optional[str]
        Offset to apply when performing SCD join
    offset_direction: OffsetDirection
        Direction of offset
    allow_exact_match: bool
        Whether to allow exact matching effective timestamps to be joined
    convert_timestamps_to_utc: bool
        Whether timestamps should be converted to UTC for joins

    Returns
    -------
    Select
    """
    # Adjust left timestamps if offset is provided
    if offset:
        offset_seconds = pd.Timedelta(offset).total_seconds()
        direction_adjustment_multiplier = -1 if offset_direction == OffsetDirection.BACKWARD else 1
        left_ts_col = adapter.dateadd_microsecond(
            make_literal_value(offset_seconds * 1e6 * direction_adjustment_multiplier),
            left_table.timestamp_column_expr,
        )
    else:
        left_ts_col = left_table.timestamp_column_expr
    right_ts_col = right_table.timestamp_column_expr
    if convert_timestamps_to_utc:
        left_ts_col = _convert_to_utc_ntz(left_ts_col, adapter)
        right_ts_col = _convert_to_utc_ntz(right_ts_col, adapter)

    # Left table. Set up special columns: TS_COL, KEY_COL, EFFECTIVE_TS_COL and TS_TIE_BREAKER_COL
    left_view_with_ts_and_key = select(
        alias_(left_ts_col, alias=TS_COL, quoted=True),
        *_alias_join_keys_as_key_cols(left_table.join_keys),
        alias_(expressions.NULL, alias=EFFECTIVE_TS_COL, quoted=True),
        alias_(
            make_literal_value(
                TS_TIE_BREAKER_VALUE_ALLOW_EXACT_MATCH
                if allow_exact_match
                else TS_TIE_BREAKER_VALUE_DISALLOW_EXACT_MATCH
            ),
            alias=TS_TIE_BREAKER_COL,
            quoted=True,
        ),
    ).from_(left_table.as_subquery())

    # Include all columns specified for the left table
    for input_col, output_col in zip(left_table.input_columns, left_table.output_columns):
        left_view_with_ts_and_key = left_view_with_ts_and_key.select(
            alias_(quoted_identifier(input_col), alias=output_col, quoted=True)
        )

    # Right table. Set up the same special columns. The ordering of the columns in the SELECT
    # statement matters (must match the left table's).
    right_ts_and_key = select(
        alias_(right_ts_col, alias=TS_COL, quoted=True),
        *_alias_join_keys_as_key_cols(right_table.join_keys),
        alias_(
            right_table.timestamp_column_expr,
            alias=EFFECTIVE_TS_COL,
            quoted=True,
        ),
        alias_(
            make_literal_value(TS_TIE_BREAKER_VALUE_SCD_TABLE),
            alias=TS_TIE_BREAKER_COL,
            quoted=True,
        ),
    ).from_(right_table.as_subquery())

    # Include all columns specified for the right table, but simply set them as NULL.
    for column in left_table.output_columns:
        right_ts_and_key = right_ts_and_key.select(
            alias_(expressions.NULL, alias=column, quoted=True)
        )

    # Merge the above two temporary tables into one
    all_ts_and_key = expressions.Union(
        this=left_view_with_ts_and_key,
        expression=right_ts_and_key,
        distinct=False,
    )

    # Sorting additionally by TS_TIE_BREAKER_COL to respect the specified exact matching behaviour.
    #
    # When allow_exact_match=True and TS_COL ties, after the sorting in LAG, rows from the left
    # table will come after SCD rows in the right table since left rows have a larger value for
    # TS_TILE_BREAKER_VALUE (2) than right rows (always 1). This way, LAG yields the exact matching
    # date, instead of a previous effective date. Vice versa for allow_exact_match=False.
    order = expressions.Order(
        expressions=[
            expressions.Ordered(this=quoted_identifier(TS_COL)),
            expressions.Ordered(this=quoted_identifier(TS_TIE_BREAKER_COL)),
        ]
    )
    num_join_keys = len(left_table.join_keys)
    matched_effective_timestamp_expr = expressions.Window(
        this=expressions.IgnoreNulls(
            this=expressions.Anonymous(
                this="LAG", expressions=[quoted_identifier(EFFECTIVE_TS_COL)]
            ),
        ),
        partition_by=_key_cols_as_quoted_identifiers(num_join_keys),
        order=order,
    )

    # Need to use a nested query for this filter due to the LAG window function
    filter_original_left_view_rows = expressions.Is(
        this=quoted_identifier(EFFECTIVE_TS_COL),
        expression=expressions.NULL,
    )

    left_view_with_effective_timestamp_expr = (
        select(
            *_key_cols_as_quoted_identifiers(num_join_keys),
            quoted_identifier(LAST_TS),
            *[quoted_identifier(col) for col in left_table.output_columns],
        )
        .from_(
            select(
                *_key_cols_as_quoted_identifiers(num_join_keys),
                alias_(matched_effective_timestamp_expr, alias=LAST_TS, quoted=True),
                *[quoted_identifier(col) for col in left_table.output_columns],
                quoted_identifier(EFFECTIVE_TS_COL),
            )
            .from_(all_ts_and_key.subquery())
            .subquery()
        )
        .where(filter_original_left_view_rows)
    )

    return left_view_with_effective_timestamp_expr


def _alias_join_keys_as_key_cols(join_keys: list[str]) -> list[Expression]:
    """
    Alias join keys as internal key columns (e.g. "CUST_ID" AS "__FB_KEY_COL_0")

    Parameters
    ----------
    join_keys: list[str]
        List of join keys

    Returns
    -------
    list[Expression]
    """
    return [
        alias_(quoted_identifier(join_key), alias=f"{KEY_COL}_{i}", quoted=True)
        for (i, join_key) in enumerate(join_keys)
    ]


def _key_cols_as_quoted_identifiers(num_join_keys: int) -> list[Expression]:
    """
    Get a list of quoted internal key columns (e.g. "__FB_KEY_COL_0")

    Parameters
    ----------
    num_join_keys: int
        Length of join keys

    Returns
    -------
    list[Expression]
    """
    return [quoted_identifier(f"{KEY_COL}_{i}") for i in range(num_join_keys)]


def _key_cols_equality_conditions(right_table_join_keys: list[str]) -> list[expressions.EQ]:
    """
    Get a list of equality conditions used for joining with scd view

    (e.g. L."__FB_KEY_COL_0" = R."CUST_ID")

    Parameters
    ----------
    right_table_join_keys: list[str]
        Join keys in the right table, will be prefixed with table alias "R"

    Returns
    -------
    list[expressions.EQ]
    """
    return [
        expressions.EQ(
            this=get_qualified_column_identifier(f"{KEY_COL}_{i}", "L"),
            expression=get_qualified_column_identifier(join_key, "R"),
        )
        for (i, join_key) in enumerate(right_table_join_keys)
    ]


def augment_scd_table_with_end_timestamp(
    table_expr: Select,
    effective_timestamp_column: str,
    natural_key_column: str,
) -> Select:
    """
    Augment a given SCD table with end timestamp column (the timestamp when an SCD record becomes
    ineffective because of the next SCD record)

    Parameters
    ----------
    table_expr: Select
        Select statement representing the SCD table
    effective_timestamp_column: str
        Effective timestamp column name
    natural_key_column: str
        Natural key column name

    Returns
    -------
    Select
    """

    order = expressions.Order(
        expressions=[
            expressions.Ordered(this=quoted_identifier(effective_timestamp_column)),
        ]
    )
    end_timestamp_expr = expressions.Window(
        this=expressions.Anonymous(
            this="LEAD", expressions=[quoted_identifier(effective_timestamp_column)]
        ),
        partition_by=[quoted_identifier(natural_key_column)],
        order=order,
    )
    updated_table_expr = select(
        "*",
        alias_(end_timestamp_expr, alias=END_TS, quoted=True),
    ).from_(table_expr.subquery())

    return updated_table_expr
