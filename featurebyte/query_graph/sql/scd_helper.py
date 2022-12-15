"""
Utilities for SCD join / lookup
"""
from __future__ import annotations

from typing import Literal, Optional

from dataclasses import dataclass

import pandas as pd
from sqlglot import expressions
from sqlglot.expressions import Select, alias_, select

from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import get_qualified_column_identifier, quoted_identifier

# Internally used identifiers when constructing SQL
TS_COL = "__FB_TS_COL"
ORIGINAL_TS_COL = "__FB_ORIGINAL_TS_COL"
EFFECTIVE_TS_COL = "__FB_EFFECTIVE_TS_COL"
KEY_COL = "__FB_KEY_COL"
LAST_TS = "__FB_LAST_TS"


@dataclass
class Table:
    """
    Representation of a table to be used in SCD join
    """

    expr: Select
    timestamp_column: str
    join_key: str
    input_columns: list[str]
    output_columns: list[str]


def get_scd_join_expr(
    left_table: Table,
    right_table: Table,
    join_type: Literal["inner", "left"],
    adapter: BaseAdapter,
    select_expr: Optional[Select] = None,
    offset: Optional[str] = None,
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
    select_expr: Optional[Select]
        Partially constructed select expression, if any

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
                    get_qualified_column_identifier(input_col, "R"), alias=output_col, quoted=True
                )
                for input_col, output_col in zip(
                    right_table.input_columns, right_table.output_columns
                )
            ],
        )

    left_view_with_last_ts_expr = augment_table_with_effective_timestamp(
        left_table=left_table, right_table=right_table, adapter=adapter, offset=offset
    )

    left_subquery = left_view_with_last_ts_expr.subquery(alias="L")
    right_subquery = right_table.expr.subquery(alias="R")
    join_conditions = [
        expressions.EQ(
            this=get_qualified_column_identifier(LAST_TS, "L"),
            expression=get_qualified_column_identifier(right_table.timestamp_column, "R"),
        ),
        expressions.EQ(
            this=get_qualified_column_identifier(KEY_COL, "L"),
            expression=get_qualified_column_identifier(right_table.join_key, "R"),
        ),
    ]

    select_expr = select_expr.from_(left_subquery).join(
        right_subquery,
        join_type=join_type,
        on=expressions.and_(*join_conditions),
    )
    return select_expr


def augment_table_with_effective_timestamp(
    left_table: Table,
    right_table: Table,
    adapter: BaseAdapter,
    offset: Optional[str],
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

    Returns
    -------
    Select
    """
    if offset:
        offset_seconds = pd.Timedelta(offset).total_seconds()
        left_ts_col = adapter.dateadd_microsecond(
            make_literal_value(offset_seconds * 1e6), quoted_identifier(left_table.timestamp_column)
        )
    else:
        left_ts_col = quoted_identifier(left_table.timestamp_column)

    # Left table. Set up three special columns: TS_COL, KEY_COL and EFFECTIVE_TS_COL
    left_view_with_ts_and_key = select(
        alias_(left_ts_col, alias=TS_COL, quoted=True),
        alias_(quoted_identifier(left_table.join_key), alias=KEY_COL, quoted=True),
        alias_(expressions.NULL, alias=EFFECTIVE_TS_COL, quoted=True),
    ).from_(left_table.expr.subquery())

    if offset:
        left_view_with_ts_and_key = left_view_with_ts_and_key.select(
            alias_(
                quoted_identifier(left_table.timestamp_column), alias=ORIGINAL_TS_COL, quoted=True
            )
        )

    # Include all columns specified for the left table
    for input_col, output_col in zip(left_table.input_columns, left_table.output_columns):
        left_view_with_ts_and_key = left_view_with_ts_and_key.select(
            alias_(quoted_identifier(input_col), alias=output_col, quoted=True)
        )

    # Right table. Set up the same special columns: TS_COL, KEY_COL and EFFECTIVE_TS_COL. The
    # ordering of the columns in the SELECT statement matters (must match the left table's).
    right_ts_and_key = select(
        alias_(quoted_identifier(right_table.timestamp_column), alias=TS_COL, quoted=True),
        alias_(quoted_identifier(right_table.join_key), alias=KEY_COL, quoted=True),
        alias_(
            quoted_identifier(right_table.timestamp_column),
            alias=EFFECTIVE_TS_COL,
            quoted=True,
        ),
    ).from_(right_table.expr.subquery())

    if offset:
        right_ts_and_key = right_ts_and_key.select(
            alias_(expressions.NULL, alias=ORIGINAL_TS_COL, quoted=True)
        )

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

    # Sorting additionally by EFFECTIVE_TS_COL allows exact matching when there are ties between
    # left timestamp and right timestamp.
    #
    # The default behaviour of this sort is NULL LAST, so extra rows from the left table (which
    # have NULL as the EFFECTIVE_TS_COL) come after SCD rows in the right table when there are
    # ties between dates. This way, LAG yields the exact matching date, instead of a previous
    # effective date.
    order = expressions.Order(
        expressions=[
            expressions.Ordered(this=quoted_identifier(TS_COL)),
            expressions.Ordered(this=quoted_identifier(EFFECTIVE_TS_COL)),
        ]
    )
    matched_effective_timestamp_expr = expressions.Window(
        this=expressions.IgnoreNulls(
            this=expressions.Anonymous(
                this="LAG", expressions=[quoted_identifier(EFFECTIVE_TS_COL)]
            ),
        ),
        partition_by=[quoted_identifier(KEY_COL)],
        order=order,
    )

    # Need to use a nested query for this filter due to the LAG window function
    filter_original_left_view_rows = expressions.Is(
        this=quoted_identifier(EFFECTIVE_TS_COL),
        expression=expressions.NULL,
    )

    # If offset was applied, select the original column instead of the shifted timestamp
    if offset:
        inner_ts_col_expr = alias_(quoted_identifier(ORIGINAL_TS_COL), alias=TS_COL, quoted=True)
    else:
        inner_ts_col_expr = quoted_identifier(TS_COL)

    left_view_with_effective_timestamp_expr = (
        select(
            quoted_identifier(TS_COL),
            quoted_identifier(KEY_COL),
            quoted_identifier(LAST_TS),
            *[quoted_identifier(col) for col in left_table.output_columns],
        )
        .from_(
            select(
                inner_ts_col_expr,
                quoted_identifier(KEY_COL),
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
