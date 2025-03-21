"""
Utilities for SCD join / lookup
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional, cast

from sqlglot import expressions
from sqlglot.expressions import Expression, Identifier, Select, alias_, select

from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import get_qualified_column_identifier, quoted_identifier

# Internally used identifiers when constructing SQL
from featurebyte.query_graph.sql.deduplication import get_deduplicated_expr
from featurebyte.query_graph.sql.offset import OffsetDirection, add_offset_to_timestamp
from featurebyte.query_graph.sql.timestamp_helper import convert_timestamp_to_utc

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

    expr: str | Expression
    timestamp_column: str | Expression
    timestamp_schema: Optional[TimestampSchema]
    join_keys: list[str]
    input_columns: list[str]
    output_columns: list[str]
    end_timestamp_column: Optional[str] = None
    end_timestamp_schema: Optional[TimestampSchema] = None

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

    def as_subquery(
        self,
        alias: Optional[str] = None,
        remove_missing_timestamp_values: bool = False,
    ) -> Expression:
        """
        Returns an expression that can be selected from (converted to subquery if required)

        Parameters
        ----------
        alias: Optional[str]
            Table alias, if specified.
        remove_missing_timestamp_values: bool
            Whether to filter out missing values in timestamp column in the returned subquery

        Returns
        -------
        Expression
        """
        if isinstance(self.expr, str):
            # This is when joining with tile tables. We can assume that tile index (the timestamp
            # column) will not have any missing values, so remove_missing_timestamp_values is
            # ignored.
            return expressions.Table(this=Identifier(this=self.expr), alias=alias)
        if isinstance(self.expr, Identifier):
            return expressions.Table(this=self.expr, alias=alias)
        assert isinstance(self.expr, Select)
        if remove_missing_timestamp_values:
            expr = self.expr_with_non_missing_timestamp_values
        else:
            expr = self.expr
        return cast(Expression, expr.subquery(alias=alias, copy=False))

    @property
    def expr_with_non_missing_timestamp_values(self) -> Select:
        """
        Get an expression for the table with missing timestamp values removed

        Returns
        -------
        Select
        """
        assert isinstance(self.expr, Select)
        assert isinstance(self.timestamp_column, str)
        return self.expr.where(
            expressions.Is(
                this=quoted_identifier(self.timestamp_column),
                expression=expressions.Not(this=expressions.Null()),
            )
        )


def has_end_timestamp_column(table: Table) -> bool:
    """
    Check if the table has an end timestamp column

    Parameters
    ----------
    table: Table
        Table to check

    Returns
    -------
    bool
    """
    if table.end_timestamp_column:
        if isinstance(table.expr, Select):
            for col_expr in table.expr.expressions:
                col_name = col_expr.alias or col_expr.name
                if col_name == table.end_timestamp_column:
                    return True
    return False


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
        include_ts_col=right_table.end_timestamp_column is not None,
    )

    left_subquery = left_view_with_last_ts_expr.subquery(alias="L")

    # Ensure right table (scd side) is unique in the join columns so that the join preserve the
    # number of rows in the left table
    assert isinstance(right_table.timestamp_column, str)
    if isinstance(right_table.expr, Select):
        # right_table.expr is a Select instance if it is a user provided SCD table.
        deduplicated_expr = get_deduplicated_expr(
            adapter=adapter,
            table_expr=right_table.expr_with_non_missing_timestamp_values,
            expected_primary_keys=[right_table.timestamp_column] + right_table.join_keys,
        )
        right_table = Table(
            expr=deduplicated_expr,
            timestamp_column=right_table.timestamp_column,
            timestamp_schema=right_table.timestamp_schema,
            join_keys=right_table.join_keys,
            input_columns=right_table.input_columns,
            output_columns=right_table.output_columns,
            end_timestamp_column=right_table.end_timestamp_column,
            end_timestamp_schema=right_table.end_timestamp_schema,
        )
    right_subquery = right_table.as_subquery(alias="R")

    assert isinstance(right_table.timestamp_column, str)
    join_conditions = [
        expressions.EQ(
            this=get_qualified_column_identifier(LAST_TS, "L"),
            expression=get_qualified_column_identifier(right_table.timestamp_column, "R"),
        ),
    ] + _key_cols_equality_conditions(right_table.join_keys)

    if has_end_timestamp_column(right_table):
        assert right_table.end_timestamp_column is not None
        end_timestamp_expr = get_qualified_column_identifier(right_table.end_timestamp_column, "R")
        end_timestamp_expr_is_null = expressions.Is(
            this=end_timestamp_expr,
            expression=expressions.Null(),
        )
        if convert_timestamps_to_utc:
            end_timestamp_expr = _convert_to_utc_ntz(
                end_timestamp_expr, right_table.end_timestamp_schema, adapter
            )
        record_validity_expr = expressions.Or(
            this=expressions.LT(
                this=adapter.normalize_timestamp_before_comparison(
                    get_qualified_column_identifier(TS_COL, "L")
                ),
                expression=adapter.normalize_timestamp_before_comparison(end_timestamp_expr),
            ),
            expression=end_timestamp_expr_is_null,
        )
        join_conditions.append(record_validity_expr)  # type: ignore

    select_expr = select_expr.from_(left_subquery, copy=False).join(
        right_subquery,
        join_type=join_type,
        on=expressions.and_(*join_conditions),
        copy=False,
    )
    return select_expr


def _convert_to_utc_ntz(
    col_expr: expressions.Expression,
    timestamp_schema: Optional[TimestampSchema],
    adapter: BaseAdapter,
) -> expressions.Expression:
    """
    Convert timestamp expression to UTC values and NTZ timestamps

    Parameters
    ----------
    col_expr: expressions.Expression
        Timestamp expression to convert
    timestamp_schema: TimestampSchema
        Timestamp schema
    adapter: BaseAdapter
        Instance of BaseAdapter for engine specific sql generation

    Returns
    -------
    expressions.Expression
    """
    if timestamp_schema is None:
        return adapter.convert_to_utc_timestamp(col_expr)
    return convert_timestamp_to_utc(
        column_expr=col_expr,
        timestamp_schema=timestamp_schema,
        adapter=adapter,
    )


def augment_table_with_effective_timestamp(
    left_table: Table,
    right_table: Table,
    adapter: BaseAdapter,
    offset: Optional[str],
    offset_direction: OffsetDirection,
    allow_exact_match: bool = True,
    convert_timestamps_to_utc: bool = True,
    include_ts_col: bool = False,
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
    include_ts_col: bool
        Whether to include the timestamp column in the output

    Returns
    -------
    Select
    """
    # Adjust left timestamps if offset is provided
    if offset:
        left_ts_col = add_offset_to_timestamp(
            adapter=adapter,
            timestamp_expr=left_table.timestamp_column_expr,
            offset=offset,
            offset_direction=offset_direction,
        )
    else:
        left_ts_col = left_table.timestamp_column_expr
    right_ts_col = right_table.timestamp_column_expr
    if convert_timestamps_to_utc:
        left_ts_col = _convert_to_utc_ntz(left_ts_col, left_table.timestamp_schema, adapter)
        right_ts_col = _convert_to_utc_ntz(right_ts_col, right_table.timestamp_schema, adapter)

    # Left table. Set up special columns: TS_COL, KEY_COL, EFFECTIVE_TS_COL and TS_TIE_BREAKER_COL
    left_view_with_ts_and_key = select(
        alias_(left_ts_col, alias=TS_COL, quoted=True),
        *_alias_join_keys_as_key_cols(left_table.join_keys),
        alias_(expressions.Null(), alias=EFFECTIVE_TS_COL, quoted=True),
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
            alias_(quoted_identifier(input_col), alias=output_col, quoted=True), copy=False
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
    ).from_(right_table.as_subquery(remove_missing_timestamp_values=True), copy=False)

    # Include all columns specified for the right table, but simply set them as NULL.
    for column in left_table.output_columns:
        right_ts_and_key = right_ts_and_key.select(
            alias_(expressions.Null(), alias=column, quoted=True), copy=False
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
            expressions.Ordered(
                this=expressions.Column(this=quoted_identifier(TS_COL)), nulls_first=True
            ),
            expressions.Ordered(this=quoted_identifier(TS_TIE_BREAKER_COL)),
        ]
    )
    num_join_keys = len(left_table.join_keys)
    matched_effective_timestamp_expr = adapter.lag_ignore_nulls(
        expr=quoted_identifier(EFFECTIVE_TS_COL),
        partition_by=_key_cols_as_quoted_identifiers(num_join_keys),
        order=order,
    )

    # Need to use a nested query for this filter due to the LAG window function
    filter_original_left_view_rows = expressions.Is(
        this=quoted_identifier(EFFECTIVE_TS_COL),
        expression=expressions.Null(),
    )

    left_view_with_effective_timestamp_expr = (
        select(
            *_key_cols_as_quoted_identifiers(num_join_keys),
            quoted_identifier(LAST_TS),
            *[quoted_identifier(TS_COL)] if include_ts_col else [],
            *[quoted_identifier(col) for col in left_table.output_columns],
        )
        .from_(
            select(
                *_key_cols_as_quoted_identifiers(num_join_keys),
                alias_(matched_effective_timestamp_expr, alias=LAST_TS, quoted=True),
                *[quoted_identifier(TS_COL)] if include_ts_col else [],
                *[quoted_identifier(col) for col in left_table.output_columns],
                quoted_identifier(EFFECTIVE_TS_COL),
            )
            .from_(all_ts_and_key.subquery(copy=False))
            .subquery(copy=False)
        )
        .where(filter_original_left_view_rows, copy=False)
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
    adapter: BaseAdapter,
    table_expr: Select,
    effective_timestamp_column: str,
    effective_timestamp_schema: Optional[TimestampSchema],
    natural_key_column: str,
) -> Select:
    """
    Augment a given SCD table with end timestamp column (the timestamp when an SCD record becomes
    ineffective because of the next SCD record)

    Parameters
    ----------
    adapter: BaseAdapter
        SQL adapter
    table_expr: Select
        Select statement representing the SCD table
    effective_timestamp_column: str
        Effective timestamp column name
    effective_timestamp_schema: Optional[TimestampSchema]
        Effective timestamp schema
    natural_key_column: str
        Natural key column name

    Returns
    -------
    Select
    """
    effective_timestamp_expr = quoted_identifier(effective_timestamp_column)
    if effective_timestamp_schema is not None:
        effective_timestamp_expr = convert_timestamp_to_utc(
            column_expr=effective_timestamp_expr,
            timestamp_schema=effective_timestamp_schema,
            adapter=adapter,
        )
    order = expressions.Order(
        expressions=[
            expressions.Ordered(this=effective_timestamp_expr),
        ]
    )
    end_timestamp_expr = expressions.Window(
        this=expressions.Anonymous(this="LEAD", expressions=[effective_timestamp_expr]),
        partition_by=[quoted_identifier(natural_key_column)],
        order=order,
    )
    updated_table_expr = select(
        "*",
        alias_(end_timestamp_expr, alias=END_TS, quoted=True),
    ).from_(table_expr.subquery())

    return updated_table_expr
