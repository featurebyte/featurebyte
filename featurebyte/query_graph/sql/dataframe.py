"""
Module for sql generation given a DataFrame (typically request data)
"""

from __future__ import annotations

from typing import cast

import pandas as pd
from sqlglot import expressions
from sqlglot.expressions import Expression, select

from featurebyte.query_graph.sql.ast.literal import make_literal_value


def construct_dataframe_sql_expr(
    request_dataframe: pd.DataFrame, date_cols: list[str]
) -> expressions.Select:
    """Construct a SELECT statement that uploads the request data

    This does not use write_pandas and should only be used for small request data (e.g. request data
    during preview that has only one row)

    Parameters
    ----------
    request_dataframe : DataFrame
        Request dataframe
    date_cols : list[str]
        List of date columns

    Returns
    -------
    expressions.Select
    """
    row_exprs = []
    for _, row in request_dataframe.iterrows():
        columns = []
        for col, value in row.items():
            cast_as_timestamp = col in date_cols
            expr = make_literal_value(value, cast_as_timestamp)
            columns.append(expressions.alias_(expr, expressions.Identifier(this=col, quoted=True)))
        row_expr = select(*columns)
        row_exprs.append(row_expr)

    # The Union expression is nested, and has the first row's expression at the outermost
    # layer. So, start from the last row's expression and build from bottom up.
    row_exprs_reversed = row_exprs[::-1]
    union_expr: Expression = row_exprs_reversed[0]
    for row_expr in row_exprs_reversed[1:]:
        union_expr = expressions.Union(this=row_expr, distinct=False, expression=union_expr)

    return cast(expressions.Select, union_expr)
