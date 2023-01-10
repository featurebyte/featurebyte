"""
Utilities related to SQL generation for groupby operations
"""
from __future__ import annotations

from typing import Optional, cast

from sqlglot import expressions, parse_one
from sqlglot.expressions import Expression

from featurebyte.enum import AggFunc
from featurebyte.query_graph.sql.common import quoted_identifier


def get_aggregation_expression(
    agg_func: AggFunc, input_column: Optional[str | Expression]
) -> Expression:
    """
    Convert an AggFunc and input column name to a SQL expression to be used in GROUP BY

    Parameters
    ----------
    agg_func : AggFunc
        Aggregation function
    input_column : str
        Input column name

    Returns
    -------
    Expression
    """
    if input_column is not None:
        if isinstance(input_column, str):
            input_column_expr = quoted_identifier(input_column)
        else:
            input_column_expr = input_column
    else:
        input_column_expr = None

    agg_func_sql_mapping = {
        AggFunc.SUM: "SUM",
        AggFunc.AVG: "AVG",
        AggFunc.MIN: "MIN",
        AggFunc.MAX: "MAX",
        AggFunc.STD: "STDDEV",
    }
    expr: Expression
    if agg_func in agg_func_sql_mapping:
        assert input_column_expr is not None
        sql_func = agg_func_sql_mapping[agg_func]
        expr = expressions.Anonymous(this=sql_func, expressions=[input_column_expr])
    else:
        if agg_func == AggFunc.COUNT:
            expr = cast(Expression, parse_one("COUNT(*)"))
        else:
            # Must be NA_COUNT
            assert agg_func == AggFunc.NA_COUNT
            assert input_column_expr is not None
            expr_is_null = expressions.Is(this=input_column_expr, expression=expressions.NULL)
            expr = expressions.Sum(
                this=expressions.Cast(this=expr_is_null, to=parse_one("INTEGER"))
            )
    return expr
