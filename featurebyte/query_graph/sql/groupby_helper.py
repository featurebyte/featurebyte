"""
Utilities related to SQL generation for groupby operations
"""
from __future__ import annotations

from typing import Optional, cast

from dataclasses import dataclass

from sqlglot import expressions, parse_one
from sqlglot.expressions import Expression, Select, alias_

from featurebyte.enum import AggFunc
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.common import quoted_identifier


@dataclass
class GroupbyColumn:
    """
    Represents a set of parameters that produces one output column in a groupby statement
    """

    agg_func: AggFunc
    parent_expr: Optional[Expression]
    result_name: str


@dataclass
class GroupbyKey:
    """
    Represents a groupby key
    """

    expr: Expression
    name: str

    def get_alias(self) -> Expression:
        """
        Returns an alias expression that maps expr to name

        Returns
        -------
        Expression
        """
        return cast(Expression, alias_(self.expr, self.name, quoted=True))


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


def get_groupby_expr(
    input_expr: Select,
    groupby_keys: list[GroupbyKey],
    groupby_columns: list[GroupbyColumn],
    value_by: Optional[GroupbyKey],
    adapter: BaseAdapter,
) -> Select:
    """
    Construct a GROUP BY statement using the provided expression as input

    Parameters
    ----------
    input_expr: Select
        Input to be aggregated
    groupby_keys: list[GroupbyKey]
        List of groupby keys
    groupby_columns: list[GroupbyColumn]
        List of GroupbyColumn objects specifying the aggregation details (agg_func, input / output
        names)
    value_by: Optional[GroupbyKey]
        Optional category parameter
    adapter: BaseAdapter
        Adapter for generating engine specific expressions

    Returns
    -------
    Select
    """
    agg_exprs = [
        alias_(
            get_aggregation_expression(
                agg_func=column.agg_func,
                input_column=column.parent_expr,
            ),
            alias=column.result_name + ("_inner" if value_by is not None else ""),
            quoted=True,
        )
        for column in groupby_columns
    ]

    select_keys = [k.get_alias() for k in groupby_keys]
    keys = [k.expr for k in groupby_keys]
    if value_by:
        select_keys.append(value_by.get_alias())
        keys.append(value_by.expr)

    groupby_expr = input_expr.select(*select_keys, *agg_exprs).group_by(*keys)

    if value_by is not None:
        groupby_expr = adapter.construct_key_value_aggregation_sql(
            point_in_time_column=None,
            serving_names=[k.name for k in groupby_keys],
            value_by=value_by.name,
            agg_result_names=[col.result_name for col in groupby_columns],
            inner_agg_result_names=[col.result_name + "_inner" for col in groupby_columns],
            inner_agg_expr=groupby_expr,
        )

    return groupby_expr
