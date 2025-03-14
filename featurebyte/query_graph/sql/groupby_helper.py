"""
Utilities related to SQL generation for groupby operations
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, cast

from sqlglot import expressions, parse_one
from sqlglot.expressions import Expression, Select, alias_, select

from featurebyte.enum import AggFunc, DBVarType, SourceType
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.adapter.base import VectorAggColumn
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import get_qualified_column_identifier, quoted_identifier
from featurebyte.query_graph.sql.vector_helper import should_use_element_wise_vector_aggregation

ROW_NUMBER = "__FB_GROUPBY_HELPER_ROW_NUMBER"


@dataclass
class GroupbyColumn:
    """
    Represents a set of parameters that produces one output column in a groupby statement
    """

    agg_func: AggFunc
    # parent_expr refers to the expression that is used to generate the column. For example, `sum(col1)`.
    # This is not provided for certain aggregations like count.
    parent_expr: Optional[Expression]
    # parent_dtype is the dtype of the parent column.
    parent_dtype: Optional[DBVarType]
    # result_name is the aliased name of the aggregation result. For example if we want `sum(col1) as "sum_col_1",
    # the result_name is "sum_col_1".
    result_name: str
    # parent_cols refers to the columns used in the parent_expr. As an example, for the aggregation `sum(col1)`, the
    # parent_cols here will be ['col1'].
    parent_cols: List[Expression]
    # Use this to skip quoting result name when aliasing.
    quote_result_name: bool = True


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


def _get_vector_sql_func(agg_func: AggFunc, is_tile: bool) -> str:
    """
    Helper function to get the SQL function name for an aggregate function.

    Parameters
    ----------
    agg_func : AggFunc
        Aggregate function
    is_tile : bool
        Whether the aggregate function is for a tile column

    Returns
    -------
    str
        This is the name of the aggregate function in SQL.
    """
    if is_tile and agg_func == AggFunc.AVG:
        return "VECTOR_AGGREGATE_AVG"
    array_parent_agg_func_sql_mapping = {
        AggFunc.MAX: "VECTOR_AGGREGATE_MAX",
        AggFunc.AVG: "VECTOR_AGGREGATE_SIMPLE_AVERAGE",
        AggFunc.SUM: "VECTOR_AGGREGATE_SUM",
    }
    assert agg_func in array_parent_agg_func_sql_mapping
    return array_parent_agg_func_sql_mapping[agg_func]


def column_distinct_count_including_null(
    column_expr: Expression, adapter: BaseAdapter
) -> Expression:
    """
    Get an expression for counting the number of distinct values in a column including null values

    Parameters
    ----------
    column_expr: Expression
        Column expression
    adapter: BaseAdapter
        Adapter

    Returns
    -------
    Expression
    """
    return expressions.Add(
        this=expressions.Count(this=expressions.Distinct(expressions=[column_expr])),
        expression=expressions.Cast(
            this=expressions.GT(
                this=adapter.count_if(
                    expressions.Is(
                        this=column_expr,
                        expression=expressions.Null(),
                    )
                ),
                expression=make_literal_value(0),
            ),
            to=expressions.DataType.build("BIGINT"),
        ),
    )


def get_aggregation_expression(
    agg_func: AggFunc,
    input_column: Optional[str | Expression],
    parent_dtype: Optional[DBVarType],
    adapter: BaseAdapter,
) -> Expression:
    """
    Convert an AggFunc and input column name to a SQL expression to be used in GROUP BY

    Parameters
    ----------
    agg_func : AggFunc
        Aggregation function
    input_column : str
        Input column name
    parent_dtype : Optional[DBVarType]
        Parent column data type
    adapter: BaseAdapter
        Adapter

    Returns
    -------
    Expression
    """
    # Check if it's a count function
    if agg_func == AggFunc.COUNT:
        return expressions.Count(this=expressions.Star())

    # Get input_column_expr from input_column
    assert input_column is not None
    if isinstance(input_column, str):
        input_column_expr = quoted_identifier(input_column)
    else:
        input_column_expr = input_column

    if agg_func == AggFunc.COUNT_DISTINCT:
        return column_distinct_count_including_null(input_column_expr, adapter)

    # Try to get a built-in SQL aggregate function
    agg_func_sql_mapping = {
        AggFunc.SUM: "SUM",
        AggFunc.AVG: "AVG",
        AggFunc.MIN: "MIN",
        AggFunc.MAX: "MAX",
        AggFunc.STD: "STDDEV",
        AggFunc.LATEST: "FIRST_VALUE",
    }
    if agg_func in agg_func_sql_mapping:
        sql_func = agg_func_sql_mapping[agg_func]
        if parent_dtype is not None and parent_dtype in DBVarType.array_types():
            sql_func = _get_vector_sql_func(agg_func, False)
            is_vector_aggregate = True
        else:
            is_vector_aggregate = False
        if is_vector_aggregate:
            return adapter.call_vector_aggregation_function(sql_func, [input_column_expr])
        return expressions.Anonymous(this=sql_func, expressions=[input_column_expr])

    # Must be NA_COUNT
    assert agg_func == AggFunc.NA_COUNT
    expr_is_null = expressions.Is(this=input_column_expr, expression=expressions.Null())
    return expressions.Sum(this=expressions.Cast(this=expr_is_null, to=parse_one("INTEGER")))


def get_vector_agg_column_snowflake(
    input_expr: Select,
    agg_func: AggFunc,
    groupby_keys: List[GroupbyKey],
    groupby_column: GroupbyColumn,
    index: int,
    is_tile: bool,
) -> VectorAggColumn:
    """
    Returns the vector aggregate expression for snowflake. This will call the vector aggregate function, and return its
    value as part of a table.

    Parameters
    ----------
    input_expr : Select
        Input expression
    agg_func : AggFunc
        Aggregation function
    groupby_keys : List[GroupbyKey]
        List of groupby keys
    groupby_column : GroupbyColumn
        Groupby column
    index : int
        Index of the vector aggregate column
    is_tile : bool
        Whether the aggregate function is for a tile column

    Returns
    -------
    VectorAggColumn
    """

    initial_data_table_name = "INITIAL_DATA"
    select_keys = [
        alias_(
            get_qualified_column_identifier(k.name, initial_data_table_name),
            alias=k.name,
            quoted=True,
        )
        for k in groupby_keys
    ]
    partition_by_keys = [
        get_qualified_column_identifier(k.name, initial_data_table_name) for k in groupby_keys
    ]

    agg_name = f"AGG_{index}"
    # The VECTOR_AGG_RESULT column value here, is a constant and is the name of the return value defined in the
    # UDTFs.
    agg_result_column = alias_(
        get_qualified_column_identifier("VECTOR_AGG_RESULT", agg_name),
        alias=groupby_column.result_name,
        quoted=True,
    )
    parent_cols = groupby_column.parent_cols
    agg_func_expr = expressions.Anonymous(
        this=_get_vector_sql_func(agg_func, is_tile),
        expressions=[parent_col.name for parent_col in parent_cols],
    )
    window_expr = expressions.Window(this=agg_func_expr, partition_by=partition_by_keys)
    table_expr = expressions.Table(
        this=expressions.Anonymous(this="TABLE", expressions=[window_expr]),
        alias=expressions.TableAlias(this=quoted_identifier(agg_name)),
    )

    updated_groupby_keys = []
    for key in groupby_keys:
        updated_groupby_keys.append(alias_(key.expr, alias=key.name, quoted=True))
    for parent_col in parent_cols:
        updated_groupby_keys.append(alias_(parent_col, alias=parent_col.name))

    updated_input_expr_with_select_keys = input_expr.select(*updated_groupby_keys)
    expr = (
        select(
            *select_keys,
            agg_result_column,
        )
        .from_(
            updated_input_expr_with_select_keys.subquery(alias=initial_data_table_name),
        )
        .join(table_expr)
    )
    return VectorAggColumn(
        aggr_expr=expr,
        result_name=groupby_column.result_name,
    )


def _split_agg_and_snowflake_vector_aggregation_columns(
    input_expr: Select,
    groupby_keys: list[GroupbyKey],
    groupby_columns: list[GroupbyColumn],
    value_by: Optional[GroupbyKey],
    source_type: SourceType,
    is_tile: bool,
) -> tuple[list[Expression], list[VectorAggColumn]]:
    """
    Split the list of groupby columns into normal aggregations, and snowflake vector aggregation columns.

    Note that the return value will only contain VectorAggColumns if the source type is for snowflake, as special
    handling is required for vector aggregations there. For other data warehouses, we will return vector aggregations
    as part fo the normal aggregation expressions.

    Parameters
    ----------
    input_expr: Select
        Input expression
    groupby_keys: list[GroupbyKey]
        List of groupby keys
    groupby_columns: list[GroupbyColumn]
        List of groupby columns
    value_by: Optional[GroupbyKey]
        Value by key
    source_type: SourceType
        Source type
    is_tile: bool
        Whether the query is for a tile

    Returns
    -------
    tuple[list[Expression], list[VectorAggColumn]]
        The first list contains the normal aggregation expressions, and the second list contains the vector aggregation.
    """
    non_vector_agg_exprs = []
    vector_agg_cols = []
    for index, column in enumerate(groupby_columns):
        if (
            should_use_element_wise_vector_aggregation(column.agg_func, column.parent_dtype)
            and source_type == SourceType.SNOWFLAKE
        ):
            vector_agg_cols.append(
                get_vector_agg_column_snowflake(
                    input_expr=input_expr,
                    agg_func=column.agg_func,
                    groupby_keys=groupby_keys,
                    groupby_column=column,
                    index=index,
                    is_tile=is_tile,
                )
            )
        else:
            assert column.parent_expr is not None
            non_vector_agg_exprs.append(
                alias_(
                    column.parent_expr,
                    alias=column.result_name + ("_inner" if value_by is not None else ""),
                    quoted=column.quote_result_name,
                )
            )
    return non_vector_agg_exprs, vector_agg_cols


def make_window_expr(
    agg_func_expr: Expression,
    key_exprs: list[Expression],
    window_order_by: Expression,
) -> Expression:
    """
    Make a window expression for the given aggregation function expression.

    Parameters
    ----------
    agg_func_expr : Expression
        Aggregation function expression
    key_exprs : list[Expression]
        List of key expressions
    window_order_by: Expression
        Window order by expression

    Returns
    -------
    Expression
    """
    order = expressions.Order(expressions=[expressions.Ordered(this=window_order_by, desc=True)])
    window_expr = expressions.Window(this=agg_func_expr, partition_by=key_exprs, order=order)
    return window_expr


def update_aggregation_expression_for_columns(
    groupby_columns: list[GroupbyColumn],
    keys: list[GroupbyKey],
    window_order_by: Optional[Expression],
    adapter: BaseAdapter,
) -> list[GroupbyColumn]:
    """
    Helper function to update the aggregation expression for the groupby columns. This will update the parent_expr
    in the GroupbyColumn object to be the aggregation expression (eg. `sum(col1)`.

    Parameters
    ----------
    groupby_columns: list[GroupbyColumn]
        List of groupby columns
    keys: list[GroupbyKey]
        List of groupby keys
    window_order_by: Optional[Expression]
        Window order by expression for order dependent aggregation
    adapter: BaseAdapter
        Adapter

    Returns
    -------
    list[GroupbyColumn]
    """
    output: list[GroupbyColumn] = []
    for column in groupby_columns:
        if not (
            column.parent_dtype in DBVarType.array_types()
            and adapter.source_type == SourceType.SNOWFLAKE
        ):
            aggregation_expression = get_aggregation_expression(
                agg_func=column.agg_func,
                input_column=column.parent_cols[0] if column.parent_cols else None,
                parent_dtype=column.parent_dtype,
                adapter=adapter,
            )
            if window_order_by is not None:
                aggregation_expression = make_window_expr(
                    agg_func_expr=aggregation_expression,
                    key_exprs=[key.expr for key in keys],
                    window_order_by=window_order_by,
                )
            column.parent_expr = aggregation_expression
        output.append(column)
    return output


def get_groupby_expr(
    input_expr: Select,
    groupby_keys: list[GroupbyKey],
    groupby_columns: list[GroupbyColumn],
    value_by: Optional[GroupbyKey],
    adapter: BaseAdapter,
    window_order_by: Optional[Expression] = None,
) -> Select:
    """
    Construct a GROUP BY statement using the provided expression as input. If window_order_by is
    provided, the aggregation will be done in a windowed manner (window function partitioned by the
    groupby keys and ordered by window_order_by).

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
    window_order_by: Optional[Expression]
        Window order by expression for order dependent aggregation

    Returns
    -------
    Select
    """
    updated_groupby_columns = update_aggregation_expression_for_columns(
        groupby_columns,
        groupby_keys + ([] if value_by is None else [value_by]),
        window_order_by,
        adapter,
    )
    agg_exprs, snowflake_vector_agg_cols = _split_agg_and_snowflake_vector_aggregation_columns(
        input_expr,
        groupby_keys,
        updated_groupby_columns,
        value_by,
        adapter.source_type,
        is_tile=False,
    )

    select_keys = [k.get_alias() for k in groupby_keys]
    keys = [k.expr for k in groupby_keys]
    if value_by:
        select_keys.append(value_by.get_alias())
        keys.append(value_by.expr)

    if window_order_by is None:
        aggregated_expr = adapter.group_by(
            input_expr,
            select_keys,
            agg_exprs,
            keys,
            snowflake_vector_agg_cols,
        )
    else:
        select_with_row_number = input_expr.select(
            alias_(
                make_window_expr(
                    expressions.Anonymous(this="ROW_NUMBER"),
                    key_exprs=keys,
                    window_order_by=window_order_by,
                ),
                alias=ROW_NUMBER,
                quoted=True,
            ),
            *select_keys,
            *agg_exprs,
        )
        aggregated_expr = (
            select(
                *[quoted_identifier(k.name) for k in groupby_keys]
                + ([quoted_identifier(value_by.name)] if value_by is not None else [])
                + [quoted_identifier(col.result_name) for col in groupby_columns],
            )
            .from_(select_with_row_number.subquery())
            .where(
                expressions.EQ(this=quoted_identifier(ROW_NUMBER), expression=make_literal_value(1))
            )
        )

    if value_by is not None:
        aggregated_expr = adapter.construct_key_value_aggregation_sql(
            point_in_time_column=None,
            serving_names=[k.name for k in groupby_keys],
            value_by=value_by.name,
            agg_result_names=[col.result_name for col in updated_groupby_columns],
            inner_agg_result_names=[col.result_name + "_inner" for col in updated_groupby_columns],
            inner_agg_expr=aggregated_expr,
        )

    return aggregated_expr
