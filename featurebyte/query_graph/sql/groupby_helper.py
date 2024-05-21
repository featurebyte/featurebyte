"""
Utilities related to SQL generation for groupby operations
"""

from __future__ import annotations

from typing import List, Optional, cast

from dataclasses import dataclass

from sqlglot import expressions, parse_one
from sqlglot.expressions import Expression, Select, alias_, select

from featurebyte.enum import AggFunc, DBVarType, SourceType
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.adapter.base import VectorAggColumn
from featurebyte.query_graph.sql.common import get_qualified_column_identifier, quoted_identifier
from featurebyte.query_graph.sql.vector_helper import should_use_element_wise_vector_aggregation


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


def get_aggregation_expression(
    agg_func: AggFunc, input_column: Optional[str | Expression], parent_dtype: Optional[DBVarType]
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

    Returns
    -------
    Expression
    """
    # Check if it's a count function
    if agg_func == AggFunc.COUNT:
        return expressions.Count(this="*")

    # Get input_column_expr from input_column
    assert input_column is not None
    input_column_expr = input_column
    if isinstance(input_column, str):
        input_column_expr = quoted_identifier(input_column)

    # Try to get a built-in SQL aggregate function
    agg_func_sql_mapping = {
        AggFunc.SUM: "SUM",
        AggFunc.AVG: "AVG",
        AggFunc.MIN: "MIN",
        AggFunc.MAX: "MAX",
        AggFunc.STD: "STDDEV",
    }
    if agg_func in agg_func_sql_mapping:
        sql_func = agg_func_sql_mapping[agg_func]
        if parent_dtype is not None and parent_dtype in DBVarType.array_types():
            sql_func = _get_vector_sql_func(agg_func, False)
        return expressions.Anonymous(this=sql_func, expressions=[input_column_expr])

    # Must be NA_COUNT
    assert agg_func == AggFunc.NA_COUNT
    expr_is_null = expressions.Is(this=input_column_expr, expression=expressions.NULL)
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
    # pylint: disable=too-many-locals
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
    table_expr = expressions.Anonymous(this="TABLE", expressions=[window_expr])
    aliased_table_expr = alias_(table_expr, alias=agg_name, quoted=True)

    updated_groupby_keys = []
    for key in groupby_keys:
        updated_groupby_keys.append(alias_(key.expr, alias=key.name, quoted=True))
    for parent_col in parent_cols:
        updated_groupby_keys.append(alias_(parent_col, alias=parent_col.name))

    updated_input_expr_with_select_keys = input_expr.select(*updated_groupby_keys)
    expr = select(
        *select_keys,
        agg_result_column,
    ).from_(
        updated_input_expr_with_select_keys.subquery(alias=initial_data_table_name),
        aliased_table_expr,
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
            non_vector_agg_exprs.append(
                alias_(
                    column.parent_expr,
                    alias=column.result_name + ("_inner" if value_by is not None else ""),
                    quoted=column.quote_result_name,
                )
            )
    return non_vector_agg_exprs, vector_agg_cols


def update_aggregation_expression_for_columns(
    groupby_columns: list[GroupbyColumn], adapter_type: SourceType
) -> list[GroupbyColumn]:
    """
    Helper function to update the aggregation expression for the groupby columns. This will update the parent_expr
    in the GroupbyColumn object to be the aggregation expression (eg. `sum(col1)`.

    Parameters
    ----------
    groupby_columns: list[GroupbyColumn]
        List of groupby columns
    adapter_type: SourceType
        Adapter type

    Returns
    -------
    list[GroupbyColumn]
    """
    output: list[GroupbyColumn] = []
    for column in groupby_columns:
        if not (
            column.parent_dtype in DBVarType.array_types() and adapter_type == SourceType.SNOWFLAKE
        ):
            aggregation_expression = get_aggregation_expression(
                agg_func=column.agg_func,
                input_column=column.parent_cols[0] if column.parent_cols else None,
                parent_dtype=column.parent_dtype,
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
) -> Select:
    """
    Construct a GROUP BY statement using the provided expression as input.

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
    updated_groupby_columns = update_aggregation_expression_for_columns(
        groupby_columns, adapter.source_type
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

    groupby_expr = adapter.group_by(
        input_expr,
        select_keys,
        agg_exprs,
        keys,
        snowflake_vector_agg_cols,
    )

    if value_by is not None:
        groupby_expr = adapter.construct_key_value_aggregation_sql(
            point_in_time_column=None,
            serving_names=[k.name for k in groupby_keys],
            value_by=value_by.name,
            agg_result_names=[col.result_name for col in updated_groupby_columns],
            inner_agg_result_names=[col.result_name + "_inner" for col in updated_groupby_columns],
            inner_agg_expr=groupby_expr,
        )

    return groupby_expr
