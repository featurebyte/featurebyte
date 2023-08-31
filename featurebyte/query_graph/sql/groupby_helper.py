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


@dataclass
class GroupbyColumn:
    """
    Represents a set of parameters that produces one output column in a groupby statement
    """

    agg_func: AggFunc
    parent_expr: Optional[Expression]
    parent_dtype: Optional[DBVarType]
    result_name: str
    # This represents the columns that we're performing the aggregate function over.
    # For example, if we are doing a SUM(col), parent_cols will contain `["col"]`.
    # Note that this is a list because we can have multiple columns in the case of custom aggregations (eg. vector
    # avg aggregation which takes in two inputs - sum and count).
    parent_cols: list[str]


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
    agg_func: AggFunc,
    input_column: Optional[str | Expression],
    parent_dtype: Optional[DBVarType],
    is_tile: bool,
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
    is_tile : bool
        Whether the aggregation is for a tile

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
    array_parent_agg_func_sql_mapping = {
        AggFunc.MAX: "VECTOR_AGGREGATE_MAX",
        AggFunc.AVG: "VECTOR_AGGREGATE_SIMPLE_AVERAGE",
        AggFunc.SUM: "VECTOR_AGGREGATE_SUM",
    }
    expr: Expression
    if agg_func in agg_func_sql_mapping:
        assert input_column_expr is not None
        sql_func = agg_func_sql_mapping[agg_func]
        if parent_dtype is not None and parent_dtype == DBVarType.ARRAY:
            assert agg_func in array_parent_agg_func_sql_mapping
            sql_func = array_parent_agg_func_sql_mapping[agg_func]
            if is_tile and agg_func == AggFunc.AVG:
                sql_func = "VECTOR_AGGREGATE_AVG"
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


def get_vector_agg_column_snowflake(
    input_expr: Select,
    agg_func: AggFunc,
    groupby_keys: List[GroupbyKey],
    groupby_column: GroupbyColumn,
    index: int,
    is_tile: bool = False,
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
        Whether the input expression is a tile

    Returns
    -------
    VectorAggColumn
    """
    # pylint: disable=too-many-locals
    array_parent_agg_func_sql_mapping = {
        AggFunc.MAX: "VECTOR_AGGREGATE_MAX",
        AggFunc.AVG: "VECTOR_AGGREGATE_SIMPLE_AVERAGE",
        AggFunc.SUM: "VECTOR_AGGREGATE_SUM",
    }
    assert agg_func in array_parent_agg_func_sql_mapping
    snowflake_agg_func = array_parent_agg_func_sql_mapping[agg_func]
    if is_tile and agg_func == AggFunc.AVG:
        snowflake_agg_func = "VECTOR_AGGREGATE_AVG"

    initial_data_table_name = "INITIAL_DATA"
    select_keys = [
        alias_(
            expressions.Identifier(this=f"{initial_data_table_name}.{k.expr}"),
            alias=k.name,
            quoted=True,
        )
        for k in groupby_keys
    ]
    partition_by_keys = [
        expressions.Identifier(this=f"{initial_data_table_name}.{k.expr}") for k in groupby_keys
    ]

    agg_name = f"AGG_{index}"
    # The VECTOR_AGG_RESULT column value here, is a constant and is the name of the return value defined in the
    # UDTFs.
    agg_result_column = alias_(
        get_qualified_column_identifier("VECTOR_AGG_RESULT", agg_name),
        alias=groupby_column.result_name,
        quoted=True,
    )
    agg_func_expr = expressions.Anonymous(
        this=snowflake_agg_func, expressions=groupby_column.parent_cols
    )
    window_expr = expressions.Window(this=agg_func_expr, partition_by=partition_by_keys)
    table_expr = expressions.Anonymous(this="TABLE", expressions=[window_expr])
    aliased_table_expr = alias_(table_expr, alias=agg_name, quoted=True)

    updated_groupby_keys = []
    for key in groupby_keys:
        updated_groupby_keys.append(alias_(key.expr, alias=key.name, quoted=True))
    for col in groupby_column.parent_cols:
        updated_groupby_keys.append(alias_(col, alias=col))

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
    is_tile: bool = False,
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

    Returns
    -------
    tuple[list[Expression], list[VectorAggColumn]]
        The first list contains the normal aggregation expressions, and the second list contains the vector aggregation.
    """
    non_vector_agg_exprs = []
    vector_agg_cols = []
    for index, column in enumerate(groupby_columns):
        if column.parent_dtype == DBVarType.ARRAY and source_type == SourceType.SNOWFLAKE:
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
                    get_aggregation_expression(
                        agg_func=column.agg_func,
                        input_column=column.parent_cols[0],
                        parent_dtype=column.parent_dtype,
                        is_tile=is_tile,
                    ),
                    alias=column.result_name + ("_inner" if value_by is not None else ""),
                    quoted=True,
                )
            )
    return non_vector_agg_exprs, vector_agg_cols


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
    agg_exprs, snowflake_vector_agg_cols = _split_agg_and_snowflake_vector_aggregation_columns(
        input_expr, groupby_keys, groupby_columns, value_by, adapter.source_type
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
            agg_result_names=[col.result_name for col in groupby_columns],
            inner_agg_result_names=[col.result_name + "_inner" for col in groupby_columns],
            inner_agg_expr=groupby_expr,
        )

    return groupby_expr
