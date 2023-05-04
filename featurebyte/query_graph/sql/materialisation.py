"""
SQL generation related to materialising tables such as ObservationTable
"""
from __future__ import annotations

from typing import Optional

from sqlglot import expressions
from sqlglot.expressions import Select, alias_, select

from featurebyte.enum import SourceType, SpecialColumnName
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import (
    get_fully_qualified_table_name,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.interpreter import GraphInterpreter


def get_source_expr(
    source: TableDetails,
) -> Select:
    """
    Construct SQL query to materialize a table from a source table

    Parameters
    ----------
    source: TableDetails
        Source table details

    Returns
    -------
    Select
    """
    return expressions.select("*").from_(get_fully_qualified_table_name(source.dict()))


def get_source_count_expr(
    source: TableDetails,
) -> Select:
    """
    Construct SQL query to get the row count of a source table

    Parameters
    ----------
    source: TableDetails
        Source table details

    Returns
    -------
    Select
    """
    return expressions.select(
        expressions.alias_(
            expressions.Count(this=expressions.Star()), alias="row_count", quoted=True
        )
    ).from_(get_fully_qualified_table_name(source.dict()))


def get_view_expr(
    graph: QueryGraphModel,
    node_name: str,
    source_type: SourceType,
) -> Select:
    """
    Construct SQL query to materialize a view given its query graph

    Parameters
    ----------
    graph: QueryGraphModel
        Query graph
    node_name: str
        Name of the node to materialize
    source_type: SourceType
        Source type information

    Returns
    -------
    Select
    """
    interpreter = GraphInterpreter(query_graph=graph, source_type=source_type)
    table_expr = interpreter.construct_materialize_expr(node_name)
    return table_expr


def get_most_recent_point_in_time_sql(
    destination: TableDetails,
    source_type: SourceType,
) -> str:
    """
    Construct SQL query to get the most recent point in time

    Parameters
    ----------
    destination: TableDetails
        Destination table details
    source_type: SourceType
        Source type information

    Returns
    -------
    str
    """
    query = expressions.select(
        expressions.Max(this=quoted_identifier(SpecialColumnName.POINT_IN_TIME))
    ).from_(get_fully_qualified_table_name(destination.dict()))
    return sql_to_string(query, source_type=source_type)


def get_row_count_sql(table_expr: Select, source_type: SourceType) -> str:
    """
    Construct SQL query to get the row count of a table

    Parameters
    ----------
    table_expr: Select
        Table expression
    source_type: SourceType
        Source type information

    Returns
    -------
    str
    """
    expr = expressions.select(
        expressions.alias_(
            expressions.Count(this=expressions.Star()), alias="row_count", quoted=True
        )
    ).from_(table_expr.subquery())
    return sql_to_string(expr, source_type=source_type)


def select_and_rename_columns(
    table_expr: Select,
    columns: list[str],
    columns_rename_mapping: Optional[dict[str, str]],
) -> Select:
    """
    Select columns from a table expression

    Parameters
    ----------
    table_expr: Select
        Table expression
    columns: list[str]
        List of column names
    columns_rename_mapping: dict[str, str]
        Mapping from column names to new column names

    Returns
    -------
    Select
    """
    if columns_rename_mapping:
        column_exprs = [
            alias_(quoted_identifier(col), columns_rename_mapping.get(col, col), quoted=True)
            for col in columns
        ]
    else:
        column_exprs = [quoted_identifier(col) for col in columns]
    return select(*column_exprs).from_(table_expr.subquery())
