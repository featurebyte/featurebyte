"""
SQL generation related to materialising tables such as ObservationTable
"""
from __future__ import annotations

from sqlglot import expressions
from sqlglot.expressions import Expression, Select

from featurebyte import SourceType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import get_fully_qualified_table_name, sql_to_string
from featurebyte.query_graph.sql.interpreter import GraphInterpreter


def create_table_as(table_details: TableDetails, select_expr: Select) -> Expression:
    """
    Construct query to create a table using a select statement

    Parameters
    ----------
    table_details: TableDetails
        TableDetails of the table to be created
    select_expr: Select
        Select expression

    Returns
    -------
    Expression
    """
    destination_expr = get_fully_qualified_table_name(table_details.dict())
    return expressions.Create(
        this=expressions.Table(this=destination_expr),
        kind="TABLE",
        replace=True,
        expression=select_expr,
    )


def get_materialise_from_source_sql(
    source: TableDetails,
    destination: TableDetails,
    source_type: SourceType,
) -> str:
    """
    Construct SQL query to materialise a table from a source table

    Parameters
    ----------
    source: TableDetails
        Source table details
    destination: TableDetails
        Destination table details
    source_type: SourceType
        Source type information

    Returns
    -------
    str
    """
    source_expr = expressions.select("*").from_(get_fully_qualified_table_name(source.dict()))
    copy_table_expr = create_table_as(destination, source_expr)
    return sql_to_string(copy_table_expr, source_type=source_type)


def get_materialise_from_view_sql(
    graph: QueryGraphModel,
    node_name: str,
    destination: TableDetails,
    source_type: SourceType,
) -> str:
    """
    Construct SQL query to materialise a view given its query graph

    Parameters
    ----------
    graph: QueryGraphModel
        Query graph
    node_name: str
        Name of the node to materialise
    destination: TableDetails
        Destination table details
    source_type: SourceType
        Source type information

    Returns
    -------
    str
    """
    interpreter = GraphInterpreter(query_graph=graph, source_type=source_type)
    table_expr = interpreter.construct_materialise_expr(node_name)
    query = create_table_as(destination, table_expr)
    return sql_to_string(query, source_type=source_type)
