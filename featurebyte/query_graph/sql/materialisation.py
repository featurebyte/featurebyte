"""
SQL generation related to materialising tables such as ObservationTable
"""

from __future__ import annotations

from typing import List, Optional

from sqlglot import expressions
from sqlglot.expressions import Expression, Select, alias_, select

from featurebyte.enum import InternalName, SourceType
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
    column_names: Optional[List[str]] = None,
) -> Select:
    """
    Construct SQL query to materialize a table from a source table

    Parameters
    ----------
    source: TableDetails
        Source table details
    column_names: Optional[List[str]]
        List of column names to select if specified

    Returns
    -------
    Select
    """
    select_expr = expressions.select().from_(get_fully_qualified_table_name(source.dict()))
    if column_names:
        select_expr = select_expr.select(*[quoted_identifier(col) for col in column_names])
    else:
        select_expr = select_expr.select("*")
    return select_expr


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


def get_row_index_column_expr() -> Expression:
    """
    Returns an expression for a running number aliased as TABLE_ROW_INDEX

    Returns
    -------
    Expression
    """
    return expressions.alias_(  # type: ignore[no-any-return]
        expressions.Window(
            this=expressions.Anonymous(this="ROW_NUMBER"),
            order=expressions.Order(expressions=[expressions.Literal.number(1)]),
        ),
        alias=InternalName.TABLE_ROW_INDEX,
        quoted=True,
    )


def get_feature_store_id_expr(
    database_name: Optional[str],
    schema_name: Optional[str],
) -> Select:
    """
    Construct SQL query to get the feature store id of a featurebyte schema

    Parameters
    ----------
    database_name: Optional[str]
        Database name
    schema_name: Optional[str]
        Schema name

    Returns
    -------
    Select
    """
    return expressions.select(quoted_identifier("FEATURE_STORE_ID")).from_(
        expressions.Table(
            this=quoted_identifier("METADATA_SCHEMA"),
            db=quoted_identifier(schema_name) if schema_name else None,
            catalog=quoted_identifier(database_name) if database_name else None,
        )
    )
