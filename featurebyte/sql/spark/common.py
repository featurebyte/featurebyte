"""
Common utilities for Spark SQL
"""
from __future__ import annotations

TABLE_PROPERTIES = "TBLPROPERTIES('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')"


def construct_create_delta_table_query(table_name: str, table_query: str) -> str:
    """
    Construct a query to create a delta table in Spark based table_query

    Parameters
    ----------
    table_name: str
        Table name
    table_query: str
        SQL query for the contents of the table

    Returns
    -------
    str
        Query to create a delta table in Spark
    """
    return f"create table {table_name} using delta {TABLE_PROPERTIES} as {table_query}"
