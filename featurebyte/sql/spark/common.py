"""
Common utilities for Spark SQL
"""
from __future__ import annotations

from typing import Optional

TABLE_PROPERTIES = "TBLPROPERTIES('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')"


def construct_create_delta_table_query(
    table_name: str, table_query: str, partition_keys: Optional[str] = None
) -> str:
    """
    Construct a query to create a delta table in Spark based table_query

    Parameters
    ----------
    table_name: str
        Table name
    table_query: str
        SQL query for the contents of the table
    partition_keys: str
        Partition keys

    Returns
    -------
    str
        Query to create a delta table in Spark
    """
    partition_clause = ""
    if partition_keys:
        partition_clause = f"PARTITIONED BY ({partition_keys})"

    return f"""
            CREATE TABLE {table_name} USING DELTA
                {partition_clause}
                {TABLE_PROPERTIES}
            AS
                {table_query}
            """
