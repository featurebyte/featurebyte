"""
Common utilities for Spark SQL
"""
from __future__ import annotations

from typing import Optional

import time

from featurebyte.logger import logger
from featurebyte.session.base import BaseSession

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


async def retry_sql(
    session: BaseSession, sql: str, retry_num: int = 3, sleep_interval: int = 1
) -> None:
    """
    Retry sql operation

    Parameters
    ----------
    session: BaseSession
        Spark session
    sql: str
        SQL query
    retry_num: int
        Number of retries
    sleep_interval: int
        Sleep interval between retries

    Raises
    ------
    Exception
        if the sql operation fails after retry_num retries
    """

    for i in range(retry_num):
        try:
            await session.execute_query(sql)
            break
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.error(f"Problem with sql run {i}: {err}")
            if i == retry_num - 1:
                raise err

        time.sleep(sleep_interval)
