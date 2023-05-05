"""
Common utilities for Spark SQL
"""
from __future__ import annotations

from typing import Optional

import asyncio
from random import randint

import pandas as pd

from featurebyte.logging import get_logger
from featurebyte.session.base import BaseSession
from featurebyte.session.snowflake import SnowflakeSession

TABLE_PROPERTIES = "TBLPROPERTIES('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')"


logger = get_logger(__name__)


def construct_create_table_query(
    table_name: str,
    table_query: str,
    partition_keys: Optional[str] = None,
    session: Optional[BaseSession] = None,
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
    session: BaseSession
        Spark or Snowflake session

    Returns
    -------
    str
        Query to create a delta table in Spark
    """
    partition_clause = ""
    if partition_keys:
        partition_clause = f"PARTITIONED BY ({partition_keys})"

    sql = f"""
            CREATE TABLE {table_name} USING DELTA
                {partition_clause}
                {TABLE_PROPERTIES}
            AS
                {table_query}
            """
    if session is not None and isinstance(session, SnowflakeSession):
        sql = f"CREATE TABLE {table_name} AS (SELECT * FROM ({table_query}))"

    return sql


async def retry_sql(
    session: BaseSession,
    sql: str,
    retry_num: int = 10,
    sleep_interval: int = 5,
) -> pd.DataFrame | None:
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

    Returns
    -------
    pd.DataFrame
        Result of the sql operation

    Raises
    ------
    Exception
        if the sql operation fails after retry_num retries
    """

    for i in range(retry_num):
        try:
            return await session.execute_query_long_running(sql)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning(
                "SQL query failed",
                extra={"attempt": i, "query": sql.strip()[:50].replace("\n", " ")},
            )
            if i == retry_num - 1:
                logger.error("SQL query failed", extra={"attempts": retry_num, "exception": exc})
                raise

        random_interval = randint(1, sleep_interval)
        await asyncio.sleep(random_interval)

    return None
