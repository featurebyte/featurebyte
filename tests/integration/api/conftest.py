import pytest_asyncio
from sqlglot import parse_one

from featurebyte.query_graph.sql.common import sql_to_string


@pytest_asyncio.fixture(name="source_table_with_invalid_dates", scope="session")
async def source_table_with_invalid_dates_fixture(session, feature_store, catalog):
    """
    Fixture for a source table with invalid dates
    """
    _ = catalog
    query = sql_to_string(
        parse_one(
            """
            CREATE TABLE TABLE_INVALID_DATES AS
            SELECT 1 AS "id", CAST('2021-01-01 10:00:00' AS TIMESTAMP) AS "date_col"
            UNION ALL
            SELECT 2 AS "id", CAST('0019-01-01 10:00:00' AS TIMESTAMP) AS "date_col"
            UNION ALL
            SELECT 3 AS "id", CAST('0019-01-01 10:00:00' AS TIMESTAMP) AS "date_col"
            UNION ALL
            SELECT 4 AS "id", CAST('0019-01-01 10:00:00' AS TIMESTAMP) AS "date_col"
            UNION ALL
            SELECT 5 AS "id", CAST('9019-01-01 10:00:00' AS TIMESTAMP) AS "date_col"
            UNION ALL
            SELECT 6 AS "id", CAST('2023-01-01 10:00:00' AS TIMESTAMP) AS "date_col"
            """,
            read="snowflake",
        ),
        source_type=session.source_type,
    )
    await session.execute_query(query)
    ds = feature_store.get_data_source()
    return ds.get_source_table(
        table_name="TABLE_INVALID_DATES",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )


@pytest_asyncio.fixture(name="source_table_with_numeric_strings", scope="session")
async def source_table_with_numeric_strings_fixture(session, feature_store, catalog):
    """
    Fixture for a source table with VARCHAR column containing numeric values
    """
    _ = catalog
    query = sql_to_string(
        parse_one(
            """
            CREATE TABLE TEST_CAST_NUMERIC AS
            SELECT 1 AS "event_id", '100' AS "numeric_string", CAST('2001-01-01 12:00:00' AS TIMESTAMP) AS "event_timestamp", 1 AS "cust_id"
            UNION ALL
            SELECT 2 AS "event_id", '200' AS "numeric_string", CAST('2001-01-01 13:00:00' AS TIMESTAMP) AS "event_timestamp", 1 AS "cust_id"
            UNION ALL
            SELECT 3 AS "event_id", '300' AS "numeric_string", CAST('2001-01-01 14:00:00' AS TIMESTAMP) AS "event_timestamp", 2 AS "cust_id"
            UNION ALL
            SELECT 4 AS "event_id", 'invalid' AS "numeric_string", CAST('2001-01-01 15:00:00' AS TIMESTAMP) AS "event_timestamp", 2 AS "cust_id"
            """,
            read="snowflake",
        ),
        source_type=session.source_type,
    )
    await session.execute_query(query)
    ds = feature_store.get_data_source()
    return ds.get_source_table(
        table_name="TEST_CAST_NUMERIC",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
