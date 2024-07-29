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
