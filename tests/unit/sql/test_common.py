"""
Unit tests for featurebyte/sql/common.py
"""
from unittest.mock import call

import pytest

from featurebyte.sql.common import register_temporary_physical_table


@pytest.mark.asyncio
async def test_register_temporary_physical_table(mock_snowflake_session):
    """
    Test register_temporary_physical_table
    """
    query = "select a, b, c from my_table"
    try:
        async with register_temporary_physical_table(mock_snowflake_session, query) as temp_table:
            assert mock_snowflake_session.execute_query_long_running.call_args == call(
                f"CREATE TABLE {temp_table} AS (SELECT * FROM (select a, b, c from my_table))"
            )
            raise RuntimeError("Fail on purpose")
    except RuntimeError:
        pass
    assert mock_snowflake_session.drop_table.call_args == call(
        temp_table,
        schema_name="sf_schema",
        database_name="sf_db",
        if_exists=True,
    )
