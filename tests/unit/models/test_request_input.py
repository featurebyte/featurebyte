"""
Unit tests related to RequestInput
"""
import textwrap
from unittest.mock import Mock, call

import pandas as pd
import pytest

from featurebyte import SourceType
from featurebyte.models.request_input import SourceTableRequestInput
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.session.snowflake import SnowflakeSession


@pytest.fixture(name="session")
def session_fixture():
    """
    Fixture for the db session object
    """
    return Mock(
        name="mock_snowflake_session",
        spec=SnowflakeSession,
        source_type=SourceType.SNOWFLAKE,
    )


@pytest.fixture(name="destination_table")
def destination_table_fixture():
    """
    Fixture for a TableDetails for the materialized table location
    """
    return TableDetails(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="my_materialized_table",
    )


@pytest.mark.asyncio
async def test_materialize__with_columns_only(session, snowflake_database_table, destination_table):
    """
    Test materializing when columns filter is specified
    """
    request_input = SourceTableRequestInput(
        columns=["a", "b"],
        source=snowflake_database_table.tabular_source,
    )
    await request_input.materialize(session, destination_table, None)
    expected = textwrap.dedent(
        """
        CREATE TABLE "sf_database"."sf_schema"."my_materialized_table" AS
        SELECT
          "a",
          "b"
        FROM (
          SELECT
            *
          FROM "sf_database"."sf_schema"."sf_table"
        )
        """
    ).strip()
    assert session.execute_query.call_args_list == [call(expected)]


@pytest.mark.asyncio
async def test_materialize__with_columns_and_renames(
    session, snowflake_database_table, destination_table
):
    """
    Test materializing when columns filter and rename mapping are specified
    """
    request_input = SourceTableRequestInput(
        columns=["a", "b"],
        columns_rename_mapping={"b": "NEW_B"},
        source=snowflake_database_table.tabular_source,
    )
    await request_input.materialize(session, destination_table, None)
    expected = textwrap.dedent(
        """
        CREATE TABLE "sf_database"."sf_schema"."my_materialized_table" AS
        SELECT
          "a" AS "a",
          "b" AS "NEW_B"
        FROM (
          SELECT
            *
          FROM "sf_database"."sf_schema"."sf_table"
        )
        """
    ).strip()
    assert session.execute_query.call_args_list == [call(expected)]


@pytest.mark.asyncio
async def test_materialize__with_renames_only(session, snowflake_database_table, destination_table):
    """
    Test materializing when only the columns rename mapping is specified
    """
    request_input = SourceTableRequestInput(
        columns_rename_mapping={"b": "NEW_B"},
        source=snowflake_database_table.tabular_source,
    )
    session.execute_query.return_value = pd.DataFrame({"a": [1], "b": [2]})
    await request_input.materialize(session, destination_table, None)

    # First query retrieves the schema of the table / view
    expected_query_1 = textwrap.dedent(
        """
        SELECT
          *
        FROM "sf_database"."sf_schema"."sf_table"
        LIMIT 1
        """
    ).strip()

    # Second query materializes the table
    expected_query_2 = textwrap.dedent(
        """
        CREATE TABLE "sf_database"."sf_schema"."my_materialized_table" AS
        SELECT
          "a" AS "a",
          "b" AS "NEW_B"
        FROM (
          SELECT
            *
          FROM "sf_database"."sf_schema"."sf_table"
        )
        """
    ).strip()
    assert session.execute_query.call_args_list == [call(expected_query_1), call(expected_query_2)]
