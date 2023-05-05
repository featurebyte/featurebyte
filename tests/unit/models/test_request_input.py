"""
Unit tests related to RequestInput
"""
import textwrap
from unittest.mock import AsyncMock, Mock, call

import pytest

from featurebyte import SourceType
from featurebyte.enum import DBVarType
from featurebyte.exception import ColumnNotFoundError
from featurebyte.models.request_input import SourceTableRequestInput, ViewRequestInput
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
        list_table_schema=AsyncMock(return_value={"a": DBVarType.INT, "b": DBVarType.INT}),
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

    assert session.list_table_schema.call_args_list == [
        call(table_name="sf_table", database_name="sf_database", schema_name="sf_schema")
    ]

    expected_query = textwrap.dedent(
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
    assert session.execute_query.call_args_list == [call(expected_query)]


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

    assert session.list_table_schema.call_args_list == [
        call(table_name="sf_table", database_name="sf_database", schema_name="sf_schema")
    ]

    expected_query = textwrap.dedent(
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
    assert session.execute_query.call_args_list == [call(expected_query)]


@pytest.mark.asyncio
async def test_materialize__with_renames_only(session, snowflake_database_table, destination_table):
    """
    Test materializing when only the columns rename mapping is specified
    """
    request_input = SourceTableRequestInput(
        columns_rename_mapping={"b": "NEW_B"},
        source=snowflake_database_table.tabular_source,
    )
    await request_input.materialize(session, destination_table, None)

    assert session.list_table_schema.call_args_list == [
        call(table_name="sf_table", database_name="sf_database", schema_name="sf_schema")
    ]

    expected_query = textwrap.dedent(
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
    assert session.execute_query.call_args_list == [call(expected_query)]


@pytest.mark.asyncio
async def test_materialize__invalid_columns(session, snowflake_database_table, destination_table):
    """
    Test invalid columns filter
    """
    request_input = SourceTableRequestInput(
        columns=["unknown_column"],
        source=snowflake_database_table.tabular_source,
    )
    with pytest.raises(ColumnNotFoundError) as exc:
        await request_input.materialize(session, destination_table, None)
    assert "Columns ['unknown_column'] not found" in str(exc.value)


@pytest.mark.asyncio
async def test_materialize__invalid_rename_mapping(
    session, snowflake_database_table, destination_table
):
    """
    Test invalid columns rename mapping
    """
    request_input = SourceTableRequestInput(
        columns_rename_mapping={"unknown_column": "NEW_COL"},
        source=snowflake_database_table.tabular_source,
    )
    with pytest.raises(ColumnNotFoundError) as exc:
        await request_input.materialize(session, destination_table, None)
    assert "Columns ['unknown_column'] not found" in str(exc.value)


@pytest.mark.asyncio
async def test_materialize__from_view_with_columns_and_renames(
    session, destination_table, snowflake_event_table
):
    """
    Test materializing from view
    """
    view = snowflake_event_table.get_view()
    pruned_graph, mapped_node = view.extract_pruned_graph_and_node()
    request_input = ViewRequestInput(
        columns=["event_timestamp", "col_int"],
        columns_rename_mapping={"event_timestamp": "POINT_IN_TIME"},
        graph=pruned_graph,
        node_name=mapped_node.name,
    )
    await request_input.materialize(session, destination_table, None)

    # No need to query database to get column names
    assert session.list_table_schema.call_args_list == []

    expected_query = textwrap.dedent(
        """
        CREATE TABLE "sf_database"."sf_schema"."my_materialized_table" AS
        SELECT
          "event_timestamp" AS "POINT_IN_TIME",
          "col_int" AS "col_int"
        FROM (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "col_char" AS "col_char",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "event_timestamp" AS "event_timestamp",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."sf_table"
        )
        """
    ).strip()
    assert session.execute_query.call_args_list == [call(expected_query)]
