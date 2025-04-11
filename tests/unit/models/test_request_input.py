"""
Unit tests related to RequestInput
"""

import textwrap
from datetime import datetime
from functools import partial
from unittest.mock import AsyncMock, Mock, call

import pandas as pd
import pytest
from dateutil import tz

from featurebyte import SourceType
from featurebyte.enum import DBVarType
from featurebyte.exception import ColumnNotFoundError
from featurebyte.models.request_input import SourceTableRequestInput, ViewRequestInput
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.session.snowflake import SnowflakeSession
from tests.util.helper import assert_equal_with_expected_fixture, extract_session_executed_queries


@pytest.fixture(name="session")
def session_fixture(adapter):
    """
    Fixture for the db session object
    """
    session = Mock(
        name="mock_snowflake_session",
        spec=SnowflakeSession,
        source_type=SourceType.SNOWFLAKE,
        list_table_schema=AsyncMock(
            return_value={
                "a": ColumnSpecWithDescription(name="a", dtype=DBVarType.INT),
                "b": ColumnSpecWithDescription(name="a", dtype=DBVarType.INT),
                "event_timestamp": ColumnSpecWithDescription(
                    name="event_timestamp", dtype=DBVarType.TIMESTAMP
                ),
            }
        ),
        adapter=adapter,
    )
    session.create_table_as = partial(SnowflakeSession.create_table_as, session)
    session.execute_query_long_running.return_value = pd.DataFrame({"row_count": [1000]})
    session.get_source_info.return_value.source_type = SourceType.SNOWFLAKE
    return session


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
    assert session.execute_query_long_running.call_args_list == [call(expected_query)]


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
    await request_input.materialize(
        session, destination_table, None, columns_to_exclude_missing_values=["NEW_B"]
    )

    assert session.list_table_schema.call_args_list == [
        call(table_name="sf_table", database_name="sf_database", schema_name="sf_schema")
    ]

    expected_query = textwrap.dedent(
        """
        CREATE TABLE "sf_database"."sf_schema"."my_materialized_table" AS
        SELECT
          "a",
          "NEW_B"
        FROM (
          SELECT
            "a" AS "a",
            "b" AS "NEW_B"
          FROM (
            SELECT
              *
            FROM "sf_database"."sf_schema"."sf_table"
          )
        )
        WHERE
              "NEW_B" IS NOT NULL
        """
    ).strip()
    assert session.execute_query_long_running.call_args_list == [call(expected_query)]


@pytest.mark.asyncio
async def test_materialize__with_renames_only(session, snowflake_database_table, destination_table):
    """
    Test materializing when only the columns rename mapping is specified
    """
    request_input = SourceTableRequestInput(
        columns=["a", "b"],
        columns_rename_mapping={"b": "NEW_B"},
        source=snowflake_database_table.tabular_source,
    )
    await request_input.materialize(
        session, destination_table, None, columns_to_exclude_missing_values=["NEW_B"]
    )

    assert session.list_table_schema.call_args_list == [
        call(table_name="sf_table", database_name="sf_database", schema_name="sf_schema")
    ]

    expected_query = textwrap.dedent(
        """
        CREATE TABLE "sf_database"."sf_schema"."my_materialized_table" AS
        SELECT
          "a",
          "NEW_B"
        FROM (
          SELECT
            "a" AS "a",
            "b" AS "NEW_B"
          FROM (
            SELECT
              *
            FROM "sf_database"."sf_schema"."sf_table"
          )
        )
        WHERE
              "NEW_B" IS NOT NULL
        """
    ).strip()
    assert session.execute_query_long_running.call_args_list == [call(expected_query)]


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
    await request_input.materialize(
        session, destination_table, None, columns_to_exclude_missing_values=["POINT_IN_TIME"]
    )

    # No need to query database to get column names
    assert session.list_table_schema.call_args_list == []

    expected_query = textwrap.dedent(
        """
        CREATE TABLE "sf_database"."sf_schema"."my_materialized_table" AS
        SELECT
          "POINT_IN_TIME",
          "col_int"
        FROM (
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP) AS "POINT_IN_TIME",
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
        )
        WHERE
              "POINT_IN_TIME" IS NOT NULL
        """
    ).strip()
    assert session.execute_query_long_running.call_args_list == [call(expected_query)]


@pytest.mark.asyncio
async def test_materialize__view_bigquery(
    session, destination_table, snowflake_event_table, bigquery_source_info, update_fixtures
):
    """
    Test materializing from view in bigquery
    """
    session.get_source_info.return_value = bigquery_source_info
    session.source_type = SourceType.BIGQUERY
    view = snowflake_event_table.get_view()
    pruned_graph, mapped_node = view.extract_pruned_graph_and_node()
    request_input = ViewRequestInput(
        columns=["event_timestamp", "col_int"],
        columns_rename_mapping={"event_timestamp": "POINT_IN_TIME"},
        graph=pruned_graph,
        node_name=mapped_node.name,
    )
    await request_input.materialize(session, destination_table, 100)

    # No need to query database to get column names
    assert session.list_table_schema.call_args_list == []

    queries = extract_session_executed_queries(session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/request_input/materialize_bigquery.sql",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_materialize__with_sample_timestamp(
    session, snowflake_event_table, destination_table
):
    """
    Test materializing with sample from timestamp
    """
    view = snowflake_event_table.get_view()
    pruned_graph, mapped_node = view.extract_pruned_graph_and_node()
    request_input = ViewRequestInput(
        columns=["event_timestamp", "col_int"],
        columns_rename_mapping={"event_timestamp": "POINT_IN_TIME"},
        graph=pruned_graph,
        node_name=mapped_node.name,
    )
    await request_input.materialize(session, destination_table, None, None, datetime(2011, 3, 8))

    # No need to query database to get column names
    assert session.list_table_schema.call_args_list == []

    expected_query = textwrap.dedent(
        """
        CREATE TABLE "sf_database"."sf_schema"."my_materialized_table" AS
        SELECT
          "POINT_IN_TIME",
          "col_int"
        FROM (
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP) AS "POINT_IN_TIME",
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
        )
        WHERE
            "POINT_IN_TIME" < CAST('2011-03-08T00:00:00' AS TIMESTAMP)
        """
    ).strip()
    assert session.execute_query_long_running.call_args_list == [call(expected_query)]


@pytest.mark.asyncio
async def test_materialize__with_sample_timestamp_biqquery(
    session, snowflake_database_table, destination_table, bigquery_source_info
):
    """
    Test materializing with sample from timestamp
    """
    session.get_source_info.return_value = bigquery_source_info
    session.source_type = SourceType.BIGQUERY
    request_input = SourceTableRequestInput(
        columns=["a", "b", "event_timestamp"],
        columns_rename_mapping={"event_timestamp": "POINT_IN_TIME"},
        source=snowflake_database_table.tabular_source,
    )
    await request_input.materialize(
        session, destination_table, None, datetime(2011, 3, 8), datetime(2012, 5, 9)
    )

    assert session.list_table_schema.call_args_list == [
        call(table_name="sf_table", database_name="sf_database", schema_name="sf_schema")
    ]

    expected_query = textwrap.dedent(
        """
        CREATE TABLE `sf_database`.`sf_schema`.`my_materialized_table` AS
        SELECT
          `a`,
          `b`,
          `POINT_IN_TIME`
        FROM (
          SELECT
            `a` AS `a`,
            `b` AS `b`,
            `event_timestamp` AS `POINT_IN_TIME`
          FROM (
            SELECT
              *
            FROM `sf_database`.`sf_schema`.`sf_table`
          )
        )
        WHERE
            CAST(`POINT_IN_TIME` AS DATETIME) >= CAST('2011-03-08T00:00:00' AS DATETIME) AND
            CAST(`POINT_IN_TIME` AS DATETIME) < CAST('2012-05-09T00:00:00' AS DATETIME)
        """
    ).strip()
    assert session.execute_query_long_running.call_args_list == [call(expected_query)]


@pytest.mark.asyncio
async def test_materialize__with_sample_timestamp_no_columns_rename(
    session, snowflake_event_table, destination_table
):
    """
    Test materializing with sample from timestamp without column rename,
    expect to have no time filtering since there is no POINT_IN_TIME column
    """
    view = snowflake_event_table.get_view()
    view = view[view.col_int == 1]
    pruned_graph, mapped_node = view.extract_pruned_graph_and_node()
    request_input = ViewRequestInput(
        graph=pruned_graph,
        node_name=mapped_node.name,
    )
    await request_input.materialize(
        session, destination_table, None, datetime(2011, 3, 8), datetime(2012, 5, 9)
    )

    expected_query = textwrap.dedent(
        """
        CREATE TABLE "sf_database"."sf_schema"."my_materialized_table" AS
        SELECT
          "col_int",
          "col_float",
          "col_char",
          "col_text",
          "col_binary",
          "col_boolean",
          "event_timestamp",
          "cust_id"
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
          WHERE
            (
              "col_int" = 1
            )
        )
        """
    ).strip()
    assert session.execute_query_long_running.call_args_list == [call(expected_query)]


@pytest.mark.asyncio
async def test_materialize__with_sample_timestamp_with_tz(
    session, snowflake_event_table, destination_table
):
    """
    Test materializing with sample from timestamp with timezone
    """
    view = snowflake_event_table.get_view()
    view = view[view.col_int == 1]
    pruned_graph, mapped_node = view.extract_pruned_graph_and_node()
    request_input = ViewRequestInput(
        graph=pruned_graph,
        node_name=mapped_node.name,
        columns_rename_mapping={"event_timestamp": "POINT_IN_TIME", "cust_id": "CUST_ID"},
    )

    await request_input.materialize(
        session,
        destination_table,
        None,
        datetime(2011, 3, 8, tzinfo=tz.gettz("Asia/Singapore")),
        datetime(2012, 5, 9, tzinfo=tz.gettz("Asia/Singapore")),
    )

    expected_query = textwrap.dedent(
        """
        CREATE TABLE "sf_database"."sf_schema"."my_materialized_table" AS
        SELECT
          "col_int",
          "col_float",
          "col_char",
          "col_text",
          "col_binary",
          "col_boolean",
          "POINT_IN_TIME",
          "CUST_ID"
        FROM (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "col_char" AS "col_char",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP) AS "POINT_IN_TIME",
            "cust_id" AS "CUST_ID"
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
            WHERE
              (
                "col_int" = 1
              )
          )
        )
        WHERE
            "POINT_IN_TIME" >= CAST('2011-03-07T16:00:00' AS TIMESTAMP) AND
            "POINT_IN_TIME" < CAST('2012-05-08T16:00:00' AS TIMESTAMP)
        """
    ).strip()
    assert session.execute_query_long_running.call_args_list == [call(expected_query)]
