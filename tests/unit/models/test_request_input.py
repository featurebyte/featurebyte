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
                "b": ColumnSpecWithDescription(name="b", dtype=DBVarType.INT),
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
async def test_materialize__with_columns_only(
    session, snowflake_database_table, destination_table, snowflake_feature_store
):
    """
    Test materializing when columns filter is specified
    """
    request_input = SourceTableRequestInput(
        columns=["a", "b"],
        source=snowflake_database_table.tabular_source,
    )
    await request_input.materialize(session, destination_table, snowflake_feature_store, None)

    assert len(session.list_table_schema.call_args_list) == 2
    assert (
        call(table_name="sf_table", database_name="sf_database", schema_name="sf_schema")
        in session.list_table_schema.call_args_list
        or call(table_name="sf_table", schema_name="sf_schema", database_name="sf_database")
        in session.list_table_schema.call_args_list
    )

    expected_query = textwrap.dedent(
        """
        CREATE TABLE "sf_database"."sf_schema"."my_materialized_table" AS
        SELECT
          "a" AS "a",
          "b" AS "b"
        FROM (
          SELECT
            "a" AS "a",
            "b" AS "b",
            "event_timestamp" AS "event_timestamp"
          FROM "sf_database"."sf_schema"."sf_table"
        )
        """
    ).strip()
    assert session.execute_query_long_running.call_args_list == [call(expected_query)]


@pytest.mark.asyncio
async def test_materialize__with_columns_and_renames(
    session, snowflake_database_table, destination_table, snowflake_feature_store
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
        session,
        destination_table,
        snowflake_feature_store,
        None,
        columns_to_exclude_missing_values=["NEW_B"],
    )

    assert len(session.list_table_schema.call_args_list) == 2
    assert (
        call(table_name="sf_table", database_name="sf_database", schema_name="sf_schema")
        in session.list_table_schema.call_args_list
        or call(table_name="sf_table", schema_name="sf_schema", database_name="sf_database")
        in session.list_table_schema.call_args_list
    )

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
              "a" AS "a",
              "b" AS "b",
              "event_timestamp" AS "event_timestamp"
            FROM "sf_database"."sf_schema"."sf_table"
          )
        )
        WHERE
              "NEW_B" IS NOT NULL
        """
    ).strip()
    assert session.execute_query_long_running.call_args_list == [call(expected_query)]


@pytest.mark.asyncio
async def test_materialize__with_renames_only(
    session, snowflake_database_table, destination_table, snowflake_feature_store
):
    """
    Test materializing when only the columns rename mapping is specified
    """
    request_input = SourceTableRequestInput(
        columns=["a", "b"],
        columns_rename_mapping={"b": "NEW_B"},
        source=snowflake_database_table.tabular_source,
    )
    await request_input.materialize(
        session,
        destination_table,
        snowflake_feature_store,
        None,
        columns_to_exclude_missing_values=["NEW_B"],
    )

    assert len(session.list_table_schema.call_args_list) == 2
    assert (
        call(table_name="sf_table", database_name="sf_database", schema_name="sf_schema")
        in session.list_table_schema.call_args_list
        or call(table_name="sf_table", schema_name="sf_schema", database_name="sf_database")
        in session.list_table_schema.call_args_list
    )

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
              "a" AS "a",
              "b" AS "b",
              "event_timestamp" AS "event_timestamp"
            FROM "sf_database"."sf_schema"."sf_table"
          )
        )
        WHERE
              "NEW_B" IS NOT NULL
        """
    ).strip()
    assert session.execute_query_long_running.call_args_list == [call(expected_query)]


@pytest.mark.asyncio
async def test_materialize__invalid_columns(
    session, snowflake_database_table, destination_table, snowflake_feature_store
):
    """
    Test invalid columns filter
    """
    request_input = SourceTableRequestInput(
        columns=["unknown_column"],
        source=snowflake_database_table.tabular_source,
    )
    with pytest.raises(ColumnNotFoundError) as exc:
        await request_input.materialize(session, destination_table, snowflake_feature_store, None)
    assert "Columns ['unknown_column'] not found" in str(exc.value)


@pytest.mark.asyncio
async def test_materialize__invalid_rename_mapping(
    session, snowflake_database_table, destination_table, snowflake_feature_store
):
    """
    Test invalid columns rename mapping
    """
    request_input = SourceTableRequestInput(
        columns_rename_mapping={"unknown_column": "NEW_COL"},
        source=snowflake_database_table.tabular_source,
    )
    with pytest.raises(ColumnNotFoundError) as exc:
        await request_input.materialize(session, destination_table, snowflake_feature_store, None)
    assert "Columns ['unknown_column'] not found" in str(exc.value)


@pytest.mark.asyncio
async def test_materialize__from_view_with_columns_and_renames(
    session, destination_table, snowflake_event_table, snowflake_feature_store
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
        session,
        destination_table,
        snowflake_feature_store,
        None,
        columns_to_exclude_missing_values=["POINT_IN_TIME"],
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
    session,
    destination_table,
    snowflake_event_table,
    bigquery_source_info,
    update_fixtures,
    snowflake_feature_store,
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
    await request_input.materialize(session, destination_table, snowflake_feature_store, 100)

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
    session, snowflake_event_table, destination_table, snowflake_feature_store
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
    await request_input.materialize(
        session, destination_table, snowflake_feature_store, None, None, datetime(2011, 3, 8)
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
            "POINT_IN_TIME" < CAST('2011-03-08T00:00:00' AS TIMESTAMP)
        """
    ).strip()
    assert session.execute_query_long_running.call_args_list == [call(expected_query)]


@pytest.mark.asyncio
async def test_materialize__with_sample_timestamp_biqquery(
    session,
    snowflake_database_table,
    destination_table,
    bigquery_source_info,
    snowflake_feature_store,
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
        session,
        destination_table,
        snowflake_feature_store,
        None,
        datetime(2011, 3, 8),
        datetime(2012, 5, 9),
    )

    assert len(session.list_table_schema.call_args_list) == 2
    assert (
        call(table_name="sf_table", database_name="sf_database", schema_name="sf_schema")
        in session.list_table_schema.call_args_list
        or call(table_name="sf_table", schema_name="sf_schema", database_name="sf_database")
        in session.list_table_schema.call_args_list
    )

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
              `a` AS `a`,
              `b` AS `b`,
              `event_timestamp` AS `event_timestamp`
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
    session, snowflake_event_table, destination_table, snowflake_feature_store
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
        session,
        destination_table,
        snowflake_feature_store,
        None,
        datetime(2011, 3, 8),
        datetime(2012, 5, 9),
    )

    expected_query = textwrap.dedent(
        """
        CREATE TABLE "sf_database"."sf_schema"."my_materialized_table" AS
        SELECT
          "col_int" AS "col_int",
          "col_float" AS "col_float",
          "col_char" AS "col_char",
          "col_text" AS "col_text",
          "col_binary" AS "col_binary",
          "col_boolean" AS "col_boolean",
          "event_timestamp" AS "event_timestamp",
          "cust_id" AS "cust_id"
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
    session, snowflake_event_table, destination_table, snowflake_feature_store
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
        snowflake_feature_store,
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


class TestSplitInfo:
    """Tests for SplitInfo model validation"""

    def test_split_info_valid_two_way_split(self):
        """Test valid 2-way split"""
        from featurebyte.models.request_input import SplitInfo

        split_info = SplitInfo(split_index=0, split_ratios=[0.7, 0.3], seed=42)
        assert split_info.split_index == 0
        assert split_info.split_ratios == [0.7, 0.3]
        assert split_info.seed == 42

    def test_split_info_valid_three_way_split(self):
        """Test valid 3-way split"""
        from featurebyte.models.request_input import SplitInfo

        split_info = SplitInfo(split_index=1, split_ratios=[0.6, 0.2, 0.2], seed=1234)
        assert split_info.split_index == 1
        assert split_info.split_ratios == [0.6, 0.2, 0.2]

    def test_split_info_default_seed(self):
        """Test default seed value"""
        from featurebyte.models.request_input import SplitInfo

        split_info = SplitInfo(split_index=0, split_ratios=[0.5, 0.5])
        assert split_info.seed == 1234

    def test_split_info_invalid_ratios_not_sum_to_one(self):
        """Test that ratios must sum to 1.0"""
        from featurebyte.models.request_input import SplitInfo

        with pytest.raises(ValueError) as exc:
            SplitInfo(split_index=0, split_ratios=[0.5, 0.3])
        assert "Split ratios must sum to 1.0" in str(exc.value)

    def test_split_info_invalid_ratio_zero(self):
        """Test that ratio cannot be zero"""
        from featurebyte.models.request_input import SplitInfo

        with pytest.raises(ValueError) as exc:
            SplitInfo(split_index=0, split_ratios=[0.0, 1.0])
        assert "Each split ratio must be between 0 and 1" in str(exc.value)

    def test_split_info_invalid_ratio_negative(self):
        """Test that ratio cannot be negative"""
        from featurebyte.models.request_input import SplitInfo

        with pytest.raises(ValueError) as exc:
            SplitInfo(split_index=0, split_ratios=[-0.3, 1.3])
        assert "Each split ratio must be between 0 and 1" in str(exc.value)

    def test_split_info_invalid_ratio_greater_than_one(self):
        """Test that ratio cannot be greater than 1"""
        from featurebyte.models.request_input import SplitInfo

        with pytest.raises(ValueError) as exc:
            SplitInfo(split_index=0, split_ratios=[1.5, -0.5])
        assert "Each split ratio must be between 0 and 1" in str(exc.value)

    def test_split_info_invalid_split_index_out_of_range(self):
        """Test that split_index must be less than number of splits"""
        from featurebyte.models.request_input import SplitInfo

        with pytest.raises(ValueError) as exc:
            SplitInfo(split_index=2, split_ratios=[0.7, 0.3])
        assert "split_index (2) must be less than number of splits (2)" in str(exc.value)

    def test_split_info_invalid_too_few_ratios(self):
        """Test that at least 2 ratios are required"""
        from featurebyte.models.request_input import SplitInfo

        with pytest.raises(ValueError) as exc:
            SplitInfo(split_index=0, split_ratios=[1.0])
        assert "List should have at least 2 items" in str(exc.value)

    def test_split_info_invalid_too_many_ratios(self):
        """Test that at most 3 ratios are allowed"""
        from featurebyte.models.request_input import SplitInfo

        with pytest.raises(ValueError) as exc:
            SplitInfo(split_index=0, split_ratios=[0.25, 0.25, 0.25, 0.25])
        assert "List should have at most 3 items" in str(exc.value)


class TestGetSplitSql:
    """Tests for get_split_sql SQL generation"""

    def test_get_split_sql_two_way_first_split(self, adapter):
        """Test SQL generation for the first split of a 2-way split"""
        from sqlglot import expressions

        from featurebyte.models.request_input import BaseRequestInput, SplitInfo

        split_info = SplitInfo(split_index=0, split_ratios=[0.7, 0.3], seed=42)
        select_expr = expressions.select(
            expressions.alias_(expressions.Column(this="a"), "a"),
            expressions.alias_(expressions.Column(this="b"), "b"),
        ).from_("my_table")

        result = BaseRequestInput.get_split_sql(adapter, select_expr, split_info)
        sql = result.sql(pretty=True, dialect="snowflake")

        # First split should be: prob < 0.7
        assert '"__fb_split_prob" < 0.7' in sql
        # Should select original columns
        assert '"a"' in sql
        assert '"b"' in sql
        # Prob column appears in inner select (creation) and WHERE clause
        assert sql.count('"__fb_split_prob"') == 2
        # Final SELECT should only have original columns (not prob)
        first_select = sql.split("FROM")[0]
        assert "__fb_split_prob" not in first_select

    def test_get_split_sql_two_way_second_split(self, adapter):
        """Test SQL generation for the second split of a 2-way split"""
        from sqlglot import expressions

        from featurebyte.models.request_input import BaseRequestInput, SplitInfo

        split_info = SplitInfo(split_index=1, split_ratios=[0.7, 0.3], seed=42)
        select_expr = expressions.select(
            expressions.alias_(expressions.Column(this="a"), "a"),
            expressions.alias_(expressions.Column(this="b"), "b"),
        ).from_("my_table")

        result = BaseRequestInput.get_split_sql(adapter, select_expr, split_info)
        sql = result.sql(pretty=True, dialect="snowflake")

        # Second split should be: 0.7 <= prob < 1.0
        assert '"__fb_split_prob" >= 0.7' in sql
        assert '"__fb_split_prob" < 1.0' in sql

    def test_get_split_sql_three_way_splits(self, adapter):
        """Test SQL generation for all splits of a 3-way split"""
        from sqlglot import expressions

        from featurebyte.models.request_input import BaseRequestInput, SplitInfo

        select_expr = expressions.select(
            expressions.alias_(expressions.Column(this="x"), "x"),
        ).from_("data_table")

        # Split 0: prob < 0.6
        split_info_0 = SplitInfo(split_index=0, split_ratios=[0.6, 0.2, 0.2], seed=1234)
        result_0 = BaseRequestInput.get_split_sql(adapter, select_expr, split_info_0)
        sql_0 = result_0.sql(pretty=True, dialect="snowflake")
        assert '"__fb_split_prob" < 0.6' in sql_0

        # Split 1: 0.6 <= prob < 0.8
        split_info_1 = SplitInfo(split_index=1, split_ratios=[0.6, 0.2, 0.2], seed=1234)
        result_1 = BaseRequestInput.get_split_sql(adapter, select_expr, split_info_1)
        sql_1 = result_1.sql(pretty=True, dialect="snowflake")
        assert '"__fb_split_prob" >= 0.6' in sql_1
        assert '"__fb_split_prob" < 0.8' in sql_1

        # Split 2: 0.8 <= prob < 1.0
        split_info_2 = SplitInfo(split_index=2, split_ratios=[0.6, 0.2, 0.2], seed=1234)
        result_2 = BaseRequestInput.get_split_sql(adapter, select_expr, split_info_2)
        sql_2 = result_2.sql(pretty=True, dialect="snowflake")
        assert '"__fb_split_prob" >= 0.8' in sql_2
        assert '"__fb_split_prob" < 1.0' in sql_2

    def test_get_split_sql_preserves_columns(self, adapter):
        """Test that split SQL preserves all original columns"""
        from sqlglot import expressions

        from featurebyte.models.request_input import BaseRequestInput, SplitInfo

        split_info = SplitInfo(split_index=0, split_ratios=[0.5, 0.5], seed=42)
        select_expr = expressions.select(
            expressions.alias_(expressions.Column(this="col1"), "col1"),
            expressions.alias_(expressions.Column(this="col2"), "col2"),
            expressions.alias_(expressions.Column(this="col3"), "col3"),
        ).from_("source")

        result = BaseRequestInput.get_split_sql(adapter, select_expr, split_info)
        sql = result.sql(pretty=True, dialect="snowflake")

        # All original columns should be in the SELECT
        assert '"col1"' in sql
        assert '"col2"' in sql
        assert '"col3"' in sql
