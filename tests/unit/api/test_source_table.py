"""
Unit test for SourceTable
"""
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest

from featurebyte.api.observation_table import ObservationTable
from featurebyte.enum import DBVarType, TableDataType
from featurebyte.exception import RecordCreationException
from tests.util.helper import check_observation_table_creation_query, check_sdk_code_generation


def test_database_table(snowflake_database_table, expected_snowflake_table_preview_query):
    """
    Test SourceTable preview functionality
    """
    assert snowflake_database_table.preview_sql() == expected_snowflake_table_preview_query
    expected_dtypes = pd.Series(
        {
            "col_int": DBVarType.INT,
            "col_float": DBVarType.FLOAT,
            "col_char": DBVarType.CHAR,
            "col_text": DBVarType.VARCHAR,
            "col_binary": DBVarType.BINARY,
            "col_boolean": DBVarType.BOOL,
            "event_timestamp": DBVarType.TIMESTAMP_TZ,
            "created_at": DBVarType.TIMESTAMP_TZ,
            "cust_id": DBVarType.INT,
        }
    )
    pd.testing.assert_series_equal(snowflake_database_table.dtypes, expected_dtypes)


def test_database_table_node_parameters(snowflake_database_table):
    """Test database table node parameters"""
    node_params = snowflake_database_table.frame.node.parameters
    assert node_params.type == TableDataType.SOURCE_TABLE


def test_database_table_get_input_node(snowflake_database_table):
    """Test database table get input node"""
    pruned_graph, mapped_node = snowflake_database_table.frame.extract_pruned_graph_and_node()
    input_node_dict = pruned_graph.get_input_node(mapped_node.name).dict()
    assert input_node_dict["name"] == "input_1"
    assert input_node_dict["parameters"]["type"] == "source_table"


def test_sdk_code_generation(snowflake_database_table, update_fixtures):
    """Check SDK code generation for unsaved table"""
    check_sdk_code_generation(
        snowflake_database_table.frame,
        to_use_saved_data=False,
        fixture_path="tests/fixtures/sdk_code/generic_table.py",
        update_fixtures=update_fixtures,
        table_id=None,
    )

    # check that unsaved & saved version generate the same result for generic table
    sdk_code = snowflake_database_table.frame._generate_code(to_use_saved_data=True)
    assert sdk_code == snowflake_database_table.frame._generate_code(to_use_saved_data=False)


def test_get_or_create_event_table__get_from_persistent(
    snowflake_database_table, snowflake_event_table
):
    """Test get or create event table"""
    assert (
        snowflake_database_table.get_or_create_event_table(
            name=snowflake_event_table.name,
            event_timestamp_column=snowflake_event_table.event_timestamp_column,
            event_id_column=snowflake_event_table.event_id_column,
            record_creation_timestamp_column=snowflake_event_table.record_creation_timestamp_column,
        )
        == snowflake_event_table
    )


def test_get_or_create_item_table__get_from_persistent(
    snowflake_database_table_item_table, snowflake_item_table, snowflake_event_table
):
    """Test get or create item table"""
    assert (
        snowflake_database_table_item_table.get_or_create_item_table(
            name=snowflake_item_table.name,
            event_id_column=snowflake_item_table.event_id_column,
            item_id_column=snowflake_item_table.item_id_column,
            event_table_name=snowflake_event_table.name,
            record_creation_timestamp_column=snowflake_item_table.record_creation_timestamp_column,
        )
        == snowflake_item_table
    )


def test_get_or_create_dimension_table__get_from_persistent(
    snowflake_database_table_dimension_table, snowflake_dimension_table
):
    """Test get or create dimension table"""
    assert (
        snowflake_database_table_dimension_table.get_or_create_dimension_table(
            name=snowflake_dimension_table.name,
            dimension_id_column=snowflake_dimension_table.dimension_id_column,
            record_creation_timestamp_column=snowflake_dimension_table.record_creation_timestamp_column,
        )
        == snowflake_dimension_table
    )


def test_get_or_create_scd_table__get_from_persistent(
    snowflake_database_table_scd_table, snowflake_scd_table
):
    """Test get or create scd table"""
    assert (
        snowflake_database_table_scd_table.get_or_create_scd_table(
            name=snowflake_scd_table.name,
            natural_key_column=snowflake_scd_table.natural_key_column,
            effective_timestamp_column=snowflake_scd_table.effective_timestamp_column,
            surrogate_key_column=snowflake_scd_table.surrogate_key_column,
            current_flag_column=snowflake_scd_table.current_flag_column,
            record_creation_timestamp_column=snowflake_scd_table.record_creation_timestamp_column,
        )
        == snowflake_scd_table
    )


def test_get_or_create_event_table__create(snowflake_database_table):
    """Test get or create event table"""
    event_table = snowflake_database_table.get_or_create_event_table(
        name="some_event_table",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
        record_creation_timestamp_column="created_at",
    )
    assert event_table.name == "some_event_table"
    assert event_table.event_id_column == "col_int"
    assert event_table.event_timestamp_column == "event_timestamp"
    assert event_table.record_creation_timestamp_column == "created_at"


def test_get_or_create_item_table__create(
    snowflake_database_table_item_table, snowflake_event_table
):
    """Test get or create item table"""
    item_table = snowflake_database_table_item_table.get_or_create_item_table(
        name="some_item_table",
        event_id_column="event_id_col",
        item_id_column="item_id_col",
        event_table_name=snowflake_event_table.name,
    )
    assert item_table.name == "some_item_table"
    assert item_table.event_id_column == "event_id_col"
    assert item_table.item_id_column == "item_id_col"
    assert item_table.event_table_id == snowflake_event_table.id


def test_get_or_create_dimension_table__create(snowflake_database_table_dimension_table):
    """Test get or create dimension table"""
    dimension_table = snowflake_database_table_dimension_table.get_or_create_dimension_table(
        name="some_dimension_table",
        dimension_id_column="col_int",
        record_creation_timestamp_column="created_at",
    )
    assert dimension_table.name == "some_dimension_table"
    assert dimension_table.dimension_id_column == "col_int"
    assert dimension_table.record_creation_timestamp_column == "created_at"


def test_get_or_create_scd_table__create(snowflake_database_table_scd_table):
    """Test get or create scd table"""
    scd_table = snowflake_database_table_scd_table.get_or_create_scd_table(
        name="some_scd_table",
        natural_key_column="col_text",
        surrogate_key_column="col_int",
        effective_timestamp_column="effective_timestamp",
        end_timestamp_column="end_timestamp",
        current_flag_column="is_active",
    )
    assert scd_table.name == "some_scd_table"
    assert scd_table.natural_key_column == "col_text"
    assert scd_table.surrogate_key_column == "col_int"
    assert scd_table.effective_timestamp_column == "effective_timestamp"
    assert scd_table.end_timestamp_column == "end_timestamp"
    assert scd_table.current_flag_column == "is_active"


@pytest.mark.usefixtures("patched_observation_table_service")
def test_create_observation_table(snowflake_database_table, snowflake_execute_query):
    """
    Test creating ObservationTable from SourceTable
    """
    observation_table = snowflake_database_table.create_observation_table(
        "my_observation_table",
        columns=["event_timestamp", "cust_id"],
        columns_rename_mapping={"event_timestamp": "POINT_IN_TIME"},
    )

    # Check return type
    assert isinstance(observation_table, ObservationTable)
    assert observation_table.name == "my_observation_table"

    # Check that the correct query was executed
    query = snowflake_execute_query.call_args[0][0]
    check_observation_table_creation_query(
        query,
        """
        CREATE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE" AS
        SELECT
          "event_timestamp" AS "POINT_IN_TIME",
          "cust_id" AS "cust_id"
        FROM (
          SELECT
            *
          FROM "sf_database"."sf_schema"."sf_table"
        )
        """,
    )


@pytest.mark.usefixtures("patched_observation_table_service")
def test_create_observation_table_with_sample_rows(
    snowflake_database_table, snowflake_execute_query
):
    """
    Test creating ObservationTable from SourceTable with sampling
    """
    with patch(
        "featurebyte.models.request_input.BaseRequestInput.get_row_count",
        AsyncMock(return_value=1000),
    ):
        observation_table = snowflake_database_table.create_observation_table(
            "my_observation_table",
            sample_rows=100,
        )

    # Check return type
    assert isinstance(observation_table, ObservationTable)
    assert observation_table.name == "my_observation_table"

    # Check that the correct query was executed
    query = snowflake_execute_query.call_args[0][0]
    check_observation_table_creation_query(
        query,
        """
        CREATE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE" AS
        SELECT
          *
        FROM (
          SELECT
            *
          FROM "sf_database"."sf_schema"."sf_table"
        ) TABLESAMPLE(14)
        LIMIT 100
        """,
    )


def test_bad_materialized_tables_cleaned_up(
    snowflake_database_table,
    snowflake_execute_query,
):
    """
    Test that bad materialized tables are cleaned up on any validation errors
    """
    with patch(
        "featurebyte.service.observation_table.ObservationTableService.validate_materialized_table_and_get_metadata",
        Mock(side_effect=RuntimeError("Something went wrong")),
    ):
        with pytest.raises(RecordCreationException) as exc:
            snowflake_database_table.create_observation_table("my_observation_table")

    assert "RuntimeError: Something went wrong" in str(exc.value)
    assert snowflake_execute_query.call_args[0][0].startswith(
        'DROP TABLE IF EXISTS "sf_database"."sf_schema"."OBSERVATION_TABLE_'
    )
