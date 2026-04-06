"""
Unit test for ForecastTable class
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from typeguard import TypeCheckError

from featurebyte import ForecastTable
from featurebyte.enum import TableDataType
from featurebyte.exception import (
    DuplicatedRecordException,
    ObjectHasBeenSavedError,
    RecordCreationException,
    RecordRetrievalException,
)
from featurebyte.models.forecast_table import ForecastTableModel
from tests.unit.api.base_table_test import BaseTableTestSuite, DataType
from tests.util.helper import check_sdk_code_generation


class TestForecastTableTestSuite(BaseTableTestSuite):
    """Test ForecastTable"""

    data_type = DataType.FORECAST_DATA
    col = "col_int"
    expected_columns = {
        "col_char",
        "col_float",
        "col_boolean",
        "forecast_timestamp",
        "col_text",
        "created_at",
        "col_binary",
        "col_int",
        "effective_timestamp",
    }
    expected_table_sql = """
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      CAST("col_text" AS VARCHAR) AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("forecast_timestamp" AS VARCHAR) AS "forecast_timestamp",
      "effective_timestamp" AS "effective_timestamp",
      CAST("created_at" AS VARCHAR) AS "created_at"
    FROM "sf_database"."sf_schema"."forecast_table"
    LIMIT 10
    """
    expected_table_column_sql = """
    SELECT
      "col_int" AS "col_int"
    FROM "sf_database"."sf_schema"."forecast_table"
    LIMIT 10
    """
    expected_clean_table_sql = """
    SELECT
      CAST(CASE WHEN (
        "col_int" IS NULL
      ) THEN 0 ELSE "col_int" END AS BIGINT) AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      CAST("col_text" AS VARCHAR) AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("forecast_timestamp" AS VARCHAR) AS "forecast_timestamp",
      "effective_timestamp" AS "effective_timestamp",
      CAST("created_at" AS VARCHAR) AS "created_at"
    FROM "sf_database"."sf_schema"."forecast_table"
    LIMIT 10
    """
    expected_clean_table_column_sql = """
    SELECT
      CAST(CASE WHEN (
        "col_int" IS NULL
      ) THEN 0 ELSE "col_int" END AS BIGINT) AS "col_int"
    FROM "sf_database"."sf_schema"."forecast_table"
    LIMIT 10
    """
    expected_timestamp_column = "effective_timestamp"
    expected_special_columns = [
        "col_int",
        "effective_timestamp",
        "forecast_timestamp",
        "created_at",
    ]


def test_create_forecast_table(snowflake_database_forecast_table, catalog):
    """
    Test ForecastTable creation using tabular source
    """
    _ = catalog

    forecast_table = snowflake_database_forecast_table.create_forecast_table(
        name="sf_forecast_table",
        natural_key_column="col_int",
        effective_timestamp_column="effective_timestamp",
        forecast_timestamp_column="forecast_timestamp",
        record_creation_timestamp_column="created_at",
        description="test forecast table",
    )

    # check that node parameter is set properly
    node_params = forecast_table.frame.node.parameters
    assert node_params.id == forecast_table.id
    assert node_params.type == TableDataType.FORECAST_TABLE

    # check that forecast table columns for autocompletion
    assert set(forecast_table.columns).issubset(dir(forecast_table))
    assert forecast_table._ipython_key_completions_() == set(forecast_table.columns)

    # user input validation
    with pytest.raises(TypeCheckError) as exc:
        snowflake_database_forecast_table.create_forecast_table(
            name=123,
            effective_timestamp_column=234,
            forecast_timestamp_column=345,
        )
    assert 'argument "name" (int) is not an instance of str' in str(exc.value)


def test_create_forecast_table__without_natural_key(snowflake_database_forecast_table, catalog):
    """
    Test ForecastTable creation without natural_key_column
    """
    _ = catalog

    forecast_table = snowflake_database_forecast_table.create_forecast_table(
        name="sf_forecast_table",
        effective_timestamp_column="effective_timestamp",
        forecast_timestamp_column="forecast_timestamp",
    )
    assert forecast_table.natural_key_column is None
    assert forecast_table.effective_timestamp_column == "effective_timestamp"
    assert forecast_table.forecast_timestamp_column == "forecast_timestamp"


def test_create_forecast_table__duplicated_record(
    saved_forecast_table, snowflake_database_forecast_table
):
    """
    Test ForecastTable creation failure due to duplicated forecast table name
    """
    _ = saved_forecast_table
    with pytest.raises(DuplicatedRecordException) as exc:
        snowflake_database_forecast_table.create_forecast_table(
            name="sf_forecast_table",
            natural_key_column="col_int",
            effective_timestamp_column="effective_timestamp",
            forecast_timestamp_column="forecast_timestamp",
            record_creation_timestamp_column="created_at",
        )
    assert (
        'ForecastTable (forecast_table.name: "sf_forecast_table") exists in saved record.'
        in str(exc.value)
    )


def test_create_forecast_table__retrieval_exception(snowflake_database_forecast_table):
    """
    Test ForecastTable creation failure due to retrieval exception
    """
    with pytest.raises(RecordRetrievalException):
        with patch("featurebyte.api.base_table.Configurations"):
            snowflake_database_forecast_table.create_forecast_table(
                name="sf_forecast_table",
                natural_key_column="col_int",
                effective_timestamp_column="effective_timestamp",
                forecast_timestamp_column="forecast_timestamp",
                record_creation_timestamp_column="created_at",
            )


def test_forecast_table__save__exceptions(saved_forecast_table):
    """
    Test save forecast table failure due to conflict
    """
    # test duplicated record exception when record exists
    with pytest.raises(ObjectHasBeenSavedError) as exc:
        saved_forecast_table.save()
    expected_msg = f'ForecastTable (id: "{saved_forecast_table.id}") has been saved before.'
    assert expected_msg in str(exc.value)


def test_forecast_table__record_creation_exception(
    snowflake_database_forecast_table, snowflake_forecast_table_id, catalog
):
    """
    Test save forecast table failure due to conflict
    """
    _ = catalog
    with pytest.raises(RecordCreationException):
        with patch("featurebyte.api.savable_api_object.Configurations"):
            snowflake_database_forecast_table.create_forecast_table(
                name="sf_forecast_table",
                natural_key_column="col_int",
                effective_timestamp_column="effective_timestamp",
                forecast_timestamp_column="forecast_timestamp",
                record_creation_timestamp_column="created_at",
                _id=snowflake_forecast_table_id,
            )


def test_info(saved_forecast_table):
    """
    Test info
    """
    info_dict = saved_forecast_table.info()
    expected_info = {
        "name": "sf_forecast_table",
        "effective_timestamp_column": "effective_timestamp",
        "effective_timestamp_schema": None,
        "forecast_timestamp_column": "forecast_timestamp",
        "forecast_timestamp_schema": None,
        "record_creation_timestamp_column": "created_at",
        "status": "PUBLIC_DRAFT",
        "entities": [],
        "column_count": 9,
        "table_details": {
            "database_name": "sf_database",
            "schema_name": "sf_schema",
            "table_name": "forecast_table",
        },
        "catalog_name": "catalog",
    }
    assert info_dict.items() > expected_info.items(), info_dict
    assert "created_at" in info_dict, info_dict


def test_accessing_forecast_table_attributes(snowflake_forecast_table):
    """Test accessing forecast table object attributes"""
    assert snowflake_forecast_table.saved
    assert snowflake_forecast_table.record_creation_timestamp_column == "created_at"
    assert snowflake_forecast_table.effective_timestamp_column == "effective_timestamp"
    assert snowflake_forecast_table.forecast_timestamp_column == "forecast_timestamp"
    assert snowflake_forecast_table.natural_key_column == "col_int"
    assert snowflake_forecast_table.timestamp_column == "effective_timestamp"
    assert snowflake_forecast_table.effective_timestamp_schema is None
    assert snowflake_forecast_table.forecast_timestamp_schema is None


def test_accessing_saved_forecast_table_attributes(saved_forecast_table):
    """Test accessing saved forecast table object attributes"""
    assert saved_forecast_table.saved
    assert isinstance(saved_forecast_table.cached_model, ForecastTableModel)
    assert saved_forecast_table.record_creation_timestamp_column == "created_at"
    assert saved_forecast_table.effective_timestamp_column == "effective_timestamp"
    assert saved_forecast_table.forecast_timestamp_column == "forecast_timestamp"
    assert saved_forecast_table.natural_key_column == "col_int"
    assert saved_forecast_table.timestamp_column == "effective_timestamp"


def test_get_forecast_table(snowflake_forecast_table, mock_config_path_env):
    """
    Test ForecastTable.get function
    """
    _ = mock_config_path_env

    # load the forecast table from the persistent
    loaded_forecast_table = ForecastTable.get(snowflake_forecast_table.name)
    assert loaded_forecast_table.saved is True
    assert loaded_forecast_table == snowflake_forecast_table
    assert ForecastTable.get_by_id(id=snowflake_forecast_table.id) == snowflake_forecast_table

    with pytest.raises(RecordRetrievalException) as exc:
        ForecastTable.get("unknown_forecast_table")

    expected_msg = (
        'ForecastTable (name: "unknown_forecast_table") not found. '
        "Please save the ForecastTable object first."
    )
    assert expected_msg in str(exc.value)


def test_sdk_code_generation(saved_forecast_table, update_fixtures):
    """Check SDK code generation for unsaved table"""
    check_sdk_code_generation(
        saved_forecast_table.frame,
        to_use_saved_data=False,
        fixture_path="tests/fixtures/sdk_code/forecast_table.py",
        update_fixtures=update_fixtures,
        table_id=saved_forecast_table.id,
    )


def test_sdk_code_generation_on_saved_data(saved_forecast_table, update_fixtures):
    """Check SDK code generation for saved table"""
    check_sdk_code_generation(
        saved_forecast_table.frame,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/saved_forecast_table.py",
        update_fixtures=update_fixtures,
        table_id=saved_forecast_table.id,
    )


def test_get_view(saved_forecast_table):
    """Test get_view returns ForecastView"""
    from featurebyte.api.forecast_view import ForecastView

    forecast_view = saved_forecast_table.get_view()
    assert isinstance(forecast_view, ForecastView)
    assert forecast_view.natural_key_column == "col_int"
    assert forecast_view.timestamp_column == "effective_timestamp"
    assert forecast_view.effective_timestamp_column == "effective_timestamp"
    assert forecast_view.forecast_timestamp_column == "forecast_timestamp"


def test_get_view__auto_mode_drops_record_creation_timestamp(saved_forecast_table):
    """Test get_view in auto mode drops record_creation_timestamp_column"""
    forecast_view = saved_forecast_table.get_view()
    # record_creation_timestamp_column should be dropped in auto mode
    assert "created_at" not in forecast_view.columns


def test_get_view__manual_mode(saved_forecast_table):
    """Test get_view in manual mode"""
    from featurebyte.enum import ViewMode

    forecast_view = saved_forecast_table.get_view(view_mode=ViewMode.MANUAL)
    # In manual mode, record_creation_timestamp_column should be kept
    assert "created_at" in forecast_view.columns


def test_get_view__without_natural_key(snowflake_database_forecast_table, catalog):
    """Test get_view on ForecastTable without natural_key_column"""
    _ = catalog

    forecast_table = snowflake_database_forecast_table.create_forecast_table(
        name="sf_forecast_table",
        effective_timestamp_column="effective_timestamp",
        forecast_timestamp_column="forecast_timestamp",
    )
    from featurebyte.api.forecast_view import ForecastView

    forecast_view = forecast_table.get_view()
    assert isinstance(forecast_view, ForecastView)
    assert forecast_view.natural_key_column is None
