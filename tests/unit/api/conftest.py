"""
Common test fixtures used across api test directories
"""
import textwrap
from unittest.mock import patch

import pytest

from featurebyte.api.event_data import EventData


@pytest.fixture()
def expected_snowflake_table_preview_query() -> str:
    """
    Expected preview_sql output
    """
    return textwrap.dedent(
        """
        SELECT
          "col_int" AS "col_int",
          "col_float" AS "col_float",
          "col_char" AS "col_char",
          "col_text" AS "col_text",
          "col_binary" AS "col_binary",
          "col_boolean" AS "col_boolean",
          "event_timestamp" AS "event_timestamp",
          "created_at" AS "created_at",
          "cust_id" AS "cust_id"
        FROM "sf_database"."sf_schema"."sf_table"
        LIMIT 10
        """
    ).strip()


def pytest_generate_tests(metafunc):
    """
    Parametrize fixtures to use config object or config file specified by global environmental variable
    """
    fixture_names = ["snowflake_database_table", "snowflake_event_data"]
    for fixture_name in fixture_names:
        if fixture_name in metafunc.fixturenames:
            metafunc.parametrize(fixture_name, ["config", "config_from_env"], indirect=True)


@pytest.fixture(name="snowflake_database_table_from_config")
def snowflake_database_table_from_config_fixture(
    snowflake_connector, snowflake_execute_query, snowflake_feature_store
):
    """
    DatabaseTable object fixture (using config object)
    """
    _ = snowflake_connector, snowflake_execute_query
    yield snowflake_feature_store.get_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="sf_table",
    )


@pytest.fixture(name="snowflake_database_table")
def snowflake_database_table_fixture(
    snowflake_database_table_from_config,
    snowflake_feature_store,
    mock_config_path_env,
    request,
):
    """
    DatabaseTable object fixture
    """
    _ = mock_config_path_env
    if request.param == "config":
        yield snowflake_database_table_from_config
    if request.param == "config_from_env":
        yield snowflake_feature_store.get_table(
            database_name="sf_database",
            schema_name="sf_schema",
            table_name="sf_table",
        )


@pytest.fixture(name="snowflake_event_data_from_config")
def snowflake_event_data_from_config_fixture(
    snowflake_database_table_from_config, mock_get_persistent, snowflake_event_data_id
):
    """
    Snowflake EventData object fixture (using config object)
    """
    _ = mock_get_persistent
    yield EventData.from_tabular_source(
        tabular_source=snowflake_database_table_from_config,
        name="sf_event_data",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
        record_creation_date_column="created_at",
        _id=snowflake_event_data_id,
    )


@pytest.fixture(name="snowflake_event_data")
def snowflake_event_data_fixture(
    snowflake_event_data_from_config,
    snowflake_database_table_from_config,
    mock_config_path_env,
    mock_get_persistent,
    request,
    snowflake_event_data_id,
):
    """
    EventData object fixture
    """
    _ = mock_config_path_env, mock_get_persistent
    if request.param == "config":
        yield snowflake_event_data_from_config
    if request.param == "config_from_env":
        yield EventData.from_tabular_source(
            tabular_source=snowflake_database_table_from_config,
            name="sf_event_data",
            event_id_column="col_int",
            event_timestamp_column="event_timestamp",
            record_creation_date_column="created_at",
            _id=snowflake_event_data_id,
        )


@pytest.fixture(name="mock_insert_feature_registry")
def mock_insert_feature_registry_fixture():
    """
    Mock insert feature registry at the controller level
    """
    with patch("featurebyte.service.feature.FeatureService._insert_feature_registry") as mock:
        yield mock
