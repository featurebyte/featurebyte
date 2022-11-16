"""
Common test fixtures used across api test directories
"""
import textwrap
from datetime import datetime

import pytest
from bson.objectid import ObjectId

from featurebyte import EventView
from featurebyte.api.dimension_view import DimensionView
from featurebyte.api.event_data import EventData
from featurebyte.api.item_data import ItemData
from featurebyte.api.item_view import ItemView
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature_store import DataStatus


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


@pytest.fixture()
def expected_item_data_table_preview_query() -> str:
    """
    Expected preview_sql output
    """
    return textwrap.dedent(
        """
        SELECT
          "event_id_col" AS "event_id_col",
          "item_id_col" AS "item_id_col",
          "item_type" AS "item_type",
          "item_amount" AS "item_amount",
          "created_at" AS "created_at"
        FROM "sf_database"."sf_schema"."items_table"
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


@pytest.fixture(name="saved_event_data")
def saved_event_data_fixture(snowflake_feature_store, snowflake_event_data):
    """
    Saved event data fixture
    """
    snowflake_feature_store.save()
    previous_id = snowflake_event_data.id
    assert snowflake_event_data.saved is False
    snowflake_event_data.save()
    assert snowflake_event_data.saved is True
    assert snowflake_event_data.id == previous_id
    assert snowflake_event_data.status == DataStatus.DRAFT
    assert isinstance(snowflake_event_data.created_at, datetime)
    assert isinstance(snowflake_event_data.tabular_source.feature_store_id, ObjectId)

    # test list event data
    assert EventData.list() == ["sf_event_data"]
    yield snowflake_event_data


@pytest.fixture(name="saved_dimension_data")
def saved_dimension_data_fixture(snowflake_feature_store, snowflake_dimension_data):
    """
    Saved dimension data fixture
    """
    snowflake_feature_store.save()
    previous_id = snowflake_dimension_data.id
    assert snowflake_dimension_data.saved is False
    snowflake_dimension_data.save()
    assert snowflake_dimension_data.saved is True
    assert snowflake_dimension_data.id == previous_id
    assert snowflake_dimension_data.status == DataStatus.DRAFT
    assert isinstance(snowflake_dimension_data.created_at, datetime)
    assert isinstance(snowflake_dimension_data.tabular_source.feature_store_id, ObjectId)

    yield snowflake_dimension_data


@pytest.fixture(name="snowflake_database_table_item_data")
def snowflake_database_table_item_data_fixture(
    snowflake_connector, snowflake_execute_query, snowflake_feature_store
):
    """
    DatabaseTable object fixture for ItemData (using config object)
    """
    _ = snowflake_connector, snowflake_execute_query
    yield snowflake_feature_store.get_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="items_table",
    )


@pytest.fixture(name="snowflake_item_data")
def snowflake_item_data_fixture(
    snowflake_database_table_item_data,
    mock_get_persistent,
    snowflake_item_data_id,
    saved_event_data,
):
    """
    Snowflake ItemData object fixture (using config object)
    """
    _ = mock_get_persistent
    yield ItemData.from_tabular_source(
        tabular_source=snowflake_database_table_item_data,
        name="sf_item_data",
        event_id_column="event_id_col",
        item_id_column="item_id_col",
        event_data_name=saved_event_data.name,
        _id=snowflake_item_data_id,
    )


@pytest.fixture(name="snowflake_item_view")
def snowflake_item_view_fixture(snowflake_item_data):
    """
    ItemView fixture
    """
    item_view = ItemView.from_item_data(snowflake_item_data)
    yield item_view


@pytest.fixture(name="snowflake_dimension_view")
def snowflake_dimension_view_fixture(snowflake_dimension_data):
    """
    DimensionView fixture
    """
    dimension_view = DimensionView.from_dimension_data(snowflake_dimension_data)
    yield dimension_view


@pytest.fixture(name="snowflake_event_view")
def snowflake_event_view_fixture(snowflake_event_data, config):
    """
    EventData object fixture
    """
    _ = config
    snowflake_event_data.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(
            blind_spot="1m30s",
            frequency="6m",
            time_modulo_frequency="3m",
        )
    )
    event_view = EventView.from_event_data(event_data=snowflake_event_data)
    yield event_view
