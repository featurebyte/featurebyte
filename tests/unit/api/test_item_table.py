"""
Unit test for ItemTable class
"""
from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
from bson.objectid import ObjectId

from featurebyte.api.base_table import TableColumn
from featurebyte.api.entity import Entity
from featurebyte.api.item_table import ItemTable
from featurebyte.enum import TableDataType
from featurebyte.exception import (
    DuplicatedRecordException,
    ObjectHasBeenSavedError,
    RecordCreationException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.models.item_data import ItemDataModel
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from tests.unit.api.base_data_test import BaseTableTestSuite, DataType
from tests.util.helper import check_sdk_code_generation


@pytest.fixture(name="item_data_dict")
def item_data_dict_fixture(snowflake_database_table_item_data):
    """ItemTable in serialized dictionary format"""
    return {
        "columns_info": [
            {
                "dtype": "INT",
                "entity_id": None,
                "name": "event_id_col",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "dtype": "VARCHAR",
                "entity_id": None,
                "name": "item_id_col",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "dtype": "VARCHAR",
                "entity_id": None,
                "name": "item_type",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "dtype": "FLOAT",
                "entity_id": None,
                "name": "item_amount",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "dtype": "TIMESTAMP_TZ",
                "entity_id": None,
                "name": "created_at",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "dtype": "TIMESTAMP_TZ",
                "entity_id": None,
                "name": "event_timestamp",
                "semantic_id": None,
                "critical_data_info": None,
            },
        ],
        "created_at": None,
        "event_data_id": ObjectId("6337f9651050ee7d5980660d"),
        "event_id_column": "event_id_col",
        "_id": ObjectId("636a240ec2c2c3f335193e7f"),
        "item_id_column": "item_id_col",
        "name": "sf_item_data",
        "record_creation_timestamp_column": None,
        "tabular_source": {
            "feature_store_id": snowflake_database_table_item_data.tabular_source.feature_store_id,
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "items_table",
            },
        },
        "type": "item_data",
        "updated_at": None,
        "user_id": None,
    }


def test_from_tabular_source(snowflake_database_table_item_data, item_data_dict, saved_event_data):
    """
    Test ItemTable creation using tabular source
    """
    item_data = ItemTable.from_tabular_source(
        tabular_source=snowflake_database_table_item_data,
        name="sf_item_data",
        event_id_column="event_id_col",
        item_id_column="item_id_col",
        event_data_name="sf_event_data",
    )

    # check that node parameter is set properly
    node_params = item_data.frame.node.parameters
    assert node_params.id == item_data.id
    assert node_params.type == TableDataType.ITEM_DATA

    # check that event data columns for autocompletion
    assert set(item_data.columns).issubset(dir(item_data))
    assert item_data._ipython_key_completions_() == set(item_data.columns)

    output = item_data.dict(by_alias=True)
    item_data_dict["_id"] = item_data.id
    assert output == item_data_dict

    # user input validation
    with pytest.raises(TypeError) as exc:
        ItemTable.from_tabular_source(
            tabular_source=snowflake_database_table_item_data,
            name=123,
            event_id_column="event_id_col",
            item_id_column="item_id_col",
            event_data_name="sf_event_data",
        )
    assert 'type of argument "name" must be str; got int instead' in str(exc.value)


def test_from_tabular_source__duplicated_record(
    saved_item_data, snowflake_database_table_item_data
):
    """
    Test ItemTable creation failure due to duplicated event data name
    """
    _ = saved_item_data
    with pytest.raises(DuplicatedRecordException) as exc:
        ItemTable.from_tabular_source(
            tabular_source=snowflake_database_table_item_data,
            name="sf_item_data",
            event_id_column="event_id_col",
            item_id_column="item_id_col",
            event_data_name="sf_event_data",
        )
    assert 'ItemTable (item_data.name: "sf_item_data") exists in saved record.' in str(exc.value)


def test_from_tabular_source__retrieval_exception(snowflake_database_table_item_data):
    """
    Test ItemTable creation failure due to retrieval exception
    """
    with pytest.raises(RecordRetrievalException):
        with patch("featurebyte.api.base_table.Configurations"):
            ItemTable.from_tabular_source(
                tabular_source=snowflake_database_table_item_data,
                name="sf_item_data",
                event_id_column="event_id_col",
                item_id_column="item_id_col",
                event_data_name="sf_event_data",
            )


def test_from_tabular_source__event_data_without_event_id_column(
    snowflake_database_table_item_data,
):
    """
    Test attempting to create ItemTable using old EventData without event_id_column

    Can probably be removed once DEV-556 is resolved
    """
    with patch("featurebyte.api.item_table.EventTable") as patched_cls:
        patched_cls.get.return_value = Mock(event_id_column=None)
        with pytest.raises(ValueError) as exc:
            _ = ItemTable.from_tabular_source(
                tabular_source=snowflake_database_table_item_data,
                name="sf_item_data",
                event_id_column="event_id_col",
                item_id_column="item_id_col",
                event_data_name="sf_event_data",
            )
        assert str(exc.value) == "EventTable without event_id_column is not supported"


def test_deserialization(
    item_data_dict,
    snowflake_feature_store,
    snowflake_execute_query,
    expected_item_data_table_preview_query,
):
    """
    Test deserialize ItemTable dictionary
    """
    _ = snowflake_execute_query
    # setup proper configuration to deserialize the event data object
    item_data_dict["feature_store"] = snowflake_feature_store
    item_data = ItemTable.parse_obj(item_data_dict)
    assert item_data.preview_sql() == expected_item_data_table_preview_query


@pytest.mark.parametrize("column_name", ["event_id_column", "item_id_column"])
def test_deserialization__column_name_not_found(
    item_data_dict, snowflake_feature_store, snowflake_execute_query, column_name
):
    """
    Test column not found during deserialize ItemTable
    """
    _ = snowflake_execute_query
    item_data_dict["feature_store"] = snowflake_feature_store
    item_data_dict[column_name] = "some_random_name"
    with pytest.raises(ValueError) as exc:
        ItemTable.parse_obj(item_data_dict)
    assert 'Column "some_random_name" not found in the table!' in str(exc.value)


class TestItemTableTestSuite(BaseTableTestSuite):

    data_type = DataType.ITEM_DATA
    col = "event_id_col"
    expected_columns = {
        "event_id_col",
        "item_id_col",
        "item_type",
        "item_amount",
        "created_at",
        "event_timestamp",
    }
    expected_data_sql = """
    SELECT
      "event_id_col" AS "event_id_col",
      "item_id_col" AS "item_id_col",
      "item_type" AS "item_type",
      "item_amount" AS "item_amount",
      CAST("created_at" AS STRING) AS "created_at",
      CAST("event_timestamp" AS STRING) AS "event_timestamp"
    FROM "sf_database"."sf_schema"."items_table"
    LIMIT 10
    """
    expected_data_column_sql = """
    SELECT
      "event_id_col" AS "event_id_col"
    FROM "sf_database"."sf_schema"."items_table"
    LIMIT 10
    """
    expected_clean_data_sql = """
    SELECT
      CAST(CASE WHEN (
        "event_id_col" IS NULL
      ) THEN 0 ELSE "event_id_col" END AS BIGINT) AS "event_id_col",
      "item_id_col" AS "item_id_col",
      "item_type" AS "item_type",
      "item_amount" AS "item_amount",
      CAST("created_at" AS STRING) AS "created_at",
      CAST("event_timestamp" AS STRING) AS "event_timestamp"
    FROM "sf_database"."sf_schema"."items_table"
    LIMIT 10
    """


def test_item_data_column__as_entity(snowflake_item_data, mock_api_object_cache):
    """
    Test setting a column in the ItemTable as entity
    """
    _ = mock_api_object_cache

    # check no column associate with any entity
    assert all([col.entity_id is None for col in snowflake_item_data.columns_info])

    # create entity
    entity = Entity(name="item", serving_names=["item_id"])
    entity.save()

    item_id_col = snowflake_item_data.item_id_col
    assert isinstance(item_id_col, TableColumn)
    snowflake_item_data.item_id_col.as_entity("item")
    assert snowflake_item_data.item_id_col.info.entity_id == entity.id

    with pytest.raises(TypeError) as exc:
        snowflake_item_data.item_id_col.as_entity(1234)
    assert 'type of argument "entity_name" must be one of (str, NoneType); got int instead' in str(
        exc.value
    )

    with pytest.raises(RecordRetrievalException) as exc:
        snowflake_item_data.item_id_col.as_entity("some_random_entity")
    expected_msg = (
        'Entity (name: "some_random_entity") not found. Please save the Entity object first.'
    )
    assert expected_msg in str(exc.value)

    # remove entity association
    snowflake_item_data.item_id_col.as_entity(None)
    assert snowflake_item_data.item_id_col.info.entity_id is None


def test_item_data__save__exceptions(saved_item_data):
    """
    Test save ItemTable failure due to conflict
    """
    # test duplicated record exception when record exists
    with pytest.raises(ObjectHasBeenSavedError) as exc:
        saved_item_data.save()
    expected_msg = f'ItemTable (id: "{saved_item_data.id}") has been saved before.'
    assert expected_msg in str(exc.value)


def test_event_data__record_creation_exception(snowflake_item_data):
    """
    Test save ItemTable failure due to conflict
    """
    # check unhandled response status code
    with pytest.raises(RecordCreationException):
        with patch("featurebyte.api.api_object.Configurations"):
            snowflake_item_data.save()


def test_update_record_creation_timestamp_column__unsaved_object(
    snowflake_item_data, mock_api_object_cache
):
    """Test update record creation date column (unsaved ItemTable)"""
    _ = mock_api_object_cache
    assert snowflake_item_data.record_creation_timestamp_column is None
    snowflake_item_data.update_record_creation_timestamp_column("created_at")
    assert snowflake_item_data.record_creation_timestamp_column == "created_at"


def test_update_record_creation_timestamp_column__saved_object(saved_item_data):
    """Test update record creation date column (saved ItemTable)"""
    saved_item_data.update_record_creation_timestamp_column("created_at")
    assert saved_item_data.record_creation_timestamp_column == "created_at"

    # check that validation logic works
    with pytest.raises(RecordUpdateException) as exc:
        saved_item_data.update_record_creation_timestamp_column("random_column_name")
    expected_msg = 'Column "random_column_name" not found in the table! (type=value_error)'
    assert expected_msg in str(exc.value)

    with pytest.raises(RecordUpdateException) as exc:
        saved_item_data.update_record_creation_timestamp_column("item_id_col")
    expected_msg = (
        'Column "item_id_col" is expected to have type(s): '
        "['TIMESTAMP', 'TIMESTAMP_TZ'] (type=value_error)"
    )
    assert expected_msg in str(exc.value)


def test_get_item_data(saved_item_data, snowflake_item_data):
    """
    Test ItemTable.get function
    """

    # load the event data from the persistent
    loaded_data = ItemTable.get(saved_item_data.name)
    assert loaded_data.saved is True
    assert loaded_data == snowflake_item_data
    assert ItemTable.get_by_id(id=loaded_data.id) == snowflake_item_data

    with pytest.raises(RecordRetrievalException) as exc:
        ItemTable.get("unknown_item_data")

    expected_msg = (
        'ItemTable (name: "unknown_item_data") not found. '
        "Please save the ItemTable object first."
    )
    assert expected_msg in str(exc.value)


def test_inherit_default_feature_job_setting(
    snowflake_database_table_item_data, item_data_dict, saved_event_data
):
    """
    Test ItemTable inherits the same default feature job setting from EventData
    """
    feature_job_setting = FeatureJobSetting(
        blind_spot="1m30s",
        frequency="10m",
        time_modulo_frequency="2m",
    )
    saved_event_data.update_default_feature_job_setting(feature_job_setting=feature_job_setting)
    item_data = ItemTable.from_tabular_source(
        tabular_source=snowflake_database_table_item_data,
        name="sf_item_data",
        event_id_column="event_id_col",
        item_id_column="item_id_col",
        event_data_name="sf_event_data",
    )
    assert item_data.default_feature_job_setting == feature_job_setting


def test_list_filter(saved_item_data):
    """Test filters in list"""
    # test filter by entity
    feature_list = ItemTable.list(entity="item")
    assert feature_list.shape[0] == 1

    feature_list = ItemTable.list(entity="other_entity", include_id=True)
    assert feature_list.shape[0] == 0


def assert_info_helper(item_data_info):
    """
    Helper function to assert info from item data.
    """
    assert item_data_info["event_id_column"] == "event_id_col"
    assert item_data_info["item_id_column"] == "item_id_col"
    assert len(item_data_info["entities"]) == 1
    assert item_data_info["name"] == "sf_item_data"
    assert item_data_info["status"] == "DRAFT"


def test_info(saved_item_data):
    """
    Test info
    """
    info = saved_item_data.info()
    assert_info_helper(info)

    # setting verbose = true is a no-op for now
    info = saved_item_data.info(verbose=True)
    assert_info_helper(info)


def test_accessing_item_data_attributes(snowflake_item_data):
    """Test accessing event data object attributes"""
    assert snowflake_item_data.saved is False
    assert snowflake_item_data.record_creation_timestamp_column is None
    assert snowflake_item_data.event_id_column == "event_id_col"
    assert snowflake_item_data.item_id_column == "item_id_col"


def test_accessing_saved_item_data_attributes(saved_item_data):
    """Test accessing event data object attributes"""
    assert saved_item_data.saved
    assert isinstance(saved_item_data.cached_model, ItemDataModel)
    assert saved_item_data.record_creation_timestamp_column is None
    assert saved_item_data.event_id_column == "event_id_col"
    assert saved_item_data.item_id_column == "item_id_col"

    # check synchronization
    entity = Entity(name="item_type", serving_names=["item_type"])
    entity.save()
    cloned = ItemTable.get_by_id(id=saved_item_data.id)
    assert cloned["item_type"].info.entity_id is None
    saved_item_data["item_type"].as_entity(entity.name)
    assert saved_item_data["item_type"].info.entity_id == entity.id
    assert cloned["item_type"].info.entity_id == entity.id

    # check table_data property
    assert saved_item_data.item_id_col.info.entity_id is not None
    saved_item_data.item_id_col.as_entity(None)
    assert cloned.item_id_col.info.entity_id is None
    assert cloned.table_data.columns_info == saved_item_data.columns_info


def test_sdk_code_generation(snowflake_database_table_item_data, saved_event_data, update_fixtures):
    """Check SDK code generation for unsaved data"""
    item_data = ItemTable.from_tabular_source(
        tabular_source=snowflake_database_table_item_data,
        name="sf_item_data",
        event_id_column="event_id_col",
        item_id_column="item_id_col",
        event_data_name="sf_event_data",
    )
    check_sdk_code_generation(
        item_data.frame,
        to_use_saved_data=False,
        fixture_path="tests/fixtures/sdk_code/item_table.py",
        update_fixtures=update_fixtures,
        data_id=item_data.id,
        event_data_id=saved_event_data.id,
    )


def test_sdk_code_generation_on_saved_data(saved_item_data, update_fixtures):
    """Check SDK code generation for saved data"""
    check_sdk_code_generation(
        saved_item_data.frame,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/saved_item_table.py",
        update_fixtures=update_fixtures,
        data_id=saved_item_data.id,
    )
