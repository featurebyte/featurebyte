"""
Unit test for ItemData class
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from bson.objectid import ObjectId
from pandas.testing import assert_frame_equal

from featurebyte.api.data import DataColumn
from featurebyte.api.entity import Entity
from featurebyte.api.item_data import ItemData
from featurebyte.enum import TableDataType
from featurebyte.exception import (
    DuplicatedRecordException,
    ObjectHasBeenSavedError,
    RecordCreationException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature_store import DataStatus
from tests.unit.api.base_data_test import BaseDataTestSuite, DataType


@pytest.fixture(name="item_data_dict")
def item_data_dict_fixture(snowflake_database_table_item_data):
    """ItemData in serialized dictionary format"""
    return {
        "columns_info": [
            {"dtype": "INT", "entity_id": None, "name": "event_id_col", "semantic_id": None},
            {"dtype": "VARCHAR", "entity_id": None, "name": "item_id_col", "semantic_id": None},
            {"dtype": "VARCHAR", "entity_id": None, "name": "item_type", "semantic_id": None},
            {"dtype": "FLOAT", "entity_id": None, "name": "item_amount", "semantic_id": None},
            {"dtype": "TIMESTAMP", "entity_id": None, "name": "created_at", "semantic_id": None},
        ],
        "created_at": None,
        "event_data_id": ObjectId("6337f9651050ee7d5980660d"),
        "event_id_column": "event_id_col",
        "id": ObjectId("636a240ec2c2c3f335193e7f"),
        "item_id_column": "item_id_col",
        "name": "sf_item_data",
        "record_creation_date_column": None,
        "status": DataStatus.DRAFT,
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


@pytest.fixture(name="saved_item_data")
def saved_item_data_fixture(snowflake_feature_store, snowflake_item_data):
    """
    Saved ItemData fixture
    """
    previous_id = snowflake_item_data.id
    assert snowflake_item_data.saved is False
    snowflake_item_data.save()
    assert snowflake_item_data.saved is True
    assert snowflake_item_data.id == previous_id
    assert snowflake_item_data.status == DataStatus.DRAFT
    assert isinstance(snowflake_item_data.created_at, datetime)
    assert isinstance(snowflake_item_data.tabular_source.feature_store_id, ObjectId)

    # create entity
    entity = Entity(name="item", serving_names=["item_id"])
    entity.save()

    item_id_col = snowflake_item_data.item_id_col
    assert isinstance(item_id_col, DataColumn)
    snowflake_item_data.item_id_col.as_entity("item")
    assert snowflake_item_data.item_id_col.info.entity_id == entity.id

    # test list event data
    item_data_list = ItemData.list()
    assert_frame_equal(
        item_data_list,
        pd.DataFrame(
            {
                "name": [snowflake_item_data.name],
                "type": [snowflake_item_data.type],
                "status": [snowflake_item_data.status],
                "entities": [["item"]],
                "created_at": [snowflake_item_data.created_at],
            }
        ),
    )

    yield snowflake_item_data


def test_from_tabular_source(snowflake_database_table_item_data, item_data_dict, saved_event_data):
    """
    Test ItemData creation using tabular source
    """
    item_data = ItemData.from_tabular_source(
        tabular_source=snowflake_database_table_item_data,
        name="sf_item_data",
        event_id_column="event_id_col",
        item_id_column="item_id_col",
        event_data_name="sf_event_data",
    )

    # check that node parameter is set properly
    node_params = item_data.node.parameters
    assert node_params.id == item_data.id
    assert node_params.type == TableDataType.ITEM_DATA

    # check that event data columns for autocompletion
    assert set(item_data.columns).issubset(dir(item_data))
    assert item_data._ipython_key_completions_() == set(item_data.columns)

    item_data_dict["id"] = item_data.id
    assert item_data.dict() == item_data_dict

    # user input validation
    with pytest.raises(TypeError) as exc:
        ItemData.from_tabular_source(
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
    Test ItemData creation failure due to duplicated event data name
    """
    _ = saved_item_data
    with pytest.raises(DuplicatedRecordException) as exc:
        ItemData.from_tabular_source(
            tabular_source=snowflake_database_table_item_data,
            name="sf_item_data",
            event_id_column="event_id_col",
            item_id_column="item_id_col",
            event_data_name="sf_event_data",
        )
    assert 'ItemData (item_data.name: "sf_item_data") exists in saved record.' in str(exc.value)


def test_from_tabular_source__retrieval_exception(snowflake_database_table_item_data):
    """
    Test ItemData creation failure due to retrieval exception
    """
    with pytest.raises(RecordRetrievalException):
        with patch("featurebyte.api.data.Configurations"):
            ItemData.from_tabular_source(
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
    Test attempting to create ItemData using old EventData without event_id_column

    Can probably be removed once DEV-556 is resolved
    """
    with patch("featurebyte.api.item_data.EventData") as patched_cls:
        patched_cls.get.return_value = Mock(event_id_column=None)
        with pytest.raises(ValueError) as exc:
            _ = ItemData.from_tabular_source(
                tabular_source=snowflake_database_table_item_data,
                name="sf_item_data",
                event_id_column="event_id_col",
                item_id_column="item_id_col",
                event_data_name="sf_event_data",
            )
        assert str(exc.value) == "EventData without event_id_column is not supported"


def test_deserialization(
    item_data_dict,
    snowflake_feature_store,
    snowflake_execute_query,
    expected_item_data_table_preview_query,
):
    """
    Test deserialize ItemData dictionary
    """
    _ = snowflake_execute_query
    # setup proper configuration to deserialize the event data object
    item_data_dict["feature_store"] = snowflake_feature_store
    item_data = ItemData.parse_obj(item_data_dict)
    assert item_data.preview_sql() == expected_item_data_table_preview_query


@pytest.mark.parametrize("column_name", ["event_id_column", "item_id_column"])
def test_deserialization__column_name_not_found(
    item_data_dict, snowflake_feature_store, snowflake_execute_query, column_name
):
    """
    Test column not found during deserialize ItemData
    """
    _ = snowflake_execute_query
    item_data_dict["feature_store"] = snowflake_feature_store
    item_data_dict[column_name] = "some_random_name"
    with pytest.raises(ValueError) as exc:
        ItemData.parse_obj(item_data_dict)
    assert 'Column "some_random_name" not found in the table!' in str(exc.value)


class TestItemDataTestSuite(BaseDataTestSuite):

    data_type = DataType.ITEM_DATA
    col = "event_id_col"
    expected_columns = {
        "event_id_col",
        "item_id_col",
        "item_type",
        "item_amount",
        "created_at",
    }


def test_item_data_column__as_entity(snowflake_item_data):
    """
    Test setting a column in the ItemData as entity
    """
    # check no column associate with any entity
    assert all([col.entity_id is None for col in snowflake_item_data.columns_info])

    # create entity
    entity = Entity(name="item", serving_names=["item_id"])
    entity.save()

    item_id_col = snowflake_item_data.item_id_col
    assert isinstance(item_id_col, DataColumn)
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
    Test save ItemData failure due to conflict
    """
    # test duplicated record exception when record exists
    with pytest.raises(ObjectHasBeenSavedError) as exc:
        saved_item_data.save()
    expected_msg = f'ItemData (id: "{saved_item_data.id}") has been saved before.'
    assert expected_msg in str(exc.value)


def test_event_data__record_creation_exception(snowflake_item_data):
    """
    Test save ItemData failure due to conflict
    """
    # check unhandled response status code
    with pytest.raises(RecordCreationException):
        with patch("featurebyte.api.api_object.Configurations"):
            snowflake_item_data.save()


def test_update_record_creation_date_column__unsaved_object(snowflake_item_data):
    """Test update record creation date column (unsaved ItemData)"""
    assert snowflake_item_data.record_creation_date_column is None
    snowflake_item_data.update_record_creation_date_column("created_at")
    assert snowflake_item_data.record_creation_date_column == "created_at"


def test_update_record_creation_date_column__saved_object(saved_item_data):
    """Test update record creation date column (saved ItemData)"""
    saved_item_data.update_record_creation_date_column("created_at")
    assert saved_item_data.record_creation_date_column == "created_at"

    # check that validation logic works
    with pytest.raises(RecordUpdateException) as exc:
        saved_item_data.update_record_creation_date_column("random_column_name")
    expected_msg = 'Column "random_column_name" not found in the table! (type=value_error)'
    assert expected_msg in str(exc.value)

    with pytest.raises(RecordUpdateException) as exc:
        saved_item_data.update_record_creation_date_column("item_id_col")
    expected_msg = (
        "Column \"item_id_col\" is expected to have type(s): ['TIMESTAMP'] (type=value_error)"
    )
    assert expected_msg in str(exc.value)


def test_get_item_data(saved_item_data, snowflake_item_data):
    """
    Test ItemData.get function
    """

    # load the event data from the persistent
    loaded_data = ItemData.get(saved_item_data.name)
    assert loaded_data.saved is True
    assert loaded_data == snowflake_item_data
    assert ItemData.get_by_id(id=loaded_data.id) == snowflake_item_data

    with pytest.raises(RecordRetrievalException) as exc:
        lazy_event_data = ItemData.get("unknown_event_data")
        _ = lazy_event_data.name
    expected_msg = (
        'ItemData (name: "unknown_event_data") not found. ' "Please save the ItemData object first."
    )
    assert expected_msg in str(exc.value)


def test_inherit_default_feature_job_setting(
    snowflake_database_table_item_data, item_data_dict, saved_event_data
):
    """
    Test ItemData inherits the same default feature job setting from EventData
    """
    feature_job_setting = FeatureJobSetting(
        blind_spot="1m30s",
        frequency="10m",
        time_modulo_frequency="2m",
    )
    saved_event_data.update_default_feature_job_setting(feature_job_setting=feature_job_setting)
    item_data = ItemData.from_tabular_source(
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
    feature_list = ItemData.list(entity="item")
    assert feature_list.shape[0] == 1

    feature_list = ItemData.list(entity="other_entity")
    assert feature_list.shape[0] == 0
