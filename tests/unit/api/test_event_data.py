"""
Unit test for EventData class
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

import pytest
from bson.objectid import ObjectId
from pydantic.error_wrappers import ValidationError

from featurebyte.api.entity import Entity
from featurebyte.api.event_data import EventData, EventDataColumn
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.models.event_data import EventDataStatus, FeatureJobSetting


@pytest.fixture(name="event_data_dict")
def event_data_dict_fixture(snowflake_database_table):
    """
    EventData in serialized dictionary format
    """
    return {
        "name": "sf_event_data",
        "tabular_source": (
            snowflake_database_table.feature_store.id,
            {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
        ),
        "event_timestamp_column": "event_timestamp",
        "record_creation_date_column": "created_at",
        "column_entity_map": None,
        "default_feature_job_setting": None,
        "created_at": None,
        "updated_at": None,
        "user_id": None,
        "history": [],
        "status": None,
    }


@pytest.fixture(name="saved_event_data")
def saved_event_data_fixture(snowflake_feature_store, snowflake_event_data):
    """
    Saved event data fixture
    """
    snowflake_feature_store.save()
    previous_id = snowflake_event_data.id
    snowflake_event_data.save()
    assert snowflake_event_data.id == previous_id
    assert snowflake_event_data.status == EventDataStatus.DRAFT
    assert isinstance(snowflake_event_data.created_at, datetime)
    feature_store, _ = snowflake_event_data.tabular_source
    assert isinstance(feature_store, ObjectId)

    # test list event data
    assert EventData.list() == ["sf_event_data"]
    yield snowflake_event_data


def test_from_tabular_source(snowflake_database_table, config, event_data_dict):
    """
    Test EventData creation using tabular source
    """
    event_data = EventData.from_tabular_source(
        tabular_source=snowflake_database_table,
        name="sf_event_data",
        event_timestamp_column="event_timestamp",
        record_creation_date_column="created_at",
        credentials=config.credentials,
    )
    event_data_dict["id"] = event_data.id
    assert event_data.dict() == event_data_dict

    # user input validation
    with pytest.raises(ValidationError) as exc:
        EventData.from_tabular_source(
            tabular_source=snowflake_database_table,
            name=123,
            event_timestamp_column=234,
            record_creation_date_column=345,
            credentials=config.credentials,
        )

    assert exc.value.errors() == [
        {"loc": ("name",), "msg": "str type expected", "type": "type_error.str"},
        {"loc": ("event_timestamp_column",), "msg": "str type expected", "type": "type_error.str"},
        {
            "loc": ("record_creation_date_column",),
            "msg": "str type expected",
            "type": "type_error.str",
        },
    ]


def test_from_tabular_source__duplicated_record(saved_event_data, snowflake_database_table, config):
    """
    Test EventData creation failure due to duplicated event data name
    """
    _ = saved_event_data
    with pytest.raises(DuplicatedRecordException) as exc:
        EventData.from_tabular_source(
            tabular_source=snowflake_database_table,
            name="sf_event_data",
            event_timestamp_column="event_timestamp",
            record_creation_date_column="created_at",
            credentials=config.credentials,
        )
    assert 'EventData (event_data.name: "sf_event_data") exists in saved record.' in str(exc.value)


def test_from_tabular_source__retrieval_exception(snowflake_database_table, config):
    """
    Test EventData creation failure due to retrieval exception
    """
    with pytest.raises(RecordRetrievalException):
        with patch("featurebyte.api.event_data.Configurations"):
            EventData.from_tabular_source(
                tabular_source=snowflake_database_table,
                name="sf_event_data",
                event_timestamp_column="event_timestamp",
                record_creation_date_column="created_at",
                credentials=config.credentials,
            )


def test_deserialization(
    event_data_dict,
    config,
    snowflake_feature_store,
    snowflake_execute_query,
    expected_snowflake_table_preview_query,
):
    """
    Test deserialize event data dictionary
    """
    _ = snowflake_execute_query
    # setup proper configuration to deserialize the event data object
    event_data_dict["credentials"] = config.credentials
    event_data_dict["feature_store"] = snowflake_feature_store
    event_data = EventData.parse_obj(event_data_dict)
    assert event_data.preview_sql() == expected_snowflake_table_preview_query


def test_deserialization__column_name_not_found(
    event_data_dict, config, snowflake_feature_store, snowflake_execute_query
):
    """
    Test column not found during deserialize event data
    """
    _ = snowflake_execute_query
    event_data_dict["credentials"] = config.credentials
    event_data_dict["feature_store"] = snowflake_feature_store
    event_data_dict["record_creation_date_column"] = "some_random_name"
    with pytest.raises(ValueError) as exc:
        EventData.parse_obj(event_data_dict)
    assert 'Column "some_random_name" not found in the table!' in str(exc.value)

    event_data_dict["record_created_date_column"] = "created_at"
    event_data_dict["event_timestamp_column"] = "some_timestamp_column"
    with pytest.raises(ValueError) as exc:
        EventData.parse_obj(event_data_dict)
    assert 'Column "some_timestamp_column" not found in the table!' in str(exc.value)


def test_event_data_column__not_exists(snowflake_event_data):
    """
    Test non-exist column retrieval
    """
    expected = 'Column "non_exist_column" does not exist!'
    with pytest.raises(KeyError) as exc:
        _ = snowflake_event_data["non_exist_column"]
    assert expected in str(exc.value)

    with pytest.raises(KeyError) as exc:
        _ = snowflake_event_data.non_exist_column
    assert expected in str(exc.value)


def test_event_data_column__as_entity(snowflake_event_data):
    """
    Test setting a column in the event data as entity
    """
    assert snowflake_event_data.column_entity_map is None

    # create entity
    entity = Entity(name="customer", serving_names=["cust_id"])
    entity.save()

    col_int = snowflake_event_data.col_int
    assert isinstance(col_int, EventDataColumn)
    snowflake_event_data.col_int.as_entity("customer")
    assert snowflake_event_data.column_entity_map == {"col_int": entity.id}

    with pytest.raises(TypeError) as exc:
        snowflake_event_data.col_int.as_entity(1234)
    assert 'Unsupported type "<class \'int\'>" for tag name "1234"!' in str(exc.value)

    with pytest.raises(RecordRetrievalException) as exc:
        snowflake_event_data.col_int.as_entity("some_random_entity")
    assert 'Entity name "some_random_entity" not found!' in str(exc.value)

    # remove entity association
    snowflake_event_data.col_int.as_entity(None)
    assert snowflake_event_data.column_entity_map == {}


def test_event_data_column__as_entity__saved_event_data(saved_event_data, config):
    """
    Test setting a column in the event data as entity (saved event data)
    """
    assert saved_event_data.column_entity_map is None

    # create entity
    entity = Entity(name="customer", serving_names=["cust_id"])
    entity.save()

    saved_event_data.col_int.as_entity("customer")
    assert saved_event_data.column_entity_map == {"col_int": entity.id}

    # check that the column entity map is saved to persistent
    client = config.get_client()
    response = client.get(url=f"/event_data/{saved_event_data.id}")
    response_dict = response.json()
    assert response_dict["column_entity_map"] == {"col_int": str(entity.id)}


def test_event_data_column__as_entity__saved_event_data__record_update_exception(
    saved_event_data, config
):
    """
    Test setting a column in the event data as entity (record update exception)
    """
    _ = config

    entity = Entity(name="customer", serving_names=["cust_id"])
    entity.save()

    # test unexpected exception
    with pytest.raises(RecordUpdateException):
        with patch("featurebyte.api.event_data.Configurations"):
            saved_event_data.col_int.as_entity("customer")


def test_event_data__save__feature_store_not_saved_exception(snowflake_event_data):
    """
    Test save event data failure due to feature store object not saved
    """

    with pytest.raises(RecordCreationException) as exc:
        snowflake_event_data.save()
    feature_store_id = snowflake_event_data.feature_store.id
    expect_msg = (
        f'FeatureStore (id: "{feature_store_id}") not found. '
        f"Please save the FeatureStore object first."
    )
    assert expect_msg in str(exc.value)


def test_event_data__save__exceptions(saved_event_data):
    """
    Test save event data failure due to conflict
    """
    # test duplicated record exception when record exists
    with pytest.raises(DuplicatedRecordException) as exc:
        saved_event_data.save()
    assert exc.value.status_code == 409
    expected_msg = (
        f'EventData (id: "{saved_event_data.id}") already exists. '
        'Get the existing object by `EventData.get(name="sf_event_data")`.'
    )
    assert exc.value.response.json()["detail"] == expected_msg

    # check unhandled response status code
    with pytest.raises(RecordCreationException):
        with patch("featurebyte.api.api_object.Configurations"):
            saved_event_data.save()


def test_event_data__info__not_saved_event_data(snowflake_feature_store, snowflake_event_data):
    """
    Test info on not-saved event data
    """
    snowflake_event_data.event_timestamp_column = "col_int"
    snowflake_event_data.record_creation_date_column = "col_text"
    output = snowflake_event_data.info()
    assert output == {
        "id": snowflake_event_data.id,
        "user_id": None,
        "name": "sf_event_data",
        "record_creation_date_column": "col_text",
        "column_entity_map": None,
        "created_at": None,
        "updated_at": None,
        "default_feature_job_setting": None,
        "event_timestamp_column": "col_int",
        "history": [],
        "status": None,
        "tabular_source": (
            snowflake_feature_store.id,
            {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
        ),
    }
    feature_store_id, _ = snowflake_event_data.tabular_source
    assert isinstance(feature_store_id, ObjectId)

    # check unhandled response status code
    with pytest.raises(RecordRetrievalException):
        with patch("featurebyte.api.event_data.Configurations"):
            snowflake_event_data.info()


def test_event_data__info__saved_event_data(snowflake_feature_store, saved_event_data):
    """
    Test info on saved event data
    """
    saved_event_data_dict = saved_event_data.dict()

    # perform some modifications
    saved_event_data.event_timestamp_column = "col_int"
    saved_event_data.record_creation_date_column = "col_text"
    assert (
        saved_event_data.event_timestamp_column != saved_event_data_dict["event_timestamp_column"]
    )
    assert (
        saved_event_data.record_creation_date_column
        != saved_event_data_dict["record_creation_date_column"]
    )

    # call info & check that all those modifications gone
    output = saved_event_data.info()
    assert (
        output
        == saved_event_data_dict
        == {
            "id": saved_event_data.id,
            "user_id": None,
            "name": "sf_event_data",
            "record_creation_date_column": "created_at",
            "column_entity_map": None,
            "created_at": saved_event_data.created_at,
            "updated_at": None,
            "default_feature_job_setting": None,
            "event_timestamp_column": "event_timestamp",
            "history": [],
            "status": "DRAFT",
            "tabular_source": (
                snowflake_feature_store.id,
                {
                    "database_name": "sf_database",
                    "schema_name": "sf_schema",
                    "table_name": "sf_table",
                },
            ),
        }
    )
    feature_store_id, _ = saved_event_data.tabular_source
    assert isinstance(feature_store_id, ObjectId)


def test_update_default_job_setting(snowflake_event_data, config):
    """
    Test update default job setting on non-saved event data
    """

    # make sure the event data is not saved
    client = config.get_client()
    response = client.get(url=f"/event_data/{snowflake_event_data.id}")
    assert response.status_code == 404

    assert snowflake_event_data.default_feature_job_setting is None
    snowflake_event_data.update_default_feature_job_setting(
        blind_spot="1m30s",
        frequency="10m",
        time_modulo_frequency="2m",
    )
    assert snowflake_event_data.default_feature_job_setting == FeatureJobSetting(
        blind_spot="1m30s", frequency="10m", time_modulo_frequency="2m"
    )
    feature_store_id, _ = snowflake_event_data.tabular_source
    assert isinstance(feature_store_id, ObjectId)


def test_update_default_job_setting__saved_event_data(saved_event_data, config):
    """
    Test update default job setting on saved event data
    """
    assert saved_event_data.default_feature_job_setting is None
    saved_event_data.update_default_feature_job_setting(
        blind_spot="1m30s",
        frequency="6m",
        time_modulo_frequency="3m",
    )

    # check updated feature job settings stored at the persistent & memory
    assert saved_event_data.default_feature_job_setting == FeatureJobSetting(
        blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
    )
    client = config.get_client()
    response = client.get(url=f"/event_data/{saved_event_data.id}")
    assert response.status_code == 200
    assert response.json()["default_feature_job_setting"] == {
        "blind_spot": "1m30s",
        "frequency": "6m",
        "time_modulo_frequency": "3m",
    }


def test_update_default_job_setting__record_update_exception(snowflake_event_data):
    """
    Test unexpected exception during record update
    """
    with pytest.raises(RecordUpdateException):
        with patch("featurebyte.api.event_data.Configurations"):
            snowflake_event_data.update_default_feature_job_setting(
                blind_spot="1m",
                frequency="2m",
                time_modulo_frequency="1m",
            )


def test_get_event_data(snowflake_feature_store, snowflake_event_data, mock_config_path_env):
    """
    Test EventData.get function
    """
    _ = mock_config_path_env

    # create event data & save to persistent
    snowflake_feature_store.save()
    snowflake_event_data.save()

    # load the event data from the persistent
    loaded_event_data = EventData.get(snowflake_event_data.name)
    assert loaded_event_data == snowflake_event_data
    assert EventData.get_by_id(id=snowflake_event_data.id) == snowflake_event_data

    with pytest.raises(RecordRetrievalException) as exc:
        EventData.get("unknown_event_data")
    expected_msg = (
        'EventData (name: "unknown_event_data") not found. '
        "Please save the EventData object first."
    )
    assert expected_msg in str(exc.value)
