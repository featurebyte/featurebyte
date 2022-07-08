"""
Unit test for EventData class
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

import pytest

from featurebyte.api.entity import Entity
from featurebyte.api.event_data import EventData, EventDataColumn
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordRetrievalException,
)
from featurebyte.models.event_data import EventDataStatus


@pytest.fixture(name="event_data_dict")
def event_data_dict_fixture():
    """
    EventData in serialized dictionary format
    """
    return {
        "name": "sf_event_data",
        "tabular_source": (
            {
                "type": "snowflake",
                "details": {
                    "account": "sf_account",
                    "database": "sf_database",
                    "sf_schema": "sf_schema",
                    "warehouse": "sf_warehouse",
                },
            },
            {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
        ),
        "event_timestamp_column": "event_timestamp",
        "record_creation_date_column": "created_at",
        "column_entity_map": {},
        "default_feature_job_setting": None,
        "created_at": None,
        "history": [],
        "status": None,
    }


@pytest.fixture(name="mock_get_persistent")
def mock_get_persistent_function(git_persistent):
    """
    Mock GitDB in featurebyte.app
    """
    with patch("featurebyte.app._get_persistent") as mock_persistent:
        persistent, _ = git_persistent
        mock_persistent.return_value = persistent
        yield mock_persistent


@pytest.fixture(name="saved_event_data")
def save_event_data_fixture(mock_get_persistent, snowflake_event_data):
    """
    Saved event data fixture
    """
    _ = mock_get_persistent
    snowflake_event_data.save_as_draft()
    assert snowflake_event_data.status == EventDataStatus.DRAFT
    assert isinstance(snowflake_event_data.created_at, datetime)
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
    assert event_data.dict() == event_data_dict


def test_deserialization(
    event_data_dict,
    config,
    snowflake_execute_query,
    expected_snowflake_table_preview_query,
):
    """
    Test deserialize event data dictionary
    """
    _ = snowflake_execute_query
    # setup proper configuration to deserialize the event data object
    event_data_dict["credentials"] = config.credentials
    event_data = EventData.parse_obj(event_data_dict)
    assert event_data.preview_sql() == expected_snowflake_table_preview_query


def test_deserialization__column_name_not_found(event_data_dict, config, snowflake_execute_query):
    """
    Test column not found during deserialize event data
    """
    _ = snowflake_execute_query
    event_data_dict["credentials"] = config.credentials
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


def test_event_data_column__as_entity(snowflake_event_data, mock_get_persistent):
    """
    Test setting a column in the event data as entity
    """
    _ = mock_get_persistent

    # create entity
    entity = Entity(name="customer", serving_name="cust_id")

    col_int = snowflake_event_data.col_int
    assert isinstance(col_int, EventDataColumn)
    snowflake_event_data.col_int.as_entity("customer")
    assert snowflake_event_data.column_entity_map["col_int"] == str(entity.id)

    with pytest.raises(TypeError) as exc:
        snowflake_event_data.col_int.as_entity(1234)
    assert 'Unsupported type "<class \'int\'>" for tag name "1234"!' in str(exc.value)

    with pytest.raises(ValueError) as exc:
        snowflake_event_data.col_int.as_entity("some_random_entity")
    assert 'Entity name "some_random_entity" not found!' in str(exc.value)

    # remove entity association
    snowflake_event_data.col_int.as_entity(None)
    assert snowflake_event_data.column_entity_map == {}


def test_event_data__save_as_draft__exceptions(saved_event_data):
    """
    Test save event data object to persistent layer
    """
    # test duplicated record exception when record exists
    with pytest.raises(DuplicatedRecordException) as exc:
        saved_event_data.save_as_draft()
    assert exc.value.status_code == 409
    assert exc.value.response.json()["detail"] == 'Event Data "sf_event_data" already exists.'

    # check unhandled response status code
    with pytest.raises(RecordCreationException):
        with patch("featurebyte.api.event_data.Configurations"):
            saved_event_data.save_as_draft()


def test_event_data__info__not_saved_event_data(mock_get_persistent, snowflake_event_data):
    """
    Test info on not-saved event data
    """
    _ = mock_get_persistent
    snowflake_event_data.event_timestamp_column = "some_event_timestamp"
    snowflake_event_data.record_creation_date_column = "some_random_date"
    output = snowflake_event_data.info()
    assert output == {
        "name": "sf_event_data",
        "record_creation_date_column": "some_random_date",
        "column_entity_map": {},
        "created_at": None,
        "default_feature_job_setting": None,
        "event_timestamp_column": "some_event_timestamp",
        "history": [],
        "status": None,
        "tabular_source": (
            {
                "type": "snowflake",
                "details": {
                    "account": "sf_account",
                    "database": "sf_database",
                    "sf_schema": "sf_schema",
                    "warehouse": "sf_warehouse",
                },
            },
            {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
        ),
    }

    # check unhandled response status code
    with pytest.raises(RecordRetrievalException):
        with patch("featurebyte.api.event_data.Configurations"):
            snowflake_event_data.info()


def test_event_data__info__saved_event_data(saved_event_data, mock_config_path_env):
    """
    Test info on saved event data
    """
    _ = mock_config_path_env
    saved_event_data_dict = saved_event_data.dict()

    # perform some modifications
    saved_event_data.event_timestamp_column = "some_event_timestamp"
    saved_event_data.record_creation_date_column = "some_random_date"
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
            "name": "sf_event_data",
            "record_creation_date_column": "created_at",
            "column_entity_map": {},
            "created_at": saved_event_data.created_at,
            "default_feature_job_setting": None,
            "event_timestamp_column": "event_timestamp",
            "history": [],
            "status": "DRAFT",
            "tabular_source": (
                {
                    "type": "snowflake",
                    "details": {
                        "account": "sf_account",
                        "database": "sf_database",
                        "sf_schema": "sf_schema",
                        "warehouse": "sf_warehouse",
                    },
                },
                {
                    "database_name": "sf_database",
                    "schema_name": "sf_schema",
                    "table_name": "sf_table",
                },
            ),
        }
    )
