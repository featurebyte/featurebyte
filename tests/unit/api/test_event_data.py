"""
Unit test for EventData class
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest
from bson.objectid import ObjectId
from pydantic import ValidationError

from featurebyte.api.entity import Entity
from featurebyte.api.event_data import EventData, EventDataColumn
from featurebyte.api.feature_store import FeatureStore
from featurebyte.exception import (
    DuplicatedRecordException,
    ObjectHasBeenSavedError,
    RecordCreationException,
    RecordRetrievalException,
    RecordUpdateException,
    TableSchemaHasBeenChangedError,
)
from featurebyte.models.event_data import EventDataStatus, FeatureJobSetting
from tests.util.helper import patch_import_package


@pytest.fixture(name="event_data_dict")
def event_data_dict_fixture(snowflake_database_table):
    """EventData in serialized dictionary format"""
    return {
        "name": "sf_event_data",
        "tabular_source": {
            "feature_store_id": snowflake_database_table.feature_store.id,
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
        },
        "columns_info": [
            {"entity_id": None, "name": "col_int", "var_type": "INT"},
            {"entity_id": None, "name": "col_float", "var_type": "FLOAT"},
            {"entity_id": None, "name": "col_char", "var_type": "CHAR"},
            {"entity_id": None, "name": "col_text", "var_type": "VARCHAR"},
            {"entity_id": None, "name": "col_binary", "var_type": "BINARY"},
            {"entity_id": None, "name": "col_boolean", "var_type": "BOOL"},
            {"entity_id": None, "name": "event_timestamp", "var_type": "TIMESTAMP"},
            {"entity_id": None, "name": "created_at", "var_type": "TIMESTAMP"},
            {"entity_id": None, "name": "cust_id", "var_type": "INT"},
        ],
        "event_timestamp_column": "event_timestamp",
        "record_creation_date_column": "created_at",
        "default_feature_job_setting": None,
        "created_at": None,
        "updated_at": None,
        "user_id": None,
        "status": None,
    }


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
    assert snowflake_event_data.status == EventDataStatus.DRAFT
    assert isinstance(snowflake_event_data.created_at, datetime)
    assert isinstance(snowflake_event_data.tabular_source.feature_store_id, ObjectId)

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

    # check that event data columns for autocompletion
    assert set(event_data.columns).issubset(dir(event_data))
    assert event_data._ipython_key_completions_() == set(event_data.columns)

    event_data_dict["id"] = event_data.id
    assert event_data.dict() == event_data_dict

    # user input validation
    with pytest.raises(TypeError) as exc:
        EventData.from_tabular_source(
            tabular_source=snowflake_database_table,
            name=123,
            event_timestamp_column=234,
            record_creation_date_column=345,
            credentials=config.credentials,
        )
    assert 'type of argument "name" must be str; got int instead' in str(exc.value)


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
    with pytest.raises(KeyError) as exc:
        _ = snowflake_event_data["non_exist_column"]
    assert 'Column "non_exist_column" does not exist!' in str(exc.value)

    with pytest.raises(AttributeError) as exc:
        _ = snowflake_event_data.non_exist_column
    assert "'EventData' object has no attribute 'non_exist_column'" in str(exc.value)

    # check __getattr__ is working properly
    assert isinstance(snowflake_event_data.col_int, EventDataColumn)

    # when accessing the `columns` attribute, make sure we retrieve it properly
    assert set(snowflake_event_data.columns) == {
        "col_char",
        "col_float",
        "col_boolean",
        "event_timestamp",
        "col_text",
        "created_at",
        "col_binary",
        "col_int",
        "cust_id",
    }


def test_event_data_column__as_entity(snowflake_event_data):
    """
    Test setting a column in the event data as entity
    """
    # check no column associate with any entity
    assert all([col.entity_id is None for col in snowflake_event_data.columns_info])

    # create entity
    entity = Entity(name="customer", serving_names=["cust_id"])
    entity.save()

    col_int = snowflake_event_data.col_int
    assert isinstance(col_int, EventDataColumn)
    snowflake_event_data.col_int.as_entity("customer")
    assert snowflake_event_data.col_int.info.entity_id == entity.id

    with pytest.raises(TypeError) as exc:
        snowflake_event_data.col_int.as_entity(1234)
    assert 'type of argument "entity_name" must be one of (str, NoneType); got int instead' in str(
        exc.value
    )

    with pytest.raises(RecordRetrievalException) as exc:
        snowflake_event_data.col_int.as_entity("some_random_entity")
    assert 'Entity name "some_random_entity" not found!' in str(exc.value)

    # remove entity association
    snowflake_event_data.col_int.as_entity(None)
    assert snowflake_event_data.col_int.info.entity_id is None


def test_event_data_column__as_entity__saved_event_data(saved_event_data, config):
    """
    Test setting a column in the event data as entity (saved event data)
    """
    # check no column associate with any entity
    assert all([col.entity_id is None for col in saved_event_data.columns_info])

    # create entity
    entity = Entity(name="customer", serving_names=["cust_id"])
    entity.save()

    saved_event_data.col_int.as_entity("customer")
    assert saved_event_data.saved is True
    assert saved_event_data.col_int.info.entity_id == entity.id

    # check that the column entity map is saved to persistent
    client = config.get_client()
    response = client.get(url=f"/event_data/{saved_event_data.id}")
    response_dict = response.json()
    for col in response_dict["columns_info"]:
        if col["name"] == "col_int":
            assert col["entity_id"] == str(entity.id)


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
        with patch("featurebyte.api.api_object.Configurations"):
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


def test_info(saved_event_data):
    """
    Test info
    """
    verbose_info = saved_event_data.info(verbose=True)
    non_verbose_info = saved_event_data.info(verbose=False)
    expected_non_verbose_info = {
        "name": "sf_event_data",
        "event_timestamp_column": "event_timestamp",
        "record_creation_date_column": "created_at",
    }
    expected_verbose_info = {
        **expected_non_verbose_info,
        "columns": [
            {"entity": None, "name": "col_int", "var_type": "INT"},
            {"entity": None, "name": "col_float", "var_type": "FLOAT"},
            {"entity": None, "name": "col_char", "var_type": "CHAR"},
            {"entity": None, "name": "col_text", "var_type": "VARCHAR"},
            {"entity": None, "name": "col_binary", "var_type": "BINARY"},
            {"entity": None, "name": "col_boolean", "var_type": "BOOL"},
            {"entity": None, "name": "event_timestamp", "var_type": "TIMESTAMP"},
            {"entity": None, "name": "created_at", "var_type": "TIMESTAMP"},
            {"entity": None, "name": "cust_id", "var_type": "INT"},
        ],
    }
    assert non_verbose_info == expected_non_verbose_info
    assert verbose_info.items() > expected_verbose_info.items()
    assert set(verbose_info).difference(expected_verbose_info) == {"created_at", "updated_at"}


def test_event_data__save__exceptions(saved_event_data):
    """
    Test save event data failure due to conflict
    """
    # test duplicated record exception when record exists
    with pytest.raises(ObjectHasBeenSavedError) as exc:
        saved_event_data.save()
    expected_msg = f'EventData (id: "{saved_event_data.id}") has been saved before.'
    assert expected_msg in str(exc.value)


def test_event_data__record_creation_exception(snowflake_event_data):
    """
    Test save event data failure due to conflict
    """
    # check unhandled response status code
    with pytest.raises(RecordCreationException):
        with patch("featurebyte.api.api_object.Configurations"):
            snowflake_event_data.save()


def test_update_default_job_setting(snowflake_event_data, config):
    """
    Test update default job setting on non-saved event data
    """

    # make sure the event data is not saved
    client = config.get_client()
    response = client.get(url=f"/event_data/{snowflake_event_data.id}")
    assert response.status_code == 404
    assert snowflake_event_data.saved is False

    assert snowflake_event_data.default_feature_job_setting is None
    snowflake_event_data.update_default_feature_job_setting(
        feature_job_setting={
            "blind_spot": "1m30s",
            "frequency": "10m",
            "time_modulo_frequency": "2m",
        }
    )
    assert snowflake_event_data.saved is False
    assert snowflake_event_data.default_feature_job_setting == FeatureJobSetting(
        blind_spot="1m30s", frequency="10m", time_modulo_frequency="2m"
    )
    feature_store_id = snowflake_event_data.tabular_source.feature_store_id
    assert isinstance(feature_store_id, ObjectId)


def test_update_default_job_setting__saved_event_data(saved_event_data, config):
    """
    Test update default job setting on saved event data
    """
    assert saved_event_data.default_feature_job_setting is None
    saved_event_data.update_default_feature_job_setting(
        feature_job_setting={
            "blind_spot": "1m30s",
            "frequency": "6m",
            "time_modulo_frequency": "3m",
        }
    )
    assert saved_event_data.saved is True

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


@patch("featurebyte.api.event_data.is_notebook")
@patch("featurebyte.api.event_data.EventData.post_async_task")
def test_update_default_feature_job_setting__using_feature_job_analysis(
    mock_post_async_task,
    mock_is_notebook,
    saved_event_data,
    config,
):
    """
    Update default feature job setting using feature job analysis
    """
    # test update default feature job setting by using feature job analysis
    mock_post_async_task.return_value = {
        "analysis_result": {
            "recommended_feature_job_setting": {
                "frequency": 180,
                "job_time_modulo_frequency": 61,
                "blind_spot": 395,
                "feature_cutoff_modulo_frequency": 26,
            }
        },
        "analysis_report": "<html>This is analysis report!</html>",
    }

    with patch_import_package("IPython.display") as mock_mod:
        mock_is_notebook.return_value = False
        saved_event_data.update_default_feature_job_setting()
        assert saved_event_data.default_feature_job_setting == FeatureJobSetting(
            blind_spot="395s",
            frequency="180s",
            time_modulo_frequency="61s",
        )
        # check that ipython display not get called
        assert mock_mod.display.call_count == 0
        assert mock_mod.HTML.call_count == 0

    with patch_import_package("IPython.display") as mock_mod:
        mock_is_notebook.return_value = True
        saved_event_data.update_default_feature_job_setting()

        # check that ipython display get called
        assert mock_mod.display.call_count == 1
        assert mock_mod.HTML.call_count == 1


def test_update_default_job_setting__record_update_exception(snowflake_event_data):
    """
    Test unexpected exception during record update
    """
    with pytest.raises(RecordUpdateException):
        with patch("featurebyte.api.api_object.Configurations"):
            snowflake_event_data.update_default_feature_job_setting(
                feature_job_setting={
                    "blind_spot": "1m",
                    "frequency": "2m",
                    "time_modulo_frequency": "1m",
                }
            )


def test_update_default_job_setting__feature_job_setting_analysis_failure__event_data_not_saved(
    snowflake_event_data, config
):
    """
    Test update failure due to event data not saved
    """
    with pytest.raises(RecordCreationException) as exc:
        snowflake_event_data.update_default_feature_job_setting()
    expected_msg = f'EventData (id: "{snowflake_event_data.id}") not found. Please save the EventData object first.'
    assert expected_msg in str(exc)


@pytest.fixture(name="mock_process_store")
def mock_process_store_fixture():
    with patch("featurebyte.service.task_manager.ProcessStore") as mock:
        task_id = ObjectId()
        mock.return_value.submit = AsyncMock()
        mock.return_value.submit.return_value = task_id
        yield mock


def test_update_default_job_setting__feature_job_setting_analysis_failure(
    mock_process_store,
    saved_event_data,
    config,
):
    """
    Test feature job setting task failure
    """
    get_return = {
        "id": ObjectId(),
        "process": Mock(),
        "output_path": "some_output_path",
        "payload": {},
        "status": "FAILURE",
        "traceback": "ValueError: Event Data not found",
    }
    mock_process_store.return_value.get = AsyncMock()
    mock_process_store.return_value.get.return_value = get_return
    with pytest.raises(RecordCreationException) as exc:
        saved_event_data.update_default_feature_job_setting()
    assert "ValueError: Event Data not found" in str(exc.value)


def test_update_record_creation_date_column__unsaved_object(snowflake_database_table, config):
    """Test update record creation date column (unsaved event data)"""
    event_data = EventData.from_tabular_source(
        tabular_source=snowflake_database_table,
        name="event_data",
        event_timestamp_column="event_timestamp",
        credentials=config.credentials,
    )
    assert event_data.record_creation_date_column is None
    event_data.update_record_creation_date_column("created_at")
    assert event_data.record_creation_date_column == "created_at"


def test_update_record_creation_date_column__saved_object(saved_event_data):
    """Test update record creation date column (saved event data)"""
    saved_event_data.update_record_creation_date_column("col_float")
    assert saved_event_data.record_creation_date_column == "col_float"

    # check that validation logic works
    with pytest.raises(ValidationError):
        saved_event_data.update_record_creation_date_column("random_column_name")


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
    assert loaded_event_data.saved is True
    assert loaded_event_data == snowflake_event_data
    assert EventData.get_by_id(id=snowflake_event_data.id) == snowflake_event_data

    with pytest.raises(RecordRetrievalException) as exc:
        EventData.get("unknown_event_data")
    expected_msg = (
        'EventData (name: "unknown_event_data") not found. '
        "Please save the EventData object first."
    )
    assert expected_msg in str(exc.value)


@patch("featurebyte.api.database_table.FeatureStore.get_session")
def test_get_event_data__schema_has_been_changed(mock_get_session, saved_event_data):
    """
    Test retrieving event data after table schema has been changed
    """
    recent_schema = {"column": "INT"}
    mock_get_session.return_value.list_table_schema.return_value = recent_schema
    with pytest.raises(TableSchemaHasBeenChangedError) as exc:
        EventData.get_by_id(saved_event_data.id)
    assert "Table schema has been changed." in str(exc.value)

    # this is ok as additional column should not break backward compatibility
    recent_schema = {
        "col_binary": "BINARY",
        "col_boolean": "BOOL",
        "col_char": "CHAR",
        "col_float": "FLOAT",
        "col_int": "INT",
        "col_text": "VARCHAR",
        "created_at": "TIMESTAMP",
        "cust_id": "INT",
        "event_timestamp": "TIMESTAMP",
        "additional_column": "INT",
    }
    mock_get_session.return_value.list_table_schema.return_value = recent_schema
    _ = EventData.get_by_id(saved_event_data.id)


def test_default_feature_job_setting_history(saved_event_data):
    """
    Test default_feature_job_setting_history on saved event data
    """
    assert saved_event_data.default_feature_job_setting is None
    setting_history = saved_event_data.default_feature_job_setting_history
    assert len(setting_history) == 1
    assert setting_history[0].items() > {"setting": None}.items()
    t1 = datetime.utcnow()
    saved_event_data.update_default_feature_job_setting(
        feature_job_setting={
            "blind_spot": "1m30s",
            "frequency": "10m",
            "time_modulo_frequency": "2m",
        }
    )
    t2 = datetime.utcnow()

    history = saved_event_data.default_feature_job_setting_history
    expected_history_0 = {
        "setting": {"blind_spot": "1m30s", "frequency": "10m", "time_modulo_frequency": "2m"}
    }
    assert len(history) == 2
    assert history[0].items() >= expected_history_0.items()
    assert t2 >= datetime.fromisoformat(history[0]["created_at"]) >= t1

    saved_event_data.update_default_feature_job_setting(
        feature_job_setting={
            "blind_spot": "1m",
            "frequency": "5m",
            "time_modulo_frequency": "2m",
        }
    )
    t3 = datetime.utcnow()

    history = saved_event_data.default_feature_job_setting_history
    expected_history_1 = {
        "setting": {"blind_spot": "1m", "frequency": "5m", "time_modulo_frequency": "2m"}
    }
    assert len(history) == 3
    assert history[1].items() >= expected_history_0.items()
    assert history[0].items() >= expected_history_1.items()
    assert t3 >= datetime.fromisoformat(history[0]["created_at"]) >= t2

    # check audit history
    audit_history = saved_event_data.audit()
    expected_pagination_info = {"page": 1, "page_size": 10, "total": 3}
    assert audit_history.items() > expected_pagination_info.items()
    history_data = audit_history["data"]
    assert len(history_data) == 3
    assert (
        history_data[0].items()
        > {
            "name": 'update: "sf_event_data"',
            "action_type": "UPDATE",
        }.items()
    )
    assert (
        history_data[0]["previous_values"].items()
        > {
            "default_feature_job_setting": {
                "blind_spot": "1m30s",
                "frequency": "10m",
                "time_modulo_frequency": "2m",
            }
        }.items()
    )
    assert (
        history_data[0]["current_values"].items()
        > {
            "default_feature_job_setting": {
                "blind_spot": "1m",
                "frequency": "5m",
                "time_modulo_frequency": "2m",
            }
        }.items()
    )
    assert (
        history_data[1].items()
        > {
            "name": 'update: "sf_event_data"',
            "action_type": "UPDATE",
            "previous_values": {"default_feature_job_setting": None, "updated_at": None},
        }.items()
    )
    assert (
        history_data[1]["current_values"].items()
        > {
            "default_feature_job_setting": {
                "blind_spot": "1m30s",
                "frequency": "10m",
                "time_modulo_frequency": "2m",
            }
        }.items()
    )
    assert (
        history_data[2].items()
        > {
            "name": 'insert: "sf_event_data"',
            "action_type": "INSERT",
            "previous_values": {},
        }.items()
    )
    assert (
        history_data[2]["current_values"].items()
        > {
            "name": "sf_event_data",
            "event_timestamp_column": "event_timestamp",
            "record_creation_date_column": "created_at",
            "default_feature_job_setting": None,
            "status": "DRAFT",
        }.items()
    )
