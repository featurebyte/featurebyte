"""
Unit test for EventData class
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest
from bson.objectid import ObjectId

from featurebyte.api.event_data import EventData
from featurebyte.enum import TableDataType
from featurebyte.exception import (
    DuplicatedRecordException,
    ObjectHasBeenSavedError,
    RecordCreationException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.query_graph.model.critical_data_info import MissingValueImputation
from tests.unit.api.base_data_test import BaseDataTestSuite, DataType
from tests.util.helper import patch_import_package


@pytest.fixture(name="event_data_dict")
def event_data_dict_fixture(snowflake_database_table):
    """EventData in serialized dictionary format"""
    return {
        "type": "event_data",
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
            {
                "entity_id": None,
                "name": "col_int",
                "dtype": "INT",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "entity_id": None,
                "name": "col_float",
                "dtype": "FLOAT",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "entity_id": None,
                "name": "col_char",
                "dtype": "CHAR",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "entity_id": None,
                "name": "col_text",
                "dtype": "VARCHAR",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "entity_id": None,
                "name": "col_binary",
                "dtype": "BINARY",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "entity_id": None,
                "name": "col_boolean",
                "dtype": "BOOL",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "entity_id": None,
                "name": "event_timestamp",
                "dtype": "TIMESTAMP_TZ",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "entity_id": None,
                "name": "created_at",
                "dtype": "TIMESTAMP_TZ",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "entity_id": None,
                "name": "cust_id",
                "dtype": "INT",
                "semantic_id": None,
                "critical_data_info": None,
            },
        ],
        "event_timestamp_column": "event_timestamp",
        "event_id_column": "col_int",
        "record_creation_date_column": "created_at",
        "default_feature_job_setting": None,
        "created_at": None,
        "updated_at": None,
        "user_id": None,
        "status": "DRAFT",
        "graph": {"nodes": [], "edges": []},
        "node_name": "",
    }


def test_from_tabular_source(snowflake_database_table, event_data_dict):
    """
    Test EventData creation using tabular source
    """
    event_data = EventData.from_tabular_source(
        tabular_source=snowflake_database_table,
        name="sf_event_data",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
        record_creation_date_column="created_at",
    )

    # check that node parameter is set properly
    node_params = event_data.node.parameters
    assert node_params.id == event_data.id
    assert node_params.type == TableDataType.EVENT_DATA

    # check that event data columns for autocompletion
    assert set(event_data.columns).issubset(dir(event_data))
    assert event_data._ipython_key_completions_() == set(event_data.columns)

    output = event_data.dict()
    event_data_dict["id"] = event_data.id
    event_data_dict["graph"] = output["graph"]
    event_data_dict["node_name"] = output["node_name"]
    assert output == event_data_dict

    # user input validation
    with pytest.raises(TypeError) as exc:
        EventData.from_tabular_source(
            tabular_source=snowflake_database_table,
            name=123,
            event_id_column="col_int",
            event_timestamp_column=234,
            record_creation_date_column=345,
        )
    assert 'type of argument "name" must be str; got int instead' in str(exc.value)


def test_from_tabular_source__duplicated_record(saved_event_data, snowflake_database_table):
    """
    Test EventData creation failure due to duplicated event data name
    """
    _ = saved_event_data
    with pytest.raises(DuplicatedRecordException) as exc:
        EventData.from_tabular_source(
            tabular_source=snowflake_database_table,
            name="sf_event_data",
            event_id_column="col_int",
            event_timestamp_column="event_timestamp",
            record_creation_date_column="created_at",
        )
    assert 'EventData (event_data.name: "sf_event_data") exists in saved record.' in str(exc.value)


def test_from_tabular_source__retrieval_exception(snowflake_database_table):
    """
    Test EventData creation failure due to retrieval exception
    """
    with pytest.raises(RecordRetrievalException):
        with patch("featurebyte.api.data.Configurations"):
            EventData.from_tabular_source(
                tabular_source=snowflake_database_table,
                name="sf_event_data",
                event_id_column="col_int",
                event_timestamp_column="event_timestamp",
                record_creation_date_column="created_at",
            )


def test_deserialization(
    event_data_dict,
    snowflake_feature_store,
    snowflake_execute_query,
    expected_snowflake_table_preview_query,
):
    """
    Test deserialize event data dictionary
    """
    _ = snowflake_execute_query
    # setup proper configuration to deserialize the event data object
    event_data_dict["feature_store"] = snowflake_feature_store
    event_data = EventData.parse_obj(event_data_dict)
    assert event_data.preview_sql() == expected_snowflake_table_preview_query


def test_deserialization__column_name_not_found(
    event_data_dict, snowflake_feature_store, snowflake_execute_query
):
    """
    Test column not found during deserialize event data
    """
    _ = snowflake_execute_query
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


class TestEventDataTestSuite(BaseDataTestSuite):

    data_type = DataType.EVENT_DATA
    col = "col_int"
    expected_columns = {
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
    expected_data_sql = """
    SELECT
      CASE WHEN "col_int" IS NULL THEN 0 ELSE "col_int" END AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      "col_text" AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("event_timestamp" AS VARCHAR) AS "event_timestamp",
      CAST("created_at" AS VARCHAR) AS "created_at",
      "cust_id" AS "cust_id"
    FROM "sf_database"."sf_schema"."sf_table"
    LIMIT 10
    """
    expected_raw_data_sql = """
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      "col_text" AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("event_timestamp" AS VARCHAR) AS "event_timestamp",
      CAST("created_at" AS VARCHAR) AS "created_at",
      "cust_id" AS "cust_id"
    FROM "sf_database"."sf_schema"."sf_table"
    LIMIT 10
    """
    expected_data_column_sql = """
    SELECT
      "col_int"
    FROM (
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
    )
    LIMIT 10
    """


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


def test_info__event_data_without_record_creation_date(
    snowflake_feature_store, snowflake_database_table
):
    """Test info on event data with record creation date is None"""
    snowflake_feature_store.save()
    event_data = EventData.from_tabular_source(
        tabular_source=snowflake_database_table,
        name="sf_event_data",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
    )
    event_data.save()

    # make sure .info() can be executed without throwing any error
    _ = event_data.info()


def test_info(saved_event_data, cust_id_entity):
    """
    Test info
    """
    _ = cust_id_entity
    saved_event_data.cust_id.as_entity("customer")
    info_dict = saved_event_data.info()
    expected_info = {
        "name": "sf_event_data",
        "event_timestamp_column": "event_timestamp",
        "record_creation_date_column": "created_at",
        "default_feature_job_setting": None,
        "status": "DRAFT",
        "entities": [{"name": "customer", "serving_names": ["cust_id"]}],
        "column_count": 9,
        "table_details": {
            "database_name": "sf_database",
            "schema_name": "sf_schema",
            "table_name": "sf_table",
        },
    }
    assert info_dict.items() > expected_info.items(), info_dict
    assert info_dict["updated_at"] is not None, info_dict["updated_at"]
    assert "created_at" in info_dict, info_dict

    # update critical data info
    saved_event_data.col_int.update_critical_data_info(
        cleaning_operations=[MissingValueImputation(imputed_value=0)]
    )

    verbose_info_dict = saved_event_data.info(verbose=True)
    assert verbose_info_dict.items() > expected_info.items(), info_dict
    assert verbose_info_dict["updated_at"] is not None, verbose_info_dict["updated_at"]
    assert "created_at" in verbose_info_dict, verbose_info_dict
    assert verbose_info_dict["columns_info"] == [
        {
            "name": "col_int",
            "dtype": "INT",
            "entity": None,
            "semantic": "event_id",
            "critical_data_info": {
                "cleaning_operations": [{"type": "missing", "imputed_value": 0}]
            },
        },
        {
            "name": "col_float",
            "dtype": "FLOAT",
            "entity": None,
            "semantic": None,
            "critical_data_info": None,
        },
        {
            "name": "col_char",
            "dtype": "CHAR",
            "entity": None,
            "semantic": None,
            "critical_data_info": None,
        },
        {
            "name": "col_text",
            "dtype": "VARCHAR",
            "entity": None,
            "semantic": None,
            "critical_data_info": None,
        },
        {
            "name": "col_binary",
            "dtype": "BINARY",
            "entity": None,
            "semantic": None,
            "critical_data_info": None,
        },
        {
            "name": "col_boolean",
            "dtype": "BOOL",
            "entity": None,
            "semantic": None,
            "critical_data_info": None,
        },
        {
            "name": "event_timestamp",
            "dtype": "TIMESTAMP_TZ",
            "entity": None,
            "semantic": "event_timestamp",
            "critical_data_info": None,
        },
        {
            "name": "created_at",
            "dtype": "TIMESTAMP_TZ",
            "entity": None,
            "semantic": None,
            "critical_data_info": None,
        },
        {
            "name": "cust_id",
            "dtype": "INT",
            "entity": "customer",
            "semantic": None,
            "critical_data_info": None,
        },
    ]


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
        feature_job_setting=FeatureJobSetting(
            blind_spot="1m30s",
            frequency="10m",
            time_modulo_frequency="2m",
        )
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
        feature_job_setting=FeatureJobSetting(
            blind_spot="1m30s",
            frequency="6m",
            time_modulo_frequency="3m",
        )
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


@patch("featurebyte.common.env_util.is_notebook")
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


@patch("featurebyte.service.feature_job_setting_analysis.EventDataService.get_document")
def test_update_default_feature_job_setting__using_feature_job_analysis_no_creation_date_col(
    mock_get_document,
    saved_event_data,
    config,
):
    """
    Update default feature job setting using feature job analysis
    """
    mock_get_document.return_value = Mock(record_creation_date_column=None)

    with pytest.raises(RecordCreationException) as exc:
        saved_event_data.update_default_feature_job_setting()
    assert "Creation date column is not available for the event data." in str(exc)


@patch("featurebyte.api.event_data.EventData.post_async_task")
def test_update_default_feature_job_setting__using_feature_job_analysis_high_frequency(
    mock_post_async_task,
    saved_event_data,
    config,
):
    """
    Update default feature job setting using feature job analysis
    """
    # test update default feature job setting by using feature job analysis
    mock_post_async_task.side_effect = RecordCreationException(
        response=Mock(
            json=lambda: {
                "status": "FAILURE",
                "traceback": "featurebyte_freeware.feature_job_analysis.analysis.HighUpdateFrequencyError",
            }
        )
    )

    with pytest.raises(RecordCreationException) as exc:
        saved_event_data.update_default_feature_job_setting()
    assert "HighUpdateFrequencyError" in str(exc)


def test_update_default_job_setting__record_update_exception(snowflake_event_data):
    """
    Test unexpected exception during record update
    """
    with pytest.raises(RecordUpdateException):
        with patch("featurebyte.api.api_object.Configurations"):
            snowflake_event_data.update_default_feature_job_setting(
                feature_job_setting=FeatureJobSetting(
                    blind_spot="1m", frequency="2m", time_modulo_frequency="1m"
                )
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


def test_update_record_creation_date_column__unsaved_object(snowflake_database_table):
    """Test update record creation date column (unsaved event data)"""
    event_data = EventData.from_tabular_source(
        tabular_source=snowflake_database_table,
        name="event_data",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
    )
    assert event_data.record_creation_date_column is None
    event_data.update_record_creation_date_column("created_at")
    assert event_data.record_creation_date_column == "created_at"


def test_update_record_creation_date_column__saved_object(saved_event_data):
    """Test update record creation date column (saved event data)"""
    saved_event_data.update_record_creation_date_column("created_at")
    assert saved_event_data.record_creation_date_column == "created_at"

    # check that validation logic works
    with pytest.raises(RecordUpdateException) as exc:
        saved_event_data.update_record_creation_date_column("random_column_name")
    expected_msg = 'Column "random_column_name" not found in the table! (type=value_error)'
    assert expected_msg in str(exc.value)

    with pytest.raises(RecordUpdateException) as exc:
        saved_event_data.update_record_creation_date_column("col_float")
    expected_msg = "Column \"col_float\" is expected to have type(s): ['TIMESTAMP', 'TIMESTAMP_TZ'] (type=value_error)"
    assert expected_msg in str(exc.value)


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
        lazy_event_data = EventData.get("unknown_event_data")
        _ = lazy_event_data.name
    expected_msg = (
        'EventData (name: "unknown_event_data") not found. '
        "Please save the EventData object first."
    )
    assert expected_msg in str(exc.value)


@patch("featurebyte.api.database_table.logger")
@patch("featurebyte.service.session_manager.SessionManager.get_session")
def test_get_event_data__schema_has_been_changed(mock_get_session, mock_logger, saved_event_data):
    """
    Test retrieving event data after table schema has been changed
    """
    recent_schema = {"column": "INT"}
    mock_get_session.return_value.list_table_schema.return_value = recent_schema
    lazy_event_data = EventData.get_by_id(saved_event_data.id)
    _ = lazy_event_data.id
    assert mock_logger.warning.call_args.args[0] == "Table schema has been changed."

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
        feature_job_setting=FeatureJobSetting(
            blind_spot="1m30s", frequency="10m", time_modulo_frequency="2m"
        )
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
        feature_job_setting=FeatureJobSetting(
            blind_spot="1m", frequency="5m", time_modulo_frequency="2m"
        )
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
    expected_pagination_info = {"page": 1, "page_size": 10, "total": 4}
    assert audit_history.items() > expected_pagination_info.items()
    history_data = audit_history["data"]
    assert len(history_data) == 4
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
            "previous_values": {
                "default_feature_job_setting": None,
                "updated_at": history_data[1]["previous_values"]["updated_at"],
            },
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
        history_data[3].items()
        > {
            "name": 'insert: "sf_event_data"',
            "action_type": "INSERT",
            "previous_values": {},
        }.items()
    )
    assert (
        history_data[3]["current_values"].items()
        > {
            "name": "sf_event_data",
            "event_timestamp_column": "event_timestamp",
            "record_creation_date_column": "created_at",
            "default_feature_job_setting": None,
            "status": "DRAFT",
        }.items()
    )
