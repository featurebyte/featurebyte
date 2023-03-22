"""
Unit test for EventTable class
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import Mock, patch
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest
from bson.objectid import ObjectId

from featurebyte.api.entity import Entity
from featurebyte.api.event_table import EventTable
from featurebyte.enum import TableDataType
from featurebyte.exception import (
    DuplicatedRecordException,
    ObjectHasBeenSavedError,
    RecordCreationException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.models.event_table import EventTableModel
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.node.cleaning_operation import MissingValueImputation
from featurebyte.schema.task import Task, TaskStatus
from tests.unit.api.base_data_test import BaseTableTestSuite, DataType
from tests.util.helper import check_sdk_code_generation


@pytest.fixture(name="event_table_dict")
def event_table_dict_fixture(snowflake_database_table):
    """EventTable in serialized dictionary format"""
    return {
        "type": "event_table",
        "name": "sf_event_table",
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
        "record_creation_timestamp_column": "created_at",
        "default_feature_job_setting": None,
        "created_at": None,
        "updated_at": None,
        "user_id": None,
    }


def test_create_event_table(snowflake_database_table, event_table_dict):
    """
    Test EventTable creation using tabular source
    """
    event_table = snowflake_database_table.create_event_table(
        name="sf_event_table",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
        record_creation_timestamp_column="created_at",
    )

    # check that node parameter is set properly
    node_params = event_table.frame.node.parameters
    assert node_params.id == event_table.id
    assert node_params.type == TableDataType.EVENT_TABLE

    # check that event table columns for autocompletion
    assert set(event_table.columns).issubset(dir(event_table))
    assert event_table._ipython_key_completions_() == set(event_table.columns)

    output = event_table.dict(by_alias=True)
    event_table_dict["_id"] = event_table.id
    event_table_dict["created_at"] = event_table.created_at
    event_table_dict["updated_at"] = event_table.updated_at
    event_table_dict["columns_info"][0]["semantic_id"] = event_table.columns_info[0].semantic_id
    event_table_dict["columns_info"][6]["semantic_id"] = event_table.columns_info[6].semantic_id
    assert output == event_table_dict

    # user input validation
    with pytest.raises(TypeError) as exc:
        snowflake_database_table.create_event_table(
            name=123,
            event_id_column="col_int",
            event_timestamp_column=234,
            record_creation_timestamp_column=345,
        )
    assert 'type of argument "name" must be str; got int instead' in str(exc.value)


def test_create_event_table__duplicated_record(saved_event_table, snowflake_database_table):
    """
    Test EventTable creation failure due to duplicated event table name
    """
    _ = saved_event_table
    with pytest.raises(DuplicatedRecordException) as exc:
        snowflake_database_table.create_event_table(
            name="sf_event_table",
            event_id_column="col_int",
            event_timestamp_column="event_timestamp",
            record_creation_timestamp_column="created_at",
        )
    assert 'EventTable (event_table.name: "sf_event_table") exists in saved record.' in str(
        exc.value
    )


def test_create_event_table__retrieval_exception(snowflake_database_table):
    """
    Test EventTable creation failure due to retrieval exception
    """
    with pytest.raises(RecordRetrievalException):
        with patch("featurebyte.api.base_table.Configurations"):
            snowflake_database_table.create_event_table(
                name="sf_event_table",
                event_id_column="col_int",
                event_timestamp_column="event_timestamp",
                record_creation_timestamp_column="created_at",
            )


def test_deserialization(
    event_table_dict,
    snowflake_feature_store,
    snowflake_execute_query,
    expected_snowflake_table_preview_query,
):
    """
    Test deserialize event table dictionary
    """
    _ = snowflake_execute_query
    # setup proper configuration to deserialize the event table object
    event_table_dict["feature_store"] = snowflake_feature_store
    event_table = EventTable.parse_obj(event_table_dict)
    assert event_table.preview_sql() == expected_snowflake_table_preview_query


def test_deserialization__column_name_not_found(
    event_table_dict, snowflake_feature_store, snowflake_execute_query
):
    """
    Test column not found during deserialize event table
    """
    _ = snowflake_execute_query
    event_table_dict["feature_store"] = snowflake_feature_store
    event_table_dict["record_creation_timestamp_column"] = "some_random_name"
    with pytest.raises(ValueError) as exc:
        EventTable.parse_obj(event_table_dict)
    assert 'Column "some_random_name" not found in the table!' in str(exc.value)

    event_table_dict["record_creation_timestamp_column"] = "created_at"
    event_table_dict["event_timestamp_column"] = "some_timestamp_column"
    with pytest.raises(ValueError) as exc:
        EventTable.parse_obj(event_table_dict)
    assert 'Column "some_timestamp_column" not found in the table!' in str(exc.value)


class TestEventTableTestSuite(BaseTableTestSuite):

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
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      "col_text" AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("event_timestamp" AS STRING) AS "event_timestamp",
      CAST("created_at" AS STRING) AS "created_at",
      "cust_id" AS "cust_id"
    FROM "sf_database"."sf_schema"."sf_table"
    LIMIT 10
    """
    expected_data_column_sql = """
    SELECT
      "col_int" AS "col_int"
    FROM "sf_database"."sf_schema"."sf_table"
    LIMIT 10
    """
    expected_clean_data_sql = """
    SELECT
      CAST(CASE WHEN (
        "col_int" IS NULL
      ) THEN 0 ELSE "col_int" END AS BIGINT) AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      "col_text" AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("event_timestamp" AS STRING) AS "event_timestamp",
      CAST("created_at" AS STRING) AS "created_at",
      "cust_id" AS "cust_id"
    FROM "sf_database"."sf_schema"."sf_table"
    LIMIT 10
    """


def test_info__event_table_without_record_creation_date(snowflake_database_table_dimension_table):
    """Test info on event table with record creation timestamp is None"""
    event_table = snowflake_database_table_dimension_table.create_event_table(
        name="sf_event_table",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
    )

    # make sure .info() can be executed without throwing any error
    _ = event_table.info()


def test_info(saved_event_table, cust_id_entity):
    """
    Test info
    """
    _ = cust_id_entity
    saved_event_table.cust_id.as_entity("customer")
    info_dict = saved_event_table.info()
    expected_info = {
        "name": "sf_event_table",
        "event_timestamp_column": "event_timestamp",
        "record_creation_timestamp_column": "created_at",
        "default_feature_job_setting": None,
        "status": "DRAFT",
        "entities": [{"name": "customer", "serving_names": ["cust_id"], "catalog_name": "default"}],
        "column_count": 9,
        "table_details": {
            "database_name": "sf_database",
            "schema_name": "sf_schema",
            "table_name": "sf_table",
        },
        "catalog_name": "default",
    }
    assert info_dict.items() > expected_info.items(), info_dict
    assert info_dict["updated_at"] is not None, info_dict["updated_at"]
    assert "created_at" in info_dict, info_dict

    # update critical data info
    saved_event_table.col_int.update_critical_data_info(
        cleaning_operations=[MissingValueImputation(imputed_value=0)]
    )

    verbose_info_dict = saved_event_table.info(verbose=True)
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


def test_event_table__save__exceptions(saved_event_table):
    """
    Test save event table failure due to conflict
    """
    # test duplicated record exception when record exists
    with pytest.raises(ObjectHasBeenSavedError) as exc:
        saved_event_table.save()
    expected_msg = f'EventTable (id: "{saved_event_table.id}") has been saved before.'
    assert expected_msg in str(exc.value)


def test_event_table__record_creation_exception(snowflake_database_table, snowflake_event_table_id):
    """
    Test save event table failure due to conflict
    """
    # check unhandled response status code
    with pytest.raises(RecordCreationException):
        with patch("featurebyte.api.api_object.Configurations"):
            snowflake_database_table.create_event_table(
                name="sf_event_table",
                event_id_column="col_int",
                event_timestamp_column="event_timestamp",
                record_creation_timestamp_column="created_at",
                _id=snowflake_event_table_id,
            )


def test_update_default_job_setting__saved_event_table(
    saved_event_table, config, mock_api_object_cache
):
    """
    Test update default job setting on saved event table
    """
    _ = mock_api_object_cache

    assert saved_event_table.default_feature_job_setting is None
    saved_event_table.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(
            blind_spot="1m30s",
            frequency="6m",
            time_modulo_frequency="3m",
        )
    )
    assert saved_event_table.saved is True

    # check updated feature job settings stored at the persistent & memory
    assert saved_event_table.default_feature_job_setting == FeatureJobSetting(
        blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
    )
    client = config.get_client()
    response = client.get(url=f"/event_table/{saved_event_table.id}")
    assert response.status_code == 200
    assert response.json()["default_feature_job_setting"] == {
        "blind_spot": "1m30s",
        "frequency": "6m",
        "time_modulo_frequency": "3m",
    }


@patch("featurebyte.api.event_table.EventTable.post_async_task")
@patch("featurebyte.api.feature_job_setting_analysis.FeatureJobSettingAnalysis.get_by_id")
def test_update_default_feature_job_setting__using_feature_job_analysis(
    mock_get_by_id,
    mock_post_async_task,
    saved_event_table,
):
    """
    Update default feature job setting using feature job analysis
    """
    # test update default feature job setting by using feature job analysis
    mock_post_async_task.return_value = {"_id": ObjectId()}
    analysis = Mock()
    analysis.get_recommendation.return_value = FeatureJobSetting(
        blind_spot="0",
        frequency="24h",
        time_modulo_frequency="0",
    )
    mock_get_by_id.return_value = analysis
    saved_event_table.initialize_default_feature_job_setting()

    # should make a call to post_async_task
    mock_post_async_task.assert_called_once_with(
        route="/feature_job_setting_analysis",
        payload={
            "_id": mock_post_async_task.call_args[1]["payload"]["_id"],
            "name": None,
            "event_table_id": "6337f9651050ee7d5980660d",
            "analysis_date": None,
            "analysis_length": 2419200,
            "min_featurejob_period": 60,
            "exclude_late_job": False,
            "blind_spot_buffer_setting": 5,
            "job_time_buffer_setting": "auto",
            "late_data_allowance": 5e-05,
        },
    )

    # should make a call to display_report()
    analysis.display_report.assert_called_once()

    # should make a call to get_recommendation()
    analysis.get_recommendation.assert_called_once()

    # default_feature_job_setting should be updated
    assert saved_event_table.default_feature_job_setting == analysis.get_recommendation.return_value


@patch("featurebyte.service.feature_job_setting_analysis.EventTableService.get_document")
def test_update_default_feature_job_setting__using_feature_job_analysis_no_creation_date_col(
    mock_get_document,
    saved_event_table,
    config,
):
    """
    Update default feature job setting using feature job analysis
    """
    mock_get_document.return_value = Mock(record_creation_timestamp_column=None)

    with pytest.raises(RecordCreationException) as exc:
        saved_event_table.initialize_default_feature_job_setting()
    assert "Creation date column is not available for the event table." in str(exc)


@patch("featurebyte.api.event_table.EventTable.post_async_task")
def test_update_default_feature_job_setting__using_feature_job_analysis_high_frequency(
    mock_post_async_task,
    saved_event_table,
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
        saved_event_table.initialize_default_feature_job_setting()
    assert "HighUpdateFrequencyError" in str(exc)


def test_update_default_job_setting__record_update_exception(snowflake_event_table):
    """
    Test unexpected exception during record update
    """
    with pytest.raises(RecordUpdateException):
        with patch("featurebyte.api.api_object.Configurations"):
            snowflake_event_table.update_default_feature_job_setting(
                feature_job_setting=FeatureJobSetting(
                    blind_spot="1m", frequency="2m", time_modulo_frequency="1m"
                )
            )


def test_update_default_job_setting__feature_job_setting_analysis_failure__event_table_not_saved(
    snowflake_event_table, config
):
    """
    Test update failure due to event table not saved
    """
    with pytest.raises(RecordCreationException) as exc:
        snowflake_event_table.__dict__["id"] = ObjectId()  # assign a random ID to event table
        snowflake_event_table.initialize_default_feature_job_setting()
    expected_msg = f'EventTable (id: "{snowflake_event_table.id}") not found. Please save the EventTable object first.'
    assert expected_msg in str(exc)


@pytest.fixture(name="mock_celery")
def mock_celery_fixture():
    with patch("featurebyte.service.task_manager.celery") as mock_celery:
        mock_celery.send_task.side_effect = lambda *args, **kwargs: Mock(id=uuid4())
        mock_celery.AsyncResult.return_value.status = TaskStatus.STARTED
        yield mock_celery


@pytest.mark.asyncio
async def test_update_default_job_setting__feature_job_setting_analysis_failure(
    mock_celery,
    saved_event_table,
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
    with patch("featurebyte.service.task_manager.TaskManager.get_task") as mock_get_task:
        mock_get_task.return_value = Task(**get_return)
        with pytest.raises(RecordCreationException) as exc:
            saved_event_table.initialize_default_feature_job_setting()
    assert "ValueError: Event Data not found" in str(exc.value)


def test_update_record_creation_timestamp_column__unsaved_object(snowflake_database_table):
    """Test update record creation timestamp column (unsaved event table)"""
    event_table = snowflake_database_table.create_event_table(
        name="event_table",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
    )
    assert event_table.record_creation_timestamp_column is None
    event_table.update_record_creation_timestamp_column("created_at")
    assert event_table.record_creation_timestamp_column == "created_at"


def test_update_record_creation_timestamp_column__saved_object(saved_event_table):
    """Test update record creation timestamp column (saved event table)"""
    saved_event_table.update_record_creation_timestamp_column("created_at")
    assert saved_event_table.record_creation_timestamp_column == "created_at"

    # check that validation logic works
    with pytest.raises(RecordUpdateException) as exc:
        saved_event_table.update_record_creation_timestamp_column("random_column_name")
    expected_msg = 'Column "random_column_name" not found in the table! (type=value_error)'
    assert expected_msg in str(exc.value)

    with pytest.raises(RecordUpdateException) as exc:
        saved_event_table.update_record_creation_timestamp_column("col_float")
    expected_msg = "Column \"col_float\" is expected to have type(s): ['TIMESTAMP', 'TIMESTAMP_TZ'] (type=value_error)"
    assert expected_msg in str(exc.value)


def test_get_event_table(snowflake_event_table, mock_config_path_env):
    """
    Test EventTable.get function
    """
    _ = mock_config_path_env

    # load the event table from the persistent
    loaded_event_table = EventTable.get(snowflake_event_table.name)
    assert loaded_event_table.saved is True
    assert loaded_event_table == snowflake_event_table
    assert EventTable.get_by_id(id=snowflake_event_table.id) == snowflake_event_table

    with pytest.raises(RecordRetrievalException) as exc:
        EventTable.get("unknown_event_table")

    expected_msg = (
        'EventTable (name: "unknown_event_table") not found. '
        "Please save the EventTable object first."
    )
    assert expected_msg in str(exc.value)


@patch("featurebyte.api.source_table.logger")
@patch("featurebyte.service.session_manager.SessionManager.get_session")
def test_get_event_table__schema_has_been_changed(mock_get_session, mock_logger, saved_event_table):
    """
    Test retrieving event table after table schema has been changed
    """
    recent_schema = {"column": "INT"}
    mock_get_session.return_value.list_table_schema.return_value = recent_schema
    _ = EventTable.get_by_id(saved_event_table.id)
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
    _ = EventTable.get_by_id(saved_event_table.id)


def test_default_feature_job_setting_history(saved_event_table):
    """
    Test default_feature_job_setting_history on saved event table
    """
    assert saved_event_table.default_feature_job_setting is None
    setting_history = saved_event_table.default_feature_job_setting_history
    assert len(setting_history) == 1
    assert setting_history[0].items() > {"setting": None}.items()
    t1 = datetime.utcnow()
    saved_event_table.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(
            blind_spot="1m30s", frequency="10m", time_modulo_frequency="2m"
        )
    )
    t2 = datetime.utcnow()

    history = saved_event_table.default_feature_job_setting_history
    expected_history_0 = {
        "setting": {"blind_spot": "1m30s", "frequency": "10m", "time_modulo_frequency": "2m"}
    }
    assert len(history) == 2
    assert history[0].items() >= expected_history_0.items()
    assert t2 >= datetime.fromisoformat(history[0]["created_at"]) >= t1

    saved_event_table.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(
            blind_spot="1m", frequency="5m", time_modulo_frequency="2m"
        )
    )
    t3 = datetime.utcnow()

    history = saved_event_table.default_feature_job_setting_history
    expected_history_1 = {
        "setting": {"blind_spot": "1m", "frequency": "5m", "time_modulo_frequency": "2m"}
    }
    assert len(history) == 3
    assert history[1].items() >= expected_history_0.items()
    assert history[0].items() >= expected_history_1.items()
    assert t3 >= datetime.fromisoformat(history[0]["created_at"]) >= t2

    # check audit history
    audit_history = saved_event_table.audit()

    # check number of actions
    expected_unique_records = pd.DataFrame(
        [
            ("UPDATE", 'update: "sf_event_table"'),
            ("UPDATE", 'update: "sf_event_table"'),
            ("UPDATE", 'update: "sf_event_table"'),
            ("INSERT", 'insert: "sf_event_table"'),
        ],
        columns=["action_type", "name"],
    )
    audit_uniq_records = (
        audit_history[["action_at", "action_type", "name"]].drop_duplicates().reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(
        audit_uniq_records[expected_unique_records.columns], expected_unique_records
    )

    # check the latest action audit records
    audit_records = audit_history[audit_history.action_at == audit_uniq_records.action_at.iloc[0]]
    audit_records.reset_index(drop=True, inplace=True)
    old_updated_at = audit_records.old_value.iloc[0]
    new_updated_at = audit_records.new_value.iloc[0]
    expected_audit_records = pd.DataFrame(
        [
            (
                "UPDATE",
                'update: "sf_event_table"',
                "updated_at",
                old_updated_at,
                new_updated_at,
            ),
            (
                "UPDATE",
                'update: "sf_event_table"',
                "default_feature_job_setting.blind_spot",
                "1m30s",
                "1m",
            ),
            (
                "UPDATE",
                'update: "sf_event_table"',
                "default_feature_job_setting.frequency",
                "10m",
                "5m",
            ),
            (
                "UPDATE",
                'update: "sf_event_table"',
                "default_feature_job_setting.time_modulo_frequency",
                "2m",
                "2m",
            ),
        ],
        columns=["action_type", "name", "field_name", "old_value", "new_value"],
    )
    pd.testing.assert_frame_equal(
        audit_records[expected_audit_records.columns], expected_audit_records
    )

    # check the 2nd latest action audit records
    audit_records = audit_history[audit_history.action_at == audit_uniq_records.action_at.iloc[1]]
    audit_records.reset_index(drop=True, inplace=True)
    old_updated_at = audit_records.old_value.iloc[-1]
    new_updated_at = audit_records.new_value.iloc[-1]
    expected_audit_records = pd.DataFrame(
        [
            ("UPDATE", 'update: "sf_event_table"', "default_feature_job_setting", None, np.nan),
            (
                "UPDATE",
                'update: "sf_event_table"',
                "default_feature_job_setting.blind_spot",
                np.nan,
                "1m30s",
            ),
            (
                "UPDATE",
                'update: "sf_event_table"',
                "default_feature_job_setting.frequency",
                np.nan,
                "10m",
            ),
            (
                "UPDATE",
                'update: "sf_event_table"',
                "default_feature_job_setting.time_modulo_frequency",
                np.nan,
                "2m",
            ),
            (
                "UPDATE",
                'update: "sf_event_table"',
                "updated_at",
                old_updated_at,
                new_updated_at,
            ),
        ],
        columns=["action_type", "name", "field_name", "old_value", "new_value"],
    )
    pd.testing.assert_frame_equal(
        audit_records[expected_audit_records.columns], expected_audit_records
    )

    # check the earliest action audit records
    audit_records = audit_history[audit_history.action_at == audit_uniq_records.action_at.iloc[-1]]
    assert set(audit_records["field_name"]) == {
        "columns_info",
        "created_at",
        "default_feature_job_setting",
        "event_id_column",
        "event_timestamp_column",
        "name",
        "record_creation_timestamp_column",
        "status",
        "tabular_source.feature_store_id",
        "tabular_source.table_details.database_name",
        "tabular_source.table_details.schema_name",
        "tabular_source.table_details.table_name",
        "type",
        "updated_at",
        "user_id",
        "catalog_id",
    }


@patch("featurebyte.api.event_table.EventTable.post_async_task")
@patch("featurebyte.api.feature_job_setting_analysis.FeatureJobSettingAnalysis.get_by_id")
def test_create_new_feature_job_setting_analysis(
    mock_get_by_id,
    mock_post_async_task,
    saved_event_table,
):
    """
    Update default feature job setting using feature job analysis
    """
    # test update default feature job setting by using feature job analysis
    mock_post_async_task.return_value = {"_id": ObjectId()}
    analysis = Mock()
    mock_get_by_id.return_value = analysis
    analysis_date = datetime.utcnow()
    saved_event_table.create_new_feature_job_setting_analysis(
        analysis_date=analysis_date,
        analysis_length=3600,
        min_featurejob_period=100,
        exclude_late_job=True,
        blind_spot_buffer_setting=10,
        job_time_buffer_setting=5,
        late_data_allowance=0.1,
    )

    mock_post_async_task.assert_called_once_with(
        route="/feature_job_setting_analysis",
        payload={
            "_id": mock_post_async_task.call_args[1]["payload"]["_id"],
            "name": None,
            "event_table_id": "6337f9651050ee7d5980660d",
            "analysis_date": analysis_date.isoformat(),
            "analysis_length": 3600,
            "min_featurejob_period": 100,
            "exclude_late_job": True,
            "blind_spot_buffer_setting": 10,
            "job_time_buffer_setting": 5,
            "late_data_allowance": 0.1,
        },
    )

    # should make a call to display_report()
    analysis.display_report.assert_called_once()


@patch("featurebyte.api.feature_job_setting_analysis.FeatureJobSettingAnalysis.list")
def test_list_feature_job_setting_analysis(mock_list, saved_event_table):
    """
    Test list_feature_job_setting_analysis
    """
    output = saved_event_table.list_feature_job_setting_analysis()
    mock_list.assert_called_once_with(event_table_id=saved_event_table.id)
    assert output == mock_list.return_value


def test_event_table__entity_relation_auto_tagging(saved_event_table):
    """Test event table update: entity relation will be created automatically"""
    transaction_entity = Entity(name="transaction", serving_names=["transaction_id"])
    transaction_entity.save()

    customer = Entity(name="customer", serving_names=["customer_id"])
    customer.save()

    # add entities to event table
    assert saved_event_table.event_id_column == "col_int"
    saved_event_table.col_int.as_entity("transaction")
    saved_event_table.cust_id.as_entity("customer")

    updated_transaction_entity = Entity.get_by_id(id=transaction_entity.id)
    assert updated_transaction_entity.parents == [
        {"id": customer.id, "table_type": "event_table", "table_id": saved_event_table.id}
    ]
    updated_customer_entity = Entity.get_by_id(id=customer.id)
    assert updated_customer_entity.parents == []

    # remove primary id column's entity
    saved_event_table.col_int.as_entity(None)
    updated_transaction_entity = Entity.get_by_id(id=transaction_entity.id)
    assert updated_transaction_entity.parents == []


def test_accessing_event_table_attributes(snowflake_event_table):
    """Test accessing event table object attributes"""
    assert snowflake_event_table.saved
    assert snowflake_event_table.record_creation_timestamp_column == "created_at"
    assert snowflake_event_table.default_feature_job_setting is None
    assert snowflake_event_table.event_timestamp_column == "event_timestamp"
    assert snowflake_event_table.event_id_column == "col_int"
    assert snowflake_event_table.timestamp_column == "event_timestamp"


def test_accessing_saved_event_table_attributes(saved_event_table):
    """Test accessing event table object attributes"""
    assert saved_event_table.saved
    assert isinstance(saved_event_table.cached_model, EventTableModel)
    assert saved_event_table.record_creation_timestamp_column == "created_at"
    assert saved_event_table.default_feature_job_setting is None
    assert saved_event_table.event_timestamp_column == "event_timestamp"
    assert saved_event_table.event_id_column == "col_int"
    assert saved_event_table.timestamp_column == "event_timestamp"

    # check synchronization
    feature_job_setting = FeatureJobSetting(
        blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
    )
    cloned = EventTable.get_by_id(id=saved_event_table.id)
    assert cloned.default_feature_job_setting is None
    saved_event_table.update_default_feature_job_setting(feature_job_setting=feature_job_setting)
    assert saved_event_table.default_feature_job_setting == feature_job_setting
    assert cloned.default_feature_job_setting == feature_job_setting


def test_sdk_code_generation(snowflake_database_table, update_fixtures):
    """Check SDK code generation for unsaved table"""
    event_table = snowflake_database_table.create_event_table(
        name="sf_event_table",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
        record_creation_timestamp_column="created_at",
    )
    check_sdk_code_generation(
        event_table.frame,
        to_use_saved_data=False,
        fixture_path="tests/fixtures/sdk_code/event_table.py",
        update_fixtures=update_fixtures,
        table_id=event_table.id,
    )


def test_sdk_code_generation_on_saved_data(saved_event_table, update_fixtures):
    """Check SDK code generation for saved table"""
    check_sdk_code_generation(
        saved_event_table.frame,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/saved_event_table.py",
        update_fixtures=update_fixtures,
        table_id=saved_event_table.id,
    )
