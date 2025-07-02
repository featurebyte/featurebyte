"""
Unit test for EventTable class
"""

from __future__ import annotations

import textwrap
from datetime import datetime
from unittest import mock
from unittest.mock import Mock, patch
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest
from bson.objectid import ObjectId
from pydantic import ValidationError
from typeguard import TypeCheckError

from featurebyte import Configurations, TimestampSchema, TimeZoneColumn
from featurebyte.api.entity import Entity
from featurebyte.api.event_table import EventTable
from featurebyte.enum import DBVarType, SourceType, TableDataType
from featurebyte.exception import (
    DuplicatedRecordException,
    ObjectHasBeenSavedError,
    RecordCreationException,
    RecordDeletionException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.models.event_table import EventTableModel
from featurebyte.query_graph.model.dtype import PartitionMetadata
from featurebyte.query_graph.model.feature_job_setting import (
    CronFeatureJobSetting,
    FeatureJobSetting,
)
from featurebyte.query_graph.node.cleaning_operation import (
    AddTimestampSchema,
    DisguisedValueImputation,
    MissingValueImputation,
)
from featurebyte.schema.task import Task, TaskStatus
from tests.unit.api.base_table_test import BaseTableTestSuite, DataType
from tests.util.helper import check_sdk_code_generation, compare_pydantic_obj


@pytest.fixture(name="event_table_dict")
def event_table_dict_fixture(snowflake_database_table, user_id):
    """EventTable in serialized dictionary format"""
    return {
        "type": "event_table",
        "name": "sf_event_table",
        "description": "Some description",
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
                "description": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "entity_id": None,
                "name": "col_float",
                "dtype": "FLOAT",
                "semantic_id": None,
                "critical_data_info": None,
                "description": "Float column",
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "entity_id": None,
                "name": "col_char",
                "dtype": "CHAR",
                "semantic_id": None,
                "critical_data_info": None,
                "description": "Char column",
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "entity_id": None,
                "name": "col_text",
                "dtype": "VARCHAR",
                "semantic_id": None,
                "critical_data_info": None,
                "description": "Text column",
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "entity_id": None,
                "name": "col_binary",
                "dtype": "BINARY",
                "semantic_id": None,
                "critical_data_info": None,
                "description": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "entity_id": None,
                "name": "col_boolean",
                "dtype": "BOOL",
                "semantic_id": None,
                "critical_data_info": None,
                "description": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "entity_id": None,
                "name": "event_timestamp",
                "dtype": "TIMESTAMP_TZ",
                "semantic_id": None,
                "critical_data_info": None,
                "description": "Timestamp column",
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "entity_id": None,
                "name": "created_at",
                "dtype": "TIMESTAMP_TZ",
                "semantic_id": None,
                "critical_data_info": None,
                "description": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "entity_id": None,
                "name": "cust_id",
                "dtype": "INT",
                "semantic_id": None,
                "critical_data_info": None,
                "description": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
        ],
        "event_timestamp_column": "event_timestamp",
        "event_id_column": "col_int",
        "record_creation_timestamp_column": "created_at",
        "default_feature_job_setting": None,
        "created_at": None,
        "updated_at": None,
        "user_id": user_id,
        "event_timestamp_timezone_offset": None,
        "event_timestamp_timezone_offset_column": None,
        "event_timestamp_schema": None,
        "is_deleted": False,
    }


def test_create_event_table(snowflake_database_table, event_table_dict, catalog):
    """
    Test EventTable creation using tabular source
    """
    _ = catalog

    event_table = snowflake_database_table.create_event_table(
        name="sf_event_table",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
        record_creation_timestamp_column="created_at",
        description="Some description",
    )

    # check that node parameter is set properly
    node_params = event_table.frame.node.parameters
    assert node_params.id == event_table.id
    assert node_params.type == TableDataType.EVENT_TABLE

    # check that event table columns for autocompletion
    assert set(event_table.columns).issubset(dir(event_table))
    assert event_table._ipython_key_completions_() == set(event_table.columns)

    output = event_table.model_dump(by_alias=True)
    event_table_dict["_id"] = event_table.id
    event_table_dict["created_at"] = event_table.created_at
    event_table_dict["updated_at"] = event_table.updated_at
    event_table_dict["block_modification_by"] = []
    for column_idx in [0, 6, 7]:
        event_table_dict["columns_info"][column_idx]["semantic_id"] = event_table.columns_info[
            column_idx
        ].semantic_id
    assert output == event_table_dict

    # user input validation
    with pytest.raises(TypeCheckError) as exc:
        snowflake_database_table.create_event_table(
            name=123,
            event_id_column="col_int",
            event_timestamp_column=234,
            record_creation_timestamp_column=345,
        )
    assert 'argument "name" (int) is not an instance of str' in str(exc.value)


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
    event_table = EventTable.model_validate(event_table_dict)
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
        EventTable.model_validate(event_table_dict)
    assert 'Column "some_random_name" not found in the table!' in str(exc.value)

    event_table_dict["record_creation_timestamp_column"] = "created_at"
    event_table_dict["event_timestamp_column"] = "some_timestamp_column"
    with pytest.raises(ValueError) as exc:
        EventTable.model_validate(event_table_dict)
    assert 'Column "some_timestamp_column" not found in the table!' in str(exc.value)


class TestEventTableTestSuite(BaseTableTestSuite):
    """Test EventTable"""

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
    expected_table_sql = """
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      CAST("col_text" AS VARCHAR) AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("event_timestamp" AS VARCHAR) AS "event_timestamp",
      CAST("created_at" AS VARCHAR) AS "created_at",
      "cust_id" AS "cust_id"
    FROM "sf_database"."sf_schema"."sf_table"
    LIMIT 10
    """
    expected_table_column_sql = """
    SELECT
      "col_int" AS "col_int"
    FROM "sf_database"."sf_schema"."sf_table"
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
      CAST("event_timestamp" AS VARCHAR) AS "event_timestamp",
      CAST("created_at" AS VARCHAR) AS "created_at",
      "cust_id" AS "cust_id"
    FROM "sf_database"."sf_schema"."sf_table"
    LIMIT 10
    """
    expected_clean_table_column_sql = """
    SELECT
      CAST(CASE WHEN (
        "col_int" IS NULL
      ) THEN 0 ELSE "col_int" END AS BIGINT) AS "col_int"
    FROM "sf_database"."sf_schema"."sf_table"
    LIMIT 10
    """
    expected_timestamp_column = "event_timestamp"
    expected_special_columns = ["event_timestamp", "col_int", "created_at"]

    def test_delete(self, table_under_test):
        """Test delete"""
        with pytest.raises(RecordDeletionException) as exc:
            table_under_test.delete()
        assert "EventTable is referenced by ItemTable: sf_item_table" in str(exc.value)


def test_info__event_table_without_record_creation_date(
    snowflake_database_table_dimension_table, catalog
):
    """Test info on event table with record creation timestamp is None"""
    _ = catalog

    event_table = snowflake_database_table_dimension_table.create_event_table(
        name="sf_event_table",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
    )

    # make sure .info() can be executed without throwing any error
    _ = event_table.info()


def test_info__cron_default_feature_job_setting(event_table_with_cron_feature_job_setting):
    """
    Test info on event table with cron default feature job setting
    """
    event_table = event_table_with_cron_feature_job_setting
    info = event_table.info()
    assert "crontab" in info["default_feature_job_setting"]


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
        "status": "PUBLIC_DRAFT",
        "entities": [{"name": "customer", "serving_names": ["cust_id"], "catalog_name": "catalog"}],
        "column_count": 9,
        "table_details": {
            "database_name": "sf_database",
            "schema_name": "sf_schema",
            "table_name": "sf_table",
        },
        "catalog_name": "catalog",
    }
    assert info_dict.items() > expected_info.items(), info_dict
    assert info_dict["updated_at"] is not None, info_dict["updated_at"]
    assert "created_at" in info_dict, info_dict

    # update critical data info
    saved_event_table.col_int.update_critical_data_info(
        cleaning_operations=[MissingValueImputation(imputed_value=0)]
    )

    # update column description
    saved_event_table.col_int.update_description("new description")
    assert saved_event_table.col_int.description == "new description"

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
            "description": "new description",
        },
        {
            "name": "col_float",
            "dtype": "FLOAT",
            "entity": None,
            "semantic": None,
            "critical_data_info": None,
            "description": "Float column",
        },
        {
            "name": "col_char",
            "dtype": "CHAR",
            "entity": None,
            "semantic": None,
            "critical_data_info": None,
            "description": "Char column",
        },
        {
            "name": "col_text",
            "dtype": "VARCHAR",
            "entity": None,
            "semantic": None,
            "critical_data_info": None,
            "description": "Text column",
        },
        {
            "name": "col_binary",
            "dtype": "BINARY",
            "entity": None,
            "semantic": None,
            "critical_data_info": None,
            "description": None,
        },
        {
            "name": "col_boolean",
            "dtype": "BOOL",
            "entity": None,
            "semantic": None,
            "critical_data_info": None,
            "description": None,
        },
        {
            "name": "event_timestamp",
            "dtype": "TIMESTAMP_TZ",
            "entity": None,
            "semantic": "event_timestamp",
            "critical_data_info": None,
            "description": "Timestamp column",
        },
        {
            "name": "created_at",
            "dtype": "TIMESTAMP_TZ",
            "entity": None,
            "semantic": "record_creation_timestamp",
            "critical_data_info": None,
            "description": None,
        },
        {
            "name": "cust_id",
            "dtype": "INT",
            "entity": "customer",
            "semantic": None,
            "critical_data_info": None,
            "description": None,
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


def test_event_table__record_creation_exception(
    snowflake_database_table, snowflake_event_table_id, catalog
):
    """
    Test save event table failure due to conflict
    """
    # check unhandled response status code
    _ = catalog
    with pytest.raises(RecordCreationException):
        with patch("featurebyte.api.savable_api_object.Configurations"):
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
        feature_job_setting=FeatureJobSetting(blind_spot="1m30s", period="6m", offset="3m")
    )
    assert saved_event_table.saved is True

    # check updated feature job settings stored at the persistent & memory
    assert saved_event_table.default_feature_job_setting == FeatureJobSetting(
        blind_spot="1m30s", period="6m", offset="3m"
    )
    client = config.get_client()
    response = client.get(url=f"/event_table/{saved_event_table.id}")
    assert response.status_code == 200
    assert response.json()["default_feature_job_setting"] == {
        "blind_spot": "90s",
        "period": "360s",
        "offset": "180s",
        "execution_buffer": "0s",
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
        blind_spot="0", period="24h", offset="0"
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
            "event_table_candidate": None,
            "analysis_date": None,
            "analysis_length": 2419200,
            "min_featurejob_period": 3600,
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
def test_update_default_feature_job_setting__using_feature_job_analysis_high_period(
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
                feature_job_setting=FeatureJobSetting(blind_spot="1m", period="2m", offset="1m")
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


def test_update_default_job_setting__cron_feature_job_setting(
    event_table_with_cron_feature_job_setting, config, mock_api_object_cache
):
    """
    Test update default job setting using CronFeatureJobSetting
    """
    _ = mock_api_object_cache

    event_table = event_table_with_cron_feature_job_setting
    assert event_table.default_feature_job_setting == CronFeatureJobSetting(
        crontab="0 0 * * *",
        reference_timezone="Etc/UTC",
        blind_spot="600s",
    )
    client = config.get_client()
    response = client.get(url=f"/event_table/{event_table.id}")
    assert response.status_code == 200
    assert response.json()["default_feature_job_setting"] == {
        "crontab": {
            "minute": 0,
            "hour": 0,
            "day_of_month": "*",
            "month_of_year": "*",
            "day_of_week": "*",
        },
        "timezone": "Etc/UTC",
        "reference_timezone": "Etc/UTC",
        "blind_spot": "600s",
    }


def test_update_default_job_setting__cron_feature_job_setting_invalid(
    saved_event_table, config, mock_api_object_cache
):
    """
    Test update default job setting using CronFeatureJobSetting
    """
    _ = mock_api_object_cache
    with pytest.raises(RecordUpdateException) as exc:
        saved_event_table.update_default_feature_job_setting(
            feature_job_setting=CronFeatureJobSetting(
                crontab="0 0 1 * *", reference_timezone="Etc/UTC", blind_spot="600s"
            )
        )
    assert (
        str(exc.value)
        == "The provided CronFeatureJobSetting cannot be used as a default feature job setting (cron schedule does not result in a fixed interval)"
    )


@pytest.fixture(name="mock_celery")
def mock_celery_fixture():
    with patch("featurebyte.app.get_celery") as mock_get_celery:
        mock_celery = mock_get_celery.return_value
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


def test_update_record_creation_timestamp_column__unsaved_object(snowflake_database_table, catalog):
    """Test update record creation timestamp column (unsaved event table)"""
    _ = catalog

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
    expected_msg = 'Column "random_column_name" not found in the table!'
    assert expected_msg in str(exc.value)

    with pytest.raises(RecordUpdateException) as exc:
        saved_event_table.update_record_creation_timestamp_column("col_float")
    expected_msg = "Column \"col_float\" is expected to have type(s): ['TIMESTAMP', 'TIMESTAMP_TZ']"
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
        feature_job_setting=FeatureJobSetting(blind_spot="1m30s", period="10m", offset="2m")
    )
    t2 = datetime.utcnow()

    history = saved_event_table.default_feature_job_setting_history
    expected_history_0 = {
        "setting": {
            "blind_spot": "90s",
            "period": "600s",
            "offset": "120s",
            "execution_buffer": "0s",
        }
    }
    assert len(history) == 2
    assert history[0].items() >= expected_history_0.items()
    assert t2 >= datetime.fromisoformat(history[0]["created_at"]) >= t1

    saved_event_table.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(blind_spot="1m", period="5m", offset="2m")
    )
    t3 = datetime.utcnow()

    history = saved_event_table.default_feature_job_setting_history
    expected_history_1 = {
        "setting": {
            "blind_spot": "60s",
            "period": "300s",
            "offset": "120s",
            "execution_buffer": "0s",
        }
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
                "90s",
                "60s",
            ),
            (
                "UPDATE",
                'update: "sf_event_table"',
                "default_feature_job_setting.period",
                "600s",
                "300s",
            ),
            (
                "UPDATE",
                'update: "sf_event_table"',
                "default_feature_job_setting.offset",
                "120s",
                "120s",
            ),
            (
                "UPDATE",
                'update: "sf_event_table"',
                "default_feature_job_setting.execution_buffer",
                "0s",
                "0s",
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
                "90s",
            ),
            (
                "UPDATE",
                'update: "sf_event_table"',
                "default_feature_job_setting.execution_buffer",
                np.nan,
                "0s",
            ),
            (
                "UPDATE",
                'update: "sf_event_table"',
                "default_feature_job_setting.offset",
                np.nan,
                "120s",
            ),
            (
                "UPDATE",
                'update: "sf_event_table"',
                "default_feature_job_setting.period",
                np.nan,
                "600s",
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
        "description",
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
        "event_timestamp_timezone_offset",
        "event_timestamp_timezone_offset_column",
        "event_timestamp_schema",
        "block_modification_by",
        "is_deleted",
        "managed_view_id",
        "validation",
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
            "event_table_candidate": None,
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


def test_event_table__entity_relation_auto_tagging(saved_event_table, mock_api_object_cache):
    """Test event table update: entity relation will be created automatically"""
    _ = mock_api_object_cache

    transaction_entity = Entity(name="transaction", serving_names=["transaction_id"])
    transaction_entity.save()

    customer = Entity(name="customer", serving_names=["customer_id"])
    customer.save()

    # add entities to event table
    assert saved_event_table.event_id_column == "col_int"
    saved_event_table.col_int.as_entity("transaction")
    saved_event_table.cust_id.as_entity("customer")

    updated_transaction_entity = Entity.get_by_id(id=transaction_entity.id)
    compare_pydantic_obj(
        updated_transaction_entity.parents,
        expected=[
            {"id": customer.id, "table_type": "event_table", "table_id": saved_event_table.id}
        ],
    )
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
    feature_job_setting = FeatureJobSetting(blind_spot="1m30s", period="6m", offset="3m")
    cloned = EventTable.get_by_id(id=saved_event_table.id)
    assert cloned.default_feature_job_setting is None
    saved_event_table.update_default_feature_job_setting(feature_job_setting=feature_job_setting)
    assert saved_event_table.default_feature_job_setting == feature_job_setting
    assert cloned.default_feature_job_setting == feature_job_setting


def test_timezone_offset__valid_constant(snowflake_database_table, catalog):
    """Test specifying a constant timezone offset"""
    _ = catalog

    event_table = snowflake_database_table.create_event_table(
        name="sf_event_table",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
        event_timestamp_timezone_offset="+08:00",
    )
    assert event_table.event_timestamp_timezone_offset == "+08:00"

    input_node_params = event_table.frame.node.parameters
    assert input_node_params.event_timestamp_timezone_offset == "+08:00"


def test_timezone_offset__invalid_constant(snowflake_database_table_dimension_table):
    """Test specifying a constant timezone offset"""
    with pytest.raises(ValidationError) as exc:
        snowflake_database_table_dimension_table.create_event_table(
            name="sf_event_table",
            event_id_column="col_int",
            event_timestamp_column="event_timestamp",
            event_timestamp_timezone_offset="8 hours ahead",
        )
    assert "Invalid timezone_offset: 8 hours ahead" in str(exc.value)


def test_timezone_offset__valid_column(snowflake_database_table_dimension_table, catalog):
    """Test specifying a constant timezone offset using a column"""
    _ = catalog

    event_table = snowflake_database_table_dimension_table.create_event_table(
        name="sf_event_table",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
        event_timestamp_timezone_offset_column="col_text",
    )
    assert event_table.event_timestamp_timezone_offset_column == "col_text"

    input_node_params = event_table.frame.node.parameters
    assert input_node_params.event_timestamp_timezone_offset_column == "col_text"


def test_timezone_offset__invalid_column(snowflake_database_table_dimension_table, catalog):
    """Test specifying a constant timezone offset using a column"""
    _ = catalog
    with pytest.raises(RecordCreationException) as exc:
        snowflake_database_table_dimension_table.create_event_table(
            name="sf_event_table",
            event_id_column="col_int",
            event_timestamp_column="event_timestamp",
            event_timestamp_timezone_offset_column="col_float",
        )
    expected = "Column \"col_float\" is expected to have type(s): ['VARCHAR']"
    assert expected in str(exc.value)


def test_create_event_table__duplicated_column_name_in_different_fields(
    snowflake_database_table, catalog
):
    """Test EventTable creation failure due to duplicated column name in different fields"""
    _ = catalog
    with pytest.raises(ValueError) as exc:
        snowflake_database_table.create_event_table(
            name="sf_event_table",
            event_id_column="col_int",
            event_timestamp_column="event_timestamp",
            record_creation_timestamp_column="event_timestamp",
        )

    expected_error_message = (
        "event_timestamp_column and record_creation_timestamp_column have to be different columns in the table but "
        '"event_timestamp" is specified for both.'
    )
    assert expected_error_message in str(exc.value)


def test_sdk_code_generation(saved_event_table, update_fixtures):
    """Check SDK code generation for unsaved table"""
    check_sdk_code_generation(
        saved_event_table.frame,
        to_use_saved_data=False,
        fixture_path="tests/fixtures/sdk_code/event_table.py",
        update_fixtures=update_fixtures,
        table_id=saved_event_table.id,
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


def test_shape(snowflake_event_table, snowflake_query_map):
    """
    Test creating ObservationTable from an EventView
    """

    def side_effect(query, timeout=None, to_log_error=True, query_metadata=None):
        _ = timeout, to_log_error, query_metadata
        res = snowflake_query_map.get(query)
        if res is not None:
            return pd.DataFrame(res)
        return pd.DataFrame({"count": [1000]})

    with mock.patch(
        "featurebyte.session.snowflake.SnowflakeSession.execute_query"
    ) as mock_execute_query:
        mock_execute_query.side_effect = side_effect
        assert snowflake_event_table.shape() == (1000, 9)
        # Check that the correct query was executed
        assert (
            mock_execute_query.call_args[0][0]
            == textwrap.dedent(
                """
                WITH data AS (
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
                SELECT
                  COUNT(*) AS "count"
                FROM data
                """
            ).strip()
        )
        # test table colum shape
        assert snowflake_event_table["col_int"].shape() == (1000, 1)


def test_update_critical_data_info_with_none_value(saved_event_table):
    """Test update critical data info with None value"""
    cleaning_operations = [DisguisedValueImputation(imputed_value=None, disguised_values=[-999])]
    saved_event_table.col_int.update_critical_data_info(cleaning_operations=cleaning_operations)
    assert (
        saved_event_table.col_int.info.critical_data_info.cleaning_operations == cleaning_operations
    )


def test_create_event_table_without_event_id_column(snowflake_database_table, catalog):
    """
    Test EventTable creation using tabular source without event_id_column
    """
    _ = catalog

    event_table = snowflake_database_table.get_or_create_event_table(
        name="sf_event_table",
        event_id_column=None,
        event_timestamp_column="event_timestamp",
        record_creation_timestamp_column="created_at",
        description="Some description",
    )

    # check event table info
    event_table_info = event_table.info()
    assert event_table_info["event_id_column"] is None

    # check that node parameter is set properly
    node_params = event_table.frame.node.parameters
    assert node_params.id == event_table.id
    assert node_params.type == TableDataType.EVENT_TABLE

    output = event_table.model_dump(by_alias=True)
    assert output["event_id_column"] is None

    # expect lookup feature to be unsuccessful
    event_view = event_table.get_view()
    with pytest.raises(AssertionError) as exc:
        event_view.col_text.as_feature("some feature")
    assert "Event ID column is not available." in str(exc.value)

    # expect subset to work
    _ = event_view[["col_text", "col_int"]]


def test_associate_conflicting_dtype_to_different_tables(
    saved_event_table, saved_scd_table, cust_id_entity, mock_api_object_cache
):
    """Test associating conflicting dtype to different tables"""
    # check entity dtype is None
    assert cust_id_entity.dtype is None

    # associate cust_id column to entity
    assert saved_event_table.cust_id.info.dtype == DBVarType.INT
    saved_event_table.cust_id.as_entity(cust_id_entity.name)
    assert cust_id_entity.dtype == DBVarType.INT

    # check exception raised when associating conflicting dtype to different tables
    assert saved_scd_table.col_text.info.dtype == DBVarType.VARCHAR
    with pytest.raises(RecordUpdateException) as exc:
        with patch(
            "featurebyte.service.table_columns_info.TableColumnsInfoService._get_source_type",
            return_value=SourceType.BIGQUERY,
        ):
            saved_scd_table.col_text.as_entity(cust_id_entity.name)
    expected_msg = (
        f"Column col_text (Table ID: {saved_scd_table.id}, Name: sf_scd_table) has dtype VARCHAR which does not "
        f"match the entity dtype INT."
    )
    assert expected_msg in str(exc.value)


def test_add_timestamp_schema(
    saved_event_table, cust_id_entity, arbitrary_default_feature_job_setting, update_fixtures
):
    """Test add timestamp schema to non-special columns"""
    # add entity
    saved_event_table.cust_id.as_entity(cust_id_entity.name)

    # create a semantic
    client = Configurations().get_client()
    response = client.post(url="/semantic", json={"name": "SOME_SEMANTIC"})
    assert response.status_code == 201
    semantic_id = response.json()["_id"]

    # add semantic to the column
    cols_info = saved_event_table.columns_info
    for col_info in cols_info:
        if col_info.name == "cust_id":
            col_info.semantic_id = ObjectId(semantic_id)

    response = client.patch(
        url=f"/event_table/{saved_event_table.id}",
        json={"columns_info": [col_info.json_dict() for col_info in cols_info]},
    )
    assert response.status_code == 200

    # check the semantic is added to the column
    assert saved_event_table.cust_id.info.semantic_id == ObjectId(semantic_id)

    # add non-timestamp schema cleaning operation to the column & semantic ID is not reset
    cleaning_operations = [MissingValueImputation(imputed_value="")]
    saved_event_table.col_text.update_critical_data_info(cleaning_operations=cleaning_operations)
    assert saved_event_table.cust_id.info.semantic_id == ObjectId(semantic_id)

    # add timestamp schema to the column
    cleaning_operations = [
        AddTimestampSchema(
            timestamp_schema=TimestampSchema(
                is_utc_time=False,
                format_string="%Y-%m-%d %H:%M:%S",
                timezone="Asia/Singapore",
            )
        )
    ]
    saved_event_table.col_text.update_critical_data_info(cleaning_operations=cleaning_operations)
    assert (
        saved_event_table.col_text.info.critical_data_info.cleaning_operations
        == cleaning_operations
    )

    # check that semantic ID is reset
    assert saved_event_table.col_text.info.semantic_id is None

    # check that datetime operation can be applied on the column
    view = saved_event_table.get_view()
    view["col_text_day"] = view.col_text.dt.day

    # check preview sql
    expected_preview_sql = """
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      CAST("col_text" AS VARCHAR) AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("event_timestamp" AS VARCHAR) AS "event_timestamp",
      "cust_id" AS "cust_id",
      DATE_PART(day, TO_TIMESTAMP("col_text", '%Y-%m-%d %H:%M:%S')) AS "col_text_day"
    FROM "sf_database"."sf_schema"."sf_table"
    LIMIT 10
    """
    assert view.preview_sql().strip() == textwrap.dedent(expected_preview_sql).strip()

    # construct feature & save
    feat = view.groupby("cust_id").aggregate_over(
        "col_text_day",
        method="max",
        windows=["7d"],
        feature_names=["max_col_text_day"],
        feature_job_setting=arbitrary_default_feature_job_setting,
    )["max_col_text_day"]
    feat.save()

    check_sdk_code_generation(
        feat,
        to_use_saved_data=True,
        to_format=True,
        fixture_path="tests/fixtures/sdk_code/add_timestamp_schema.py",
        update_fixtures=update_fixtures,
        table_id=saved_event_table.id,
    )


def test_add_timestamp_schema_validation(saved_event_table):
    """Test add timestamp schema validation"""
    add_ts_schema_op = AddTimestampSchema(
        timestamp_schema=TimestampSchema(
            is_utc_time=True,
            format_string="%Y-%m-%d %H:%M:%S",
        )
    )
    imputation_op = MissingValueImputation(imputed_value=0)

    # check validation
    with pytest.raises(ValidationError) as exc:
        saved_event_table.col_text.update_critical_data_info(
            cleaning_operations=[add_ts_schema_op, imputation_op]
        )

    expected = "AddTimestampSchema must be the last operation."
    assert expected in str(exc.value)

    # check when the add timestamp schema operation is the last operation, expect no error
    saved_event_table.col_text.update_critical_data_info(
        cleaning_operations=[imputation_op, add_ts_schema_op]
    )

    # check validation check on non-existing timezone offset column
    with pytest.raises(RecordUpdateException) as exc:
        saved_event_table.col_text.update_critical_data_info(
            cleaning_operations=[
                AddTimestampSchema(
                    timestamp_schema=TimestampSchema(
                        is_utc_time=False,
                        format_string="%Y-%m-%d %H:%M:%S",
                        timezone=TimeZoneColumn(column_name="non_existing_column", type="timezone"),
                    )
                )
            ]
        )

    expected = (
        'Timezone column name "non_existing_column" is not found in columns_info: '
        "AddTimestampSchema(timestamp_schema=TimestampSchema("
        "format_string='%Y-%m-%d %H:%M:%S', is_utc_time=False, "
        "timezone=TimeZoneColumn(column_name='non_existing_column', type='timezone')))"
    )
    assert expected in str(exc.value)

    # check validation check on non-supported dtype
    with pytest.raises(RecordUpdateException) as exc:
        saved_event_table.col_int.update_critical_data_info(
            cleaning_operations=[
                AddTimestampSchema(
                    timestamp_schema=TimestampSchema(
                        is_utc_time=False,
                        format_string="%Y-%m-%d %H:%M:%S",
                        timezone=TimeZoneColumn(column_name="non_existing_column", type="timezone"),
                    )
                )
            ]
        )

    expected = (
        "AddTimestampSchema should only be used with supported datetime types: "
        "[DATE, TIMESTAMP, TIMESTAMP_TZ, VARCHAR]. INT is not supported."
    )
    assert expected in str(exc.value)

    with pytest.raises(RecordUpdateException) as exc:
        saved_event_table.col_text.update_critical_data_info(
            cleaning_operations=[
                AddTimestampSchema(
                    timestamp_schema=TimestampSchema(
                        is_utc_time=False,
                        format_string="%Y-%m-%d %H:%M:%S",
                        timezone=TimeZoneColumn(column_name="col_text", type="timezone"),
                    )
                )
            ]
        )

    expected = (
        'Timestamp schema timezone offset column "col_text" cannot be the same as the column name'
    )
    assert expected in str(exc.value)


def test_event_table_with_event_timestamp_schema(snowflake_event_table_with_timestamp_schema):
    """
    Test creating EventTable with event timestamp schema
    """
    event_table = snowflake_event_table_with_timestamp_schema
    assert event_table.event_timestamp_schema.model_dump() == {
        "format_string": None,
        "is_utc_time": False,
        "timezone": {"column_name": "tz_offset", "type": "offset"},
    }


def test_create_with_datetime_partition_column_non_existing_column(snowflake_database_table):
    """
    Test creating EventTable with datetime partition column
    """
    with pytest.raises(ValidationError) as exc:
        # Attempt to create an event table with a non-existing datetime partition column
        # This should raise an exception since the column does not exist in the source table
        snowflake_database_table.create_event_table(
            name="sf_event_table",
            event_id_column="col_int",
            event_timestamp_column="event_timestamp",
            record_creation_timestamp_column="created_at",
            description="Some description",
            datetime_partition_column="non_existing_column",
        )
    assert "Column not found in table: non_existing_column" in str(exc.value)


def test_create_with_datetime_partition_column_missing_format_string(
    snowflake_database_table, catalog
):
    """
    Test creating EventTable with datetime partition column with missing format string
    """
    _ = catalog
    with pytest.raises(ValidationError) as exc:
        # Attempt to create an event table with a datetime partition column with missing format string
        # This should raise an exception since the column is expected to have a format string
        snowflake_database_table.create_event_table(
            name="sf_event_table",
            event_id_column="col_int",
            event_timestamp_column="event_timestamp",
            record_creation_timestamp_column="created_at",
            description="Some description",
            datetime_partition_column="col_text",
        )
    assert "timestamp_schema is required for col_text with ambiguous timestamp type VARCHAR" in str(
        exc.value
    )


def test_create_event_table_partition_column_success(
    snowflake_database_table, event_table_dict, catalog
):
    """
    Test EventTable creation using tabular source
    """
    _ = catalog

    # mark a column as partition key
    snowflake_database_table.columns_info[0].partition_metadata = PartitionMetadata(
        is_partition_key=True,
    )

    event_table = snowflake_database_table.create_event_table(
        name="sf_event_table",
        event_id_column="col_int",
        event_timestamp_column="event_timestamp",
        record_creation_timestamp_column="created_at",
        description="Some description",
        datetime_partition_column="col_text",
        datetime_partition_schema=TimestampSchema(
            format_string="%Y-%m-%d %H:%M:%S",
        ),
    )

    # check that node parameter is set properly
    node_params = event_table.frame.node.parameters
    assert node_params.id == event_table.id
    assert node_params.type == TableDataType.EVENT_TABLE

    # check that event table columns for autocompletion
    assert set(event_table.columns).issubset(dir(event_table))
    assert event_table._ipython_key_completions_() == set(event_table.columns)

    # check partition metadata is set properly
    assert event_table.columns_info[3].partition_metadata.is_partition_key is True
    assert event_table.columns_info[3].dtype_metadata.timestamp_schema == TimestampSchema(
        format_string="%Y-%m-%d %H:%M:%S", is_utc_time=None, timezone=None
    )

    # expect partition metadata to be cleared for other columns
    for col_info in event_table.columns_info:
        if col_info.name != "col_text":
            assert col_info.partition_metadata is None


def test_create_event_table_partition_column_shared_column(
    snowflake_database_table, event_table_dict, catalog
):
    """
    Test EventTable creation specifying partition column with the same column as event timestamp column
    """
    _ = catalog

    event_table = snowflake_database_table.create_event_table(
        name="sf_event_table",
        event_id_column="col_int",
        event_timestamp_column="col_text",
        event_timestamp_schema=TimestampSchema(
            format_string="%Y-%m-%d %H:%M:%S",
        ),
        record_creation_timestamp_column="created_at",
        description="Some description",
        datetime_partition_column="col_text",
        datetime_partition_schema=TimestampSchema(
            format_string="%Y-%m-%d",
        ),
    )

    # expect col_text to be marked as partition key but uses event_timestamp_schema for dtype_metadata
    assert (
        event_table.columns_info[3].dtype_metadata.timestamp_schema.format_string
        == "%Y-%m-%d %H:%M:%S"
    )
    assert event_table.columns_info[3].partition_metadata == PartitionMetadata(
        is_partition_key=True
    )
