"""
Unit test for SnapshotsTable class
"""

from __future__ import annotations

import textwrap
from datetime import datetime
from unittest import mock
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError
from typeguard import TypeCheckError

from featurebyte import SnapshotsTable
from featurebyte.api.entity import Entity
from featurebyte.enum import DBVarType, TableDataType
from featurebyte.exception import (
    DuplicatedRecordException,
    ObjectHasBeenSavedError,
    RecordCreationException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.models.entity import ParentEntity
from featurebyte.models.periodic_task import Crontab
from featurebyte.models.snapshots_table import SnapshotsTableModel
from featurebyte.query_graph.model.dtype import DBVarTypeInfo, DBVarTypeMetadata
from featurebyte.query_graph.model.feature_job_setting import (
    CronFeatureJobSetting,
)
from featurebyte.query_graph.model.time_series_table import TimeInterval
from featurebyte.query_graph.model.timestamp_schema import (
    ExtendedTimestampSchema,
    TimestampSchema,
    TimestampTupleSchema,
    TimeZoneColumn,
    TimezoneOffsetSchema,
)
from featurebyte.query_graph.node.cleaning_operation import (
    AddTimestampSchema,
    DisguisedValueImputation,
    MissingValueImputation,
)
from tests.unit.api.base_table_test import BaseTableTestSuite, DataType
from tests.util.helper import check_sdk_code_generation


@pytest.fixture(name="snapshots_table_dict")
def snapshots_table_dict_fixture(snowflake_database_snapshots_table, user_id):
    """SnapshotsTable in serialized dictionary format"""
    ts_schema = {
        "format_string": "YYYY-MM-DD HH24:MI:SS",
        "timezone": "Etc/UTC",
        "is_utc_time": None,
    }
    return {
        "type": "snapshots_table",
        "name": "sf_snapshots_table",
        "description": "Some description",
        "tabular_source": {
            "feature_store_id": snowflake_database_snapshots_table.feature_store.id,
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "snapshots_table",
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
                "name": "date",
                "dtype": "VARCHAR",
                "semantic_id": None,
                "critical_data_info": None,
                "description": "Date column",
                "dtype_metadata": {"timestamp_schema": ts_schema, "timestamp_tuple_schema": None},
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
                "name": "store_id",
                "dtype": "INT",
                "semantic_id": None,
                "critical_data_info": None,
                "description": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "critical_data_info": None,
                "description": None,
                "dtype": "TIMESTAMP_TZ",
                "dtype_metadata": None,
                "entity_id": None,
                "name": "another_timestamp_col",
                "semantic_id": None,
                "partition_metadata": None,
            },
        ],
        "snapshot_id_column": "col_int",
        "snapshot_datetime_column": "date",
        "snapshot_datetime_schema": ts_schema,
        "time_interval": {"unit": "DAY", "value": 1},
        "record_creation_timestamp_column": "created_at",
        "default_feature_job_setting": None,
        "created_at": None,
        "updated_at": None,
        "user_id": user_id,
        "is_deleted": False,
    }


def test_create_snapshots_table(snowflake_database_snapshots_table, snapshots_table_dict, catalog):
    """
    Test SnapshotsTable creation using tabular source
    """
    _ = catalog

    snapshots_table = snowflake_database_snapshots_table.create_snapshots_table(
        name="sf_snapshots_table",
        snapshot_id_column="col_int",
        snapshot_datetime_column="date",
        snapshot_datetime_schema=TimestampSchema(
            format_string="YYYY-MM-DD HH24:MI:SS", timezone="Etc/UTC"
        ),
        time_interval=TimeInterval(value=1, unit="DAY"),
        record_creation_timestamp_column="created_at",
        description="Some description",
    )

    # check that node parameter is set properly
    node_params = snapshots_table.frame.node.parameters
    assert node_params.id == snapshots_table.id
    assert node_params.type == TableDataType.SNAPSHOTS_TABLE

    # check that snapshots table columns for autocompletion
    assert set(snapshots_table.columns).issubset(dir(snapshots_table))
    assert snapshots_table._ipython_key_completions_() == set(snapshots_table.columns)

    output = snapshots_table.model_dump(by_alias=True)
    snapshots_table_dict["_id"] = snapshots_table.id
    snapshots_table_dict["created_at"] = snapshots_table.created_at
    snapshots_table_dict["updated_at"] = snapshots_table.updated_at
    snapshots_table_dict["block_modification_by"] = []
    for column_idx in [0, 6, 7]:
        snapshots_table_dict["columns_info"][column_idx]["semantic_id"] = (
            snapshots_table.columns_info[column_idx].semantic_id
        )
    assert output == snapshots_table_dict

    # user input validation
    with pytest.raises(TypeCheckError) as exc:
        snowflake_database_snapshots_table.create_snapshots_table(
            name=123,
            snapshot_id_column="col_int",
            snapshot_datetime_column=234,
            snapshot_datetime_schema=TimestampSchema(
                format_string="YYYY-MM-DD HH24:MI:SS", timezone="Etc/UTC"
            ),
            time_interval=TimeInterval(value=1, unit="DAY"),
            record_creation_timestamp_column=345,
        )
    assert 'argument "name" (int) is not an instance of str' in str(exc.value)


def test_create_snapshots_table__duplicated_record(
    saved_snapshots_table, snowflake_database_snapshots_table
):
    """
    Test SnapshotsTable creation failure due to duplicated snapshots table name
    """
    _ = saved_snapshots_table
    with pytest.raises(DuplicatedRecordException) as exc:
        snowflake_database_snapshots_table.create_snapshots_table(
            name="sf_snapshots_table",
            snapshot_id_column="col_int",
            snapshot_datetime_column="date",
            snapshot_datetime_schema=TimestampSchema(
                format_string="YYYY-MM-DD HH24:MI:SS", timezone="Etc/UTC"
            ),
            time_interval=TimeInterval(value=1, unit="DAY"),
            record_creation_timestamp_column="created_at",
        )
    assert (
        'SnapshotsTable (snapshots_table.name: "sf_snapshots_table") exists in saved record.'
        in str(exc.value)
    )


def test_create_snapshots_table__retrieval_exception(snowflake_database_snapshots_table):
    """
    Test SnapshotsTable creation failure due to retrieval exception
    """
    with pytest.raises(RecordRetrievalException):
        with patch("featurebyte.api.base_table.Configurations"):
            snowflake_database_snapshots_table.create_snapshots_table(
                name="sf_snapshots_table",
                snapshot_id_column="col_int",
                snapshot_datetime_column="date",
                snapshot_datetime_schema=TimestampSchema(
                    format_string="YYYY-MM-DD HH24:MI:SS", timezone="Etc/UTC"
                ),
                time_interval=TimeInterval(value=1, unit="DAY"),
                record_creation_timestamp_column="created_at",
            )


def test_deserialization(
    snapshots_table_dict,
    snowflake_feature_store,
    snowflake_execute_query,
    expected_snapshots_table_preview_query,
):
    """
    Test deserialize snapshots table dictionary
    """
    _ = snowflake_execute_query
    # setup proper configuration to deserialize the snapshots table object
    snapshots_table_dict["feature_store"] = snowflake_feature_store
    snapshots_table = SnapshotsTable.model_validate(snapshots_table_dict)
    assert snapshots_table.preview_sql() == expected_snapshots_table_preview_query


def test_deserialization__column_name_not_found(
    snapshots_table_dict, snowflake_feature_store, snowflake_execute_query
):
    """
    Test column not found during deserialize snapshots table
    """
    _ = snowflake_execute_query
    snapshots_table_dict["feature_store"] = snowflake_feature_store
    snapshots_table_dict["record_creation_timestamp_column"] = "some_random_name"
    with pytest.raises(ValueError) as exc:
        SnapshotsTable.model_validate(snapshots_table_dict)
    assert 'Column "some_random_name" not found in the table!' in str(exc.value)

    snapshots_table_dict["record_creation_timestamp_column"] = "created_at"
    snapshots_table_dict["snapshot_datetime_column"] = "some_timestamp_column"
    with pytest.raises(ValueError) as exc:
        SnapshotsTable.model_validate(snapshots_table_dict)
    assert 'Column "some_timestamp_column" not found in the table!' in str(exc.value)


class TestSnapshotsTableTestSuite(BaseTableTestSuite):
    """Test SnapshotsTable"""

    data_type = DataType.SNAPSHOTS_DATA
    col = "col_int"
    expected_columns = {
        "col_char",
        "col_float",
        "col_boolean",
        "date",
        "col_text",
        "created_at",
        "col_binary",
        "col_int",
        "store_id",
        "another_timestamp_col",
    }
    expected_table_sql = """
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      CAST("col_text" AS VARCHAR) AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("date" AS VARCHAR) AS "date",
      CAST("created_at" AS VARCHAR) AS "created_at",
      "store_id" AS "store_id",
      CAST("another_timestamp_col" AS VARCHAR) AS "another_timestamp_col"
    FROM "sf_database"."sf_schema"."snapshots_table"
    LIMIT 10
    """
    expected_table_column_sql = """
    SELECT
      "col_int" AS "col_int"
    FROM "sf_database"."sf_schema"."snapshots_table"
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
      CAST("date" AS VARCHAR) AS "date",
      CAST("created_at" AS VARCHAR) AS "created_at",
      "store_id" AS "store_id",
      CAST("another_timestamp_col" AS VARCHAR) AS "another_timestamp_col"
    FROM "sf_database"."sf_schema"."snapshots_table"
    LIMIT 10
    """
    expected_clean_table_column_sql = """
    SELECT
      CAST(CASE WHEN (
        "col_int" IS NULL
      ) THEN 0 ELSE "col_int" END AS BIGINT) AS "col_int"
    FROM "sf_database"."sf_schema"."snapshots_table"
    LIMIT 10
    """
    expected_timestamp_column = "date"
    expected_special_columns = ["date", "col_int", "created_at"]


def test_info__snapshots_table_without_record_creation_date(
    snowflake_database_snapshots_table, catalog
):
    """Test info on snapshots table with record creation timestamp is None"""
    _ = catalog

    snapshots_table = snowflake_database_snapshots_table.create_snapshots_table(
        name="sf_snapshots_table",
        snapshot_id_column="col_int",
        snapshot_datetime_column="date",
        snapshot_datetime_schema=TimestampSchema(
            format_string="YYYY-MM-DD HH24:MI:SS", timezone="Etc/UTC"
        ),
        time_interval=TimeInterval(value=1, unit="DAY"),
    )

    # make sure .info() can be executed without throwing any error
    _ = snapshots_table.info()


def test_info(saved_snapshots_table, cust_id_entity):
    """
    Test info
    """
    _ = cust_id_entity
    saved_snapshots_table.store_id.as_entity("customer")
    info_dict = saved_snapshots_table.info()
    expected_info = {
        "name": "sf_snapshots_table",
        "snapshot_datetime_column": "date",
        "record_creation_timestamp_column": "created_at",
        "default_feature_job_setting": None,
        "status": "PUBLIC_DRAFT",
        "entities": [{"name": "customer", "serving_names": ["cust_id"], "catalog_name": "catalog"}],
        "column_count": 10,
        "table_details": {
            "database_name": "sf_database",
            "schema_name": "sf_schema",
            "table_name": "snapshots_table",
        },
        "catalog_name": "catalog",
    }
    assert info_dict.items() > expected_info.items(), info_dict
    assert info_dict["updated_at"] is not None, info_dict["updated_at"]
    assert "created_at" in info_dict, info_dict

    # update critical data info
    saved_snapshots_table.col_int.update_critical_data_info(
        cleaning_operations=[MissingValueImputation(imputed_value=0)]
    )

    # update column description
    saved_snapshots_table.col_int.update_description("new description")
    assert saved_snapshots_table.col_int.description == "new description"

    verbose_info_dict = saved_snapshots_table.info(verbose=True)
    assert verbose_info_dict.items() > expected_info.items(), info_dict
    assert verbose_info_dict["updated_at"] is not None, verbose_info_dict["updated_at"]
    assert "created_at" in verbose_info_dict, verbose_info_dict
    assert verbose_info_dict["columns_info"] == [
        {
            "name": "col_int",
            "dtype": "INT",
            "entity": None,
            "semantic": "snapshot_id",
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
            "name": "date",
            "dtype": "VARCHAR",
            "entity": None,
            "semantic": "snapshot_date_time",
            "critical_data_info": None,
            "description": "Date column",
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
            "name": "store_id",
            "dtype": "INT",
            "entity": "customer",
            "semantic": None,
            "critical_data_info": None,
            "description": None,
        },
        {
            "critical_data_info": None,
            "description": None,
            "dtype": "TIMESTAMP_TZ",
            "entity": None,
            "name": "another_timestamp_col",
            "semantic": None,
        },
    ]


def test_snapshots_table__save__exceptions(saved_snapshots_table):
    """
    Test save snapshots table failure due to conflict
    """
    # test duplicated record exception when record exists
    with pytest.raises(ObjectHasBeenSavedError) as exc:
        saved_snapshots_table.save()
    expected_msg = f'SnapshotsTable (id: "{saved_snapshots_table.id}") has been saved before.'
    assert expected_msg in str(exc.value)


def test_snapshots_table__record_creation_exception(
    snowflake_database_snapshots_table, snowflake_snapshots_table_id, catalog
):
    """
    Test save snapshots table failure due to conflict
    """
    # check unhandled response status code
    _ = catalog
    with pytest.raises(RecordCreationException):
        with patch("featurebyte.api.savable_api_object.Configurations"):
            snowflake_database_snapshots_table.create_snapshots_table(
                name="sf_snapshots_table",
                snapshot_id_column="col_int",
                snapshot_datetime_column="date",
                snapshot_datetime_schema=TimestampSchema(
                    format_string="YYYY-MM-DD HH24:MI:SS", timezone="Etc/UTC"
                ),
                time_interval=TimeInterval(value=1, unit="DAY"),
                record_creation_timestamp_column="created_at",
                _id=snowflake_snapshots_table_id,
            )


def test_update_default_job_setting__saved_snapshots_table(
    saved_snapshots_table, config, mock_api_object_cache
):
    """
    Test update default job setting on saved snapshots table
    """
    _ = mock_api_object_cache

    assert saved_snapshots_table.default_feature_job_setting is None
    saved_snapshots_table.update_default_feature_job_setting(
        feature_job_setting=CronFeatureJobSetting(
            crontab=Crontab(
                minute=0,
                hour=1,
                day_of_week="*",
                day_of_month="*",
                month_of_year="*",
            ),
            timezone="Etc/UTC",
        )
    )
    assert saved_snapshots_table.saved is True

    # check updated feature job settings stored at the persistent & memory
    assert saved_snapshots_table.default_feature_job_setting == CronFeatureJobSetting(
        crontab=Crontab(
            minute=0,
            hour=1,
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
        ),
        timezone="Etc/UTC",
        reference_timezone="Etc/UTC",
    )
    client = config.get_client()
    response = client.get(url=f"/snapshots_table/{saved_snapshots_table.id}")
    assert response.status_code == 200
    assert response.json()["default_feature_job_setting"] == {
        "crontab": {
            "minute": 0,
            "hour": 1,
            "day_of_week": "*",
            "day_of_month": "*",
            "month_of_year": "*",
        },
        "timezone": "Etc/UTC",
        "reference_timezone": "Etc/UTC",
        "blind_spot": None,
    }


def test_update_default_job_setting__record_update_exception(snowflake_snapshots_table):
    """
    Test unexpected exception during record update
    """
    with pytest.raises(RecordUpdateException):
        with patch("featurebyte.api.api_object.Configurations"):
            snowflake_snapshots_table.update_default_feature_job_setting(
                feature_job_setting=CronFeatureJobSetting(
                    crontab=Crontab(
                        minute=0,
                        hour=1,
                        day_of_week="*",
                        day_of_month="*",
                        month_of_year="*",
                    ),
                    timezone="Etc/UTC",
                )
            )

    with pytest.raises(RecordUpdateException) as exc:
        snowflake_snapshots_table.update_default_feature_job_setting(
            feature_job_setting=CronFeatureJobSetting(
                crontab=Crontab(
                    minute=0,
                    hour=1,
                    day_of_week="*",
                    day_of_month="*",
                    month_of_year="*",
                ),
                timezone="Etc/UTC",
                reference_timezone="Asia/Singapore",
            )
        )

    expected_msg = (
        "Cannot update default feature job setting reference timezone to Asia/Singapore as it is different "
        "from the timezone of the reference datetime column (Etc/UTC)."
    )
    assert expected_msg in str(exc.value)


def test_update_record_creation_timestamp_column__unsaved_object(
    snowflake_database_snapshots_table, catalog
):
    """Test update record creation timestamp column (unsaved snapshots table)"""
    _ = catalog

    snapshots_table = snowflake_database_snapshots_table.create_snapshots_table(
        name="snapshots_table",
        snapshot_id_column="col_int",
        snapshot_datetime_column="date",
        snapshot_datetime_schema=TimestampSchema(
            format_string="YYYY-MM-DD HH24:MI:SS", timezone="Etc/UTC"
        ),
        time_interval=TimeInterval(value=1, unit="DAY"),
    )
    assert snapshots_table.record_creation_timestamp_column is None
    snapshots_table.update_record_creation_timestamp_column("created_at")
    assert snapshots_table.record_creation_timestamp_column == "created_at"


def test_update_record_creation_timestamp_column__saved_object(saved_snapshots_table):
    """Test update record creation timestamp column (saved snapshots table)"""
    saved_snapshots_table.update_record_creation_timestamp_column("created_at")
    assert saved_snapshots_table.record_creation_timestamp_column == "created_at"

    # check that validation logic works
    with pytest.raises(RecordUpdateException) as exc:
        saved_snapshots_table.update_record_creation_timestamp_column("random_column_name")
    expected_msg = 'Column "random_column_name" not found in the table!'
    assert expected_msg in str(exc.value)

    with pytest.raises(RecordUpdateException) as exc:
        saved_snapshots_table.update_record_creation_timestamp_column("col_float")
    expected_msg = "Column \"col_float\" is expected to have type(s): ['TIMESTAMP', 'TIMESTAMP_TZ']"
    assert expected_msg in str(exc.value)


def test_get_snapshots_table(snowflake_snapshots_table, mock_config_path_env):
    """
    Test SnapshotsTable.get function
    """
    _ = mock_config_path_env

    # load the snapshots table from the persistent
    loaded_snapshots_table = SnapshotsTable.get(snowflake_snapshots_table.name)
    assert loaded_snapshots_table.saved is True
    assert loaded_snapshots_table == snowflake_snapshots_table
    assert SnapshotsTable.get_by_id(id=snowflake_snapshots_table.id) == snowflake_snapshots_table

    with pytest.raises(RecordRetrievalException) as exc:
        SnapshotsTable.get("unknown_snapshots_table")

    expected_msg = (
        'SnapshotsTable (name: "unknown_snapshots_table") not found. '
        "Please save the SnapshotsTable object first."
    )
    assert expected_msg in str(exc.value)


def test_default_feature_job_setting_history(saved_snapshots_table):
    """
    Test default_feature_job_setting_history on saved snapshots table
    """
    assert saved_snapshots_table.default_feature_job_setting is None
    setting_history = saved_snapshots_table.default_feature_job_setting_history
    assert len(setting_history) == 1
    assert setting_history[0].items() > {"setting": None}.items()
    t1 = datetime.utcnow()
    saved_snapshots_table.update_default_feature_job_setting(
        feature_job_setting=CronFeatureJobSetting(
            crontab=Crontab(
                minute=0,
                hour=1,
                day_of_week="*",
                day_of_month="*",
                month_of_year="*",
            ),
            timezone="Etc/UTC",
        )
    )
    t2 = datetime.utcnow()

    history = saved_snapshots_table.default_feature_job_setting_history
    expected_history_0 = {
        "setting": {
            "crontab": {
                "minute": 0,
                "hour": 1,
                "day_of_week": "*",
                "day_of_month": "*",
                "month_of_year": "*",
            },
            "timezone": "Etc/UTC",
            "reference_timezone": "Etc/UTC",
            "blind_spot": None,
        }
    }
    assert len(history) == 2
    assert history[0].items() >= expected_history_0.items()
    assert t2 >= datetime.fromisoformat(history[0]["created_at"]) >= t1

    saved_snapshots_table.update_default_feature_job_setting(
        feature_job_setting=CronFeatureJobSetting(
            crontab=Crontab(
                minute=0,
                hour=2,
                day_of_week="*",
                day_of_month="*",
                month_of_year="*",
            ),
            timezone="Etc/UTC",
            reference_timezone=None,
        )
    )
    t3 = datetime.utcnow()

    history = saved_snapshots_table.default_feature_job_setting_history
    expected_history_1 = {
        "setting": {
            "crontab": {
                "minute": 0,
                "hour": 2,
                "day_of_week": "*",
                "day_of_month": "*",
                "month_of_year": "*",
            },
            "timezone": "Etc/UTC",
            "reference_timezone": "Etc/UTC",
            "blind_spot": None,
        }
    }
    assert len(history) == 3
    assert history[1].items() >= expected_history_0.items()
    assert history[0].items() >= expected_history_1.items()
    assert t3 >= datetime.fromisoformat(history[0]["created_at"]) >= t2

    # check audit history
    audit_history = saved_snapshots_table.audit()

    # check number of actions
    expected_unique_records = pd.DataFrame(
        [
            ("UPDATE", 'update: "sf_snapshots_table"'),
            ("UPDATE", 'update: "sf_snapshots_table"'),
            ("UPDATE", 'update: "sf_snapshots_table"'),
            ("UPDATE", 'update: "sf_snapshots_table"'),
            ("INSERT", 'insert: "sf_snapshots_table"'),
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
                'update: "sf_snapshots_table"',
                "updated_at",
                old_updated_at,
                new_updated_at,
            ),
            (
                "UPDATE",
                'update: "sf_snapshots_table"',
                "default_feature_job_setting.crontab.minute",
                0,
                0,
            ),
            (
                "UPDATE",
                'update: "sf_snapshots_table"',
                "default_feature_job_setting.crontab.hour",
                1,
                2,
            ),
            (
                "UPDATE",
                'update: "sf_snapshots_table"',
                "default_feature_job_setting.crontab.day_of_month",
                "*",
                "*",
            ),
            (
                "UPDATE",
                'update: "sf_snapshots_table"',
                "default_feature_job_setting.crontab.month_of_year",
                "*",
                "*",
            ),
            (
                "UPDATE",
                'update: "sf_snapshots_table"',
                "default_feature_job_setting.crontab.day_of_week",
                "*",
                "*",
            ),
            (
                "UPDATE",
                'update: "sf_snapshots_table"',
                "default_feature_job_setting.timezone",
                "Etc/UTC",
                "Etc/UTC",
            ),
            (
                "UPDATE",
                'update: "sf_snapshots_table"',
                "default_feature_job_setting.reference_timezone",
                "Etc/UTC",
                "Etc/UTC",
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
            (
                "UPDATE",
                'update: "sf_snapshots_table"',
                "default_feature_job_setting",
                None,
                np.nan,
            ),
            (
                "UPDATE",
                'update: "sf_snapshots_table"',
                "default_feature_job_setting.crontab.day_of_month",
                np.nan,
                "*",
            ),
            (
                "UPDATE",
                'update: "sf_snapshots_table"',
                "default_feature_job_setting.crontab.day_of_week",
                np.nan,
                "*",
            ),
            (
                "UPDATE",
                'update: "sf_snapshots_table"',
                "default_feature_job_setting.crontab.hour",
                np.nan,
                1,
            ),
            (
                "UPDATE",
                'update: "sf_snapshots_table"',
                "default_feature_job_setting.crontab.minute",
                np.nan,
                0,
            ),
            (
                "UPDATE",
                'update: "sf_snapshots_table"',
                "default_feature_job_setting.crontab.month_of_year",
                np.nan,
                "*",
            ),
            (
                "UPDATE",
                'update: "sf_snapshots_table"',
                "default_feature_job_setting.reference_timezone",
                np.nan,
                "Etc/UTC",
            ),
            (
                "UPDATE",
                'update: "sf_snapshots_table"',
                "default_feature_job_setting.timezone",
                np.nan,
                "Etc/UTC",
            ),
            (
                "UPDATE",
                'update: "sf_snapshots_table"',
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
        "block_modification_by",
        "is_deleted",
        "managed_view_id",
        "validation",
        "snapshot_id_column",
        "snapshot_datetime_column",
        "snapshot_datetime_schema.is_utc_time",
        "snapshot_datetime_schema.format_string",
        "snapshot_datetime_schema.timezone",
        "time_interval.unit",
        "time_interval.value",
    }


def test_snapshots_table__entity_relation_auto_tagging(
    saved_snapshots_table, mock_api_object_cache
):
    """Test snapshots table update: entity relation will be created automatically"""
    _ = mock_api_object_cache

    transaction_entity = Entity(name="transaction", serving_names=["transaction_id"])
    transaction_entity.save()

    customer = Entity(name="customer", serving_names=["customer_id"])
    customer.save()

    # add entities to snapshots table
    assert saved_snapshots_table.snapshot_id_column == "col_int"
    saved_snapshots_table.col_int.as_entity("transaction")
    saved_snapshots_table.store_id.as_entity("customer")

    updated_transaction_entity = Entity.get_by_id(id=transaction_entity.id)
    assert updated_transaction_entity.parents == [
        ParentEntity(
            id=customer.id, table_type="snapshots_table", table_id=saved_snapshots_table.id
        )
    ]
    updated_customer_entity = Entity.get_by_id(id=customer.id)
    assert updated_customer_entity.parents == []

    # remove primary id column's entity
    saved_snapshots_table.col_int.as_entity(None)
    updated_transaction_entity = Entity.get_by_id(id=transaction_entity.id)
    assert updated_transaction_entity.parents == []


def test_accessing_snapshots_table_attributes(snowflake_snapshots_table):
    """Test accessing snapshots table object attributes"""
    assert snowflake_snapshots_table.saved
    assert snowflake_snapshots_table.record_creation_timestamp_column == "created_at"
    assert snowflake_snapshots_table.default_feature_job_setting is None
    assert snowflake_snapshots_table.snapshot_datetime_column == "date"
    assert snowflake_snapshots_table.snapshot_id_column == "col_int"
    assert snowflake_snapshots_table.timestamp_column == "date"


def test_accessing_saved_snapshots_table_attributes(saved_snapshots_table):
    """Test accessing snapshots table object attributes"""
    assert saved_snapshots_table.saved
    assert isinstance(saved_snapshots_table.cached_model, SnapshotsTableModel)
    assert saved_snapshots_table.record_creation_timestamp_column == "created_at"
    assert saved_snapshots_table.default_feature_job_setting is None
    assert saved_snapshots_table.snapshot_datetime_column == "date"
    assert saved_snapshots_table.snapshot_id_column == "col_int"
    assert saved_snapshots_table.timestamp_column == "date"

    # check synchronization
    feature_job_setting = CronFeatureJobSetting(
        crontab=Crontab(
            minute=0,
            hour=1,
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
        ),
        timezone="Etc/UTC",
    )
    cloned = SnapshotsTable.get_by_id(id=saved_snapshots_table.id)
    assert cloned.default_feature_job_setting is None
    saved_snapshots_table.update_default_feature_job_setting(
        feature_job_setting=feature_job_setting
    )
    effective_feature_job_setting = CronFeatureJobSetting(
        **feature_job_setting.model_dump(exclude={"reference_timezone"}),
        reference_timezone="Etc/UTC",
    )
    assert saved_snapshots_table.default_feature_job_setting == effective_feature_job_setting
    assert cloned.default_feature_job_setting == effective_feature_job_setting


def test_timezone__valid(snowflake_database_snapshots_table, catalog):
    """Test specifying a valid timezone"""
    _ = catalog

    snapshots_table = snowflake_database_snapshots_table.create_snapshots_table(
        name="sf_snapshots_table",
        snapshot_id_column="col_int",
        snapshot_datetime_column="date",
        snapshot_datetime_schema=TimestampSchema(
            format_string="YYYY-MM-DD HH24:MI:SS", timezone="Asia/Singapore"
        ),
        time_interval=TimeInterval(value=1, unit="DAY"),
    )
    assert snapshots_table.snapshot_datetime_schema.timezone == "Asia/Singapore"

    input_node_params = snapshots_table.frame.node.parameters
    assert input_node_params.snapshot_datetime_schema.timezone == "Asia/Singapore"

    # check update default feature job setting without providing reference timezone
    cron_feature_job_setting = CronFeatureJobSetting(
        crontab=Crontab(
            minute=0,
            hour=1,
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
        ),
        timezone="US/Pacific",
        reference_timezone=None,
    )
    snapshots_table.update_default_feature_job_setting(feature_job_setting=cron_feature_job_setting)
    default_feature_job_setting = snapshots_table.default_feature_job_setting
    assert default_feature_job_setting.crontab == cron_feature_job_setting.crontab
    assert default_feature_job_setting.timezone == cron_feature_job_setting.timezone
    assert default_feature_job_setting.reference_timezone == "Asia/Singapore"


def test_timezone__invalid(snowflake_database_snapshots_table, catalog):
    """Test specifying an invalid timezone"""
    _ = catalog

    with pytest.raises(ValidationError) as exc:
        snowflake_database_snapshots_table.create_snapshots_table(
            name="sf_snapshots_table",
            snapshot_id_column="col_int",
            snapshot_datetime_column="date",
            snapshot_datetime_schema=TimestampSchema(
                format_string="YYYY-MM-DD HH24:MI:SS", timezone="Space/Time"
            ),
            time_interval=TimeInterval(value=1, unit="DAY"),
        )
    assert "Invalid timezone name." in str(exc.value)

    with pytest.raises(RecordCreationException) as exc:
        snowflake_database_snapshots_table.create_snapshots_table(
            name="sf_snapshots_table",
            snapshot_id_column="col_int",
            snapshot_datetime_column="date",
            snapshot_datetime_schema=TimestampSchema(
                format_string="YYYY-MM-DD HH24:MI:SS", timezone="Asia/Singapore"
            ),
            time_interval=TimeInterval(value=2, unit="DAY"),
        )

    expected_msg = (
        "Only intervals defined with a single time unit (e.g., 1 hour, 1 day) are supported."
    )
    assert expected_msg in str(exc.value)


def test_timezone_offset__valid_column(snowflake_database_snapshots_table, catalog):
    """Test specifying a timezone offset using a column"""
    _ = catalog
    snapshots_table = snowflake_database_snapshots_table.create_snapshots_table(
        name="sf_snapshots_table",
        snapshot_id_column="col_int",
        snapshot_datetime_column="date",
        snapshot_datetime_schema=TimestampSchema(
            format_string="YYYY-MM-DD HH24:MI:SS",
            timezone=TimeZoneColumn(
                column_name="col_text",
                type="offset",
            ),
        ),
        time_interval=TimeInterval(value=1, unit="DAY"),
    )
    assert snapshots_table.reference_datetime_schema.timezone.column_name == "col_text"

    input_node_params = snapshots_table.frame.node.parameters
    assert input_node_params.reference_datetime_schema.timezone.column_name == "col_text"

    # check tagged semantic
    column_semantic_map = {}
    for col_info in snapshots_table.info(verbose=True)["columns_info"]:
        if col_info["semantic"]:
            column_semantic_map[col_info["name"]] = col_info["semantic"]

    assert column_semantic_map == {
        "col_int": "snapshot_id",
        "date": "snapshot_date_time",
        "col_text": "time_zone",
    }

    # check reference column map & conditionally expand columns logic
    view = snapshots_table.get_view()
    assert view._reference_column_map == {"date": ["col_text"]}
    assert view._conditionally_expand_columns(["date"]) == ["date", "col_text"]

    # create zip timestamp with timezone offset column
    ts_tz_col = view.date.zip_timestamp_timezone_columns()
    assert ts_tz_col.dtype == DBVarType.TIMESTAMP_TZ_TUPLE
    assert ts_tz_col.dtype_info == DBVarTypeInfo(
        dtype=DBVarType.TIMESTAMP_TZ_TUPLE,
        metadata=DBVarTypeMetadata(
            timestamp_schema=None,
            timestamp_tuple_schema=TimestampTupleSchema(
                timezone_offset_schema=TimezoneOffsetSchema(dtype=DBVarType.VARCHAR),
                timestamp_schema=ExtendedTimestampSchema(
                    dtype=view.date.dtype,
                    format_string="YYYY-MM-DD HH24:MI:SS",
                    timezone=TimeZoneColumn(
                        column_name="col_text",
                        type="offset",
                    ),
                ),
            ),
        ),
    )

    # test update default feature job setting
    cron_feature_job_setting = CronFeatureJobSetting(
        crontab=Crontab(
            minute=0,
            hour=1,
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
        ),
        timezone="Etc/UTC",
    )
    snapshots_table.update_default_feature_job_setting(feature_job_setting=cron_feature_job_setting)
    assert snapshots_table.default_feature_job_setting == cron_feature_job_setting

    # attempt to add timestamp schema to special column
    with pytest.raises(RecordUpdateException) as exc:
        snapshots_table.date.update_critical_data_info(
            cleaning_operations=[AddTimestampSchema(timestamp_schema=TimestampSchema())]
        )

    expected_msg = "Cannot update special column: date with AddTimestampSchema cleaning operation"
    assert expected_msg in str(exc.value)


def test_timezone_offset__invalid_column(snowflake_database_snapshots_table, catalog):
    """Test specifying a timezone offset using a column"""
    _ = catalog
    with pytest.raises(RecordCreationException) as exc:
        snowflake_database_snapshots_table.create_snapshots_table(
            name="sf_snapshots_table",
            snapshot_id_column="col_int",
            snapshot_datetime_column="date",
            snapshot_datetime_schema=TimestampSchema(
                format_string="YYYY-MM-DD HH24:MI:SS",
                timezone=TimeZoneColumn(
                    column_name="col_float",
                    type="offset",
                    format_string="TZH:TZM",
                ),
            ),
            time_interval=TimeInterval(value=1, unit="DAY"),
        )
    expected = "Column \"col_float\" is expected to have type(s): ['VARCHAR']"
    assert expected in str(exc.value)


def test_sdk_code_generation(saved_snapshots_table, update_fixtures):
    """Check SDK code generation for unsaved table"""
    check_sdk_code_generation(
        saved_snapshots_table.frame,
        to_use_saved_data=False,
        fixture_path="tests/fixtures/sdk_code/snapshots_table.py",
        update_fixtures=update_fixtures,
        table_id=saved_snapshots_table.id,
    )


def test_sdk_code_generation_on_saved_data(saved_snapshots_table, update_fixtures):
    """Check SDK code generation for saved table"""
    check_sdk_code_generation(
        saved_snapshots_table.frame,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/saved_snapshots_table.py",
        update_fixtures=update_fixtures,
        table_id=saved_snapshots_table.id,
    )


def test_shape(snowflake_snapshots_table, snowflake_query_map):
    """
    Test creating ObservationTable from an EventView
    """

    def side_effect(query, timeout=None, to_log_error=True, query_metadata=None):
        _ = (
            timeout,
            to_log_error,
            query_metadata,
        )
        res = snowflake_query_map.get(query)
        if res is not None:
            return pd.DataFrame(res)
        return pd.DataFrame({"count": [1000]})

    with mock.patch(
        "featurebyte.session.snowflake.SnowflakeSession.execute_query"
    ) as mock_execute_query:
        mock_execute_query.side_effect = side_effect
        assert snowflake_snapshots_table.shape() == (1000, 10)
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
                    "date" AS "date",
                    "created_at" AS "created_at",
                    "store_id" AS "store_id",
                    "another_timestamp_col" AS "another_timestamp_col"
                  FROM "sf_database"."sf_schema"."snapshots_table"
                )
                SELECT
                  COUNT(*) AS "count"
                FROM data
                """
            ).strip()
        )
        # test table colum shape
        assert snowflake_snapshots_table["col_int"].shape() == (1000, 1)


def test_update_critical_data_info_with_none_value(saved_snapshots_table):
    """Test update critical data info with None value"""
    cleaning_operations = [DisguisedValueImputation(imputed_value=None, disguised_values=[-999])]
    saved_snapshots_table.col_int.update_critical_data_info(cleaning_operations=cleaning_operations)
    assert (
        saved_snapshots_table.col_int.info.critical_data_info.cleaning_operations
        == cleaning_operations
    )
