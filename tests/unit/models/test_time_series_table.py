"""
Tests for TimeSeriesTable models
"""

import datetime

import pytest
from pydantic import ValidationError

from featurebyte import TimeInterval
from featurebyte.common import DEFAULT_CATALOG_ID
from featurebyte.models.feature_store import TableStatus
from featurebyte.models.time_series_table import TimeSeriesTableModel
from featurebyte.query_graph.model.feature_job_setting import CronFeatureJobSetting
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node.schema import TableDetails


@pytest.fixture(name="feature_job_setting")
def feature_job_setting_fixture():
    """Fixture for a Feature Job Setting"""
    feature_job_setting = CronFeatureJobSetting(crontab="0 0 * * *", timezone="Etc/UTC")
    return feature_job_setting


def test_time_series_table_model(snowflake_feature_store, feature_job_setting):
    """Test creation, serialization and deserialization of an TimeSeriesTable"""
    ts_schema = TimestampSchema(format_string=None, timezone="Etc/UTC", is_utc_time=None)
    columns_info = [
        {
            "name": "col",
            "dtype": "INT",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
            "description": None,
            "dtype_metadata": None,
            "partition_metadata": None,
            "nested_field_metadata": None,
        },
        {
            "name": "date",
            "dtype": "TIMESTAMP",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
            "description": None,
            "dtype_metadata": {
                "timestamp_schema": ts_schema.model_dump(),
                "timestamp_tuple_schema": None,
            },
            "partition_metadata": None,
            "nested_field_metadata": None,
        },
        {
            "name": "series_id",
            "dtype": "INT",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
            "description": None,
            "dtype_metadata": None,
            "partition_metadata": None,
            "nested_field_metadata": None,
        },
        {
            "name": "created_at",
            "dtype": "TIMESTAMP",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
            "description": None,
            "dtype_metadata": None,
            "partition_metadata": None,
            "nested_field_metadata": None,
        },
    ]
    time_series_table = TimeSeriesTableModel(
        name="my_time_series_table",
        tabular_source={
            "feature_store_id": snowflake_feature_store.id,
            "table_details": TableDetails(
                database_name="database", schema_name="schema", table_name="table"
            ),
        },
        columns_info=columns_info,
        series_id_column="series_id",
        reference_datetime_column="date",
        reference_datetime_schema=ts_schema,
        time_interval=TimeInterval(value=1, unit="DAY"),
        record_creation_timestamp_column="created_at",
        default_feature_job_setting=feature_job_setting,
        created_at=datetime.datetime(2022, 2, 1),
        status=TableStatus.PUBLISHED,
    )
    expected_time_series_table_dict = {
        "type": "time_series_table",
        "user_id": None,
        "created_at": datetime.datetime(2022, 2, 1, 0, 0),
        "updated_at": None,
        "default_feature_job_setting": {
            "crontab": {
                "minute": 0,
                "hour": 0,
                "day_of_month": "*",
                "month_of_year": "*",
                "day_of_week": "*",
            },
            "timezone": "Etc/UTC",
            "reference_timezone": None,
            "blind_spot": None,
        },
        "columns_info": columns_info,
        "series_id_column": "series_id",
        "reference_datetime_column": "date",
        "reference_datetime_schema": {
            "format_string": None,
            "timezone": "Etc/UTC",
            "is_utc_time": None,
        },
        "time_interval": {"value": 1, "unit": "DAY"},
        "_id": time_series_table.id,
        "name": "my_time_series_table",
        "record_creation_timestamp_column": "created_at",
        "status": "PUBLISHED",
        "tabular_source": {
            "feature_store_id": time_series_table.tabular_source.feature_store_id,
            "table_details": {
                "database_name": "database",
                "schema_name": "schema",
                "table_name": "table",
            },
        },
        "catalog_id": DEFAULT_CATALOG_ID,
        "block_modification_by": [],
        "description": None,
        "is_deleted": False,
        "managed_view_id": None,
        "validation": None,
        "datetime_partition_column": None,
        "datetime_partition_schema": None,
    }
    assert time_series_table.model_dump(by_alias=True) == expected_time_series_table_dict
    time_series_table_json = time_series_table.model_dump_json(by_alias=True)
    time_series_table_loaded = TimeSeriesTableModel.model_validate_json(time_series_table_json)
    assert time_series_table_loaded == time_series_table


def test_invalid_job_setting__invalid_timezone():
    """Test validation on invalid job settings"""
    with pytest.raises(ValidationError) as exc_info:
        CronFeatureJobSetting(timezone="SomeInvalidTimezone")
    assert "Invalid timezone name" in str(exc_info.value)


def test_invalid_job_setting__invalid_crontab():
    """Test validation on invalid job settings"""
    with pytest.raises(ValidationError) as exc_info:
        CronFeatureJobSetting(crontab="0.1 0 * * *")
    assert "Invalid crontab expression: 0.1 0 * * *" in str(exc_info.value)
