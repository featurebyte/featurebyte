"""
Tests for EventTable models
"""

import datetime

import pytest
from pydantic import ValidationError

from featurebyte.common import DEFAULT_CATALOG_ID
from featurebyte.models.event_table import EventTableModel
from featurebyte.models.feature_store import TableStatus
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.node.schema import TableDetails


@pytest.fixture(name="feature_job_setting")
def feature_job_setting_fixture():
    """Fixture for a Feature Job Setting"""
    feature_job_setting = FeatureJobSetting(blind_spot="10m", period="30m", offset="5m")
    return feature_job_setting


def test_event_table_model(snowflake_feature_store, feature_job_setting):
    """Test creation, serialization and deserialization of an EventTable"""
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
        },
        {
            "name": "event_date",
            "dtype": "TIMESTAMP",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
            "description": None,
            "dtype_metadata": None,
            "partition_metadata": None,
        },
        {
            "name": "event_id",
            "dtype": "INT",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
            "description": None,
            "dtype_metadata": None,
            "partition_metadata": None,
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
        },
    ]
    event_table = EventTableModel(
        name="my_event_table",
        tabular_source={
            "feature_store_id": snowflake_feature_store.id,
            "table_details": TableDetails(
                database_name="database", schema_name="schema", table_name="table"
            ),
        },
        columns_info=columns_info,
        event_id_column="event_id",
        event_timestamp_column="event_date",
        record_creation_timestamp_column="created_at",
        default_feature_job_setting=feature_job_setting,
        created_at=datetime.datetime(2022, 2, 1),
        status=TableStatus.PUBLISHED,
    )
    expected_event_table_dict = {
        "type": "event_table",
        "user_id": None,
        "created_at": datetime.datetime(2022, 2, 1, 0, 0),
        "updated_at": None,
        "default_feature_job_setting": {
            "blind_spot": "600s",
            "period": "1800s",
            "offset": "300s",
            "execution_buffer": "0s",
        },
        "columns_info": columns_info,
        "event_timestamp_column": "event_date",
        "event_timestamp_schema": None,
        "event_id_column": "event_id",
        "id": event_table.id,
        "name": "my_event_table",
        "record_creation_timestamp_column": "created_at",
        "status": "PUBLISHED",
        "tabular_source": {
            "feature_store_id": event_table.tabular_source.feature_store_id,
            "table_details": {
                "database_name": "database",
                "schema_name": "schema",
                "table_name": "table",
            },
        },
        "catalog_id": DEFAULT_CATALOG_ID,
        "event_timestamp_timezone_offset": None,
        "event_timestamp_timezone_offset_column": None,
        "block_modification_by": [],
        "description": None,
        "is_deleted": False,
        "managed_view_id": None,
        "validation": None,
    }
    assert event_table.model_dump() == expected_event_table_dict
    event_table_json = event_table.model_dump_json(by_alias=True)
    event_table_loaded = EventTableModel.model_validate_json(event_table_json)
    assert event_table_loaded == event_table


@pytest.mark.parametrize(
    "field,value",
    [
        ("blind_spot", "10something"),
        ("period", "10something"),
        ("offset", "10something"),
    ],
)
def test_invalid_job_setting__invalid_unit(feature_job_setting, field, value):
    """Test validation on invalid job settings"""
    setting_dict = feature_job_setting.model_dump()
    setting_dict[field] = value
    with pytest.raises(ValidationError) as exc_info:
        FeatureJobSetting(**setting_dict)
    assert "invalid unit" in str(exc_info.value)


def test_invalid_job_setting__too_small(feature_job_setting):
    """Test validation on invalid job settings"""
    setting_dict = feature_job_setting.model_dump()
    setting_dict["period"] = "1s"
    with pytest.raises(ValidationError) as exc_info:
        FeatureJobSetting(**setting_dict)
    assert "Duration specified is too small: 1s" in str(exc_info.value)
