"""
Tests for EventTable models
"""
import datetime

import pytest
from pydantic.error_wrappers import ValidationError

from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.models.event_table import EventTableModel
from featurebyte.models.feature_store import TableStatus
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.node.schema import TableDetails


@pytest.fixture(name="feature_job_setting")
def feature_job_setting_fixture():
    """Fixture for a Feature Job Setting"""
    feature_job_setting = FeatureJobSetting(
        blind_spot="10m",
        frequency="30m",
        time_modulo_frequency="5m",
    )
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
        },
        {
            "name": "event_date",
            "dtype": "TIMESTAMP",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
        {
            "name": "event_id",
            "dtype": "INT",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
        {
            "name": "created_at",
            "dtype": "TIMESTAMP",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
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
            "blind_spot": "10m",
            "frequency": "30m",
            "time_modulo_frequency": "5m",
        },
        "columns_info": columns_info,
        "event_timestamp_column": "event_date",
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
    }
    assert event_table.dict() == expected_event_table_dict
    event_table_json = event_table.json(by_alias=True)
    event_table_loaded = EventTableModel.parse_raw(event_table_json)
    assert event_table_loaded == event_table


@pytest.mark.parametrize(
    "field,value",
    [
        ("blind_spot", "10something"),
        ("frequency", "10something"),
        ("time_modulo_frequency", "10something"),
    ],
)
def test_invalid_job_setting__invalid_unit(feature_job_setting, field, value):
    """Test validation on invalid job settings"""
    setting_dict = feature_job_setting.dict()
    setting_dict[field] = value
    with pytest.raises(ValidationError) as exc_info:
        FeatureJobSetting(**setting_dict)
    assert "invalid unit" in str(exc_info.value)


def test_invalid_job_setting__too_small(feature_job_setting):
    """Test validation on invalid job settings"""
    setting_dict = feature_job_setting.dict()
    setting_dict["frequency"] = "1s"
    with pytest.raises(ValidationError) as exc_info:
        FeatureJobSetting(**setting_dict)
    assert "Duration specified is too small: 1s" in str(exc_info.value)
