"""
Tests for EventData models
"""
import datetime

import pytest
from pydantic.error_wrappers import ValidationError

from featurebyte.models.event_data import (
    EventDataModel,
    EventDataStatus,
    FeatureJobSetting,
    FeatureJobSettingHistoryEntry,
)
from featurebyte.models.feature_store import TableDetails


@pytest.fixture(name="feature_job_setting")
def feature_job_setting_fixture():
    """Fixture for a Feature Job Setting"""
    feature_job_setting = FeatureJobSetting(
        blind_spot="10m",
        frequency="30m",
        time_modulo_frequency="5m",
    )
    return feature_job_setting


@pytest.fixture(name="feature_job_setting_history")
def feature_job_setting_history_fixture(feature_job_setting):
    """Fixture for a Feature Job Setting history"""
    history = [
        FeatureJobSettingHistoryEntry(
            created_at=datetime.datetime(2022, 4, 1),
            setting=feature_job_setting,
        ),
        FeatureJobSettingHistoryEntry(
            created_at=datetime.datetime(2022, 2, 1),
            setting=feature_job_setting,
        ),
    ]
    return history


def test_event_data_model(
    snowflake_feature_store, feature_job_setting, feature_job_setting_history
):
    """Test creation, serialization and deserialization of an EventData"""
    event_data = EventDataModel(
        name="my_event_data",
        tabular_source=(
            snowflake_feature_store.id,
            TableDetails(database_name="database", schema_name="schema", table_name="table"),
        ),
        event_timestamp_column="event_date",
        record_creation_date_column="created_at",
        default_feature_job_setting=feature_job_setting,
        created_at=datetime.datetime(2022, 2, 1),
        history=feature_job_setting_history,
        status=EventDataStatus.PUBLISHED,
    )
    expected_event_data_dict = {
        "column_entity_map": None,
        "user_id": None,
        "created_at": datetime.datetime(2022, 2, 1, 0, 0),
        "updated_at": None,
        "default_feature_job_setting": {
            "blind_spot": "10m",
            "frequency": "30m",
            "time_modulo_frequency": "5m",
        },
        "event_timestamp_column": "event_date",
        "id": event_data.id,
        "name": "my_event_data",
        "record_creation_date_column": "created_at",
        "status": "PUBLISHED",
        "tabular_source": (
            event_data.tabular_source[0],
            {"database_name": "database", "schema_name": "schema", "table_name": "table"},
        ),
    }
    assert event_data.dict() == expected_event_data_dict
    event_data_json = event_data.json(by_alias=True)
    event_data_loaded = EventDataModel.parse_raw(event_data_json)
    assert event_data_loaded == event_data


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
