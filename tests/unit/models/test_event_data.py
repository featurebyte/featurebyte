"""
Tests for EventData models
"""
import datetime

import pytest
from pydantic.error_wrappers import ValidationError

from featurebyte.enum import SourceType
from featurebyte.models.event_data import (
    DatabaseSourceModel,
    EventDataModel,
    EventDataStatus,
    FeatureJobSetting,
    FeatureJobSettingHistoryEntry,
    SnowflakeDetails,
)


@pytest.fixture(name="snowflake_source")
def snowflake_source_fixture():
    """Fixture for a Snowflake source"""
    snowflake_details = SnowflakeDetails(
        account="account",
        warehouse="warehouse",
        database="database",
        sf_schema="schema",
    )
    snowflake_source = DatabaseSourceModel(type=SourceType.SNOWFLAKE, details=snowflake_details)
    return snowflake_source


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
            creation_date=datetime.datetime(2022, 4, 1),
            setting=feature_job_setting,
        ),
        FeatureJobSettingHistoryEntry(
            creation_date=datetime.datetime(2022, 2, 1),
            setting=feature_job_setting,
        ),
    ]
    return history


@pytest.fixture(name="event_data_model_dict")
def event_data_model_dict_fixture():
    """Fixture for a Event Data dict"""
    return {
        "name": "my_event_data",
        "tabular_source": (
            {
                "type": "snowflake",
                "details": {
                    "account": "account",
                    "warehouse": "warehouse",
                    "database": "database",
                    "sf_schema": "schema",
                },
            },
            "table",
        ),
        "event_timestamp_column": "event_date",
        "record_creation_date_column": "created_at",
        "column_entity_map": {},
        "column_description_map": {},
        "default_feature_job_setting": {
            "blind_spot": "10m",
            "frequency": "30m",
            "time_modulo_frequency": "5m",
        },
        "created_at": datetime.datetime(2022, 2, 1),
        "history": [
            {
                "creation_date": datetime.datetime(2022, 4, 1),
                "setting": {
                    "blind_spot": "10m",
                    "frequency": "30m",
                    "time_modulo_frequency": "5m",
                },
            },
            {
                "creation_date": datetime.datetime(2022, 2, 1),
                "setting": {
                    "blind_spot": "10m",
                    "frequency": "30m",
                    "time_modulo_frequency": "5m",
                },
            },
        ],
        "status": "PUBLISHED",
    }


def test_event_data_model(
    snowflake_source, feature_job_setting, feature_job_setting_history, event_data_model_dict
):
    """Test creation, serialization and deserialization of an EventData"""
    event_data = EventDataModel(
        name="my_event_data",
        tabular_source=(snowflake_source, "table"),
        event_timestamp_column="event_date",
        record_creation_date_column="created_at",
        default_feature_job_setting=feature_job_setting,
        created_at=datetime.datetime(2022, 2, 1),
        history=feature_job_setting_history,
        status=EventDataStatus.PUBLISHED,
    )
    assert event_data.dict() == event_data_model_dict
    event_data_json = event_data.json()
    event_data_loaded = event_data.parse_raw(event_data_json)
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
