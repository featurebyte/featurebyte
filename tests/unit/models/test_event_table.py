"""
Tests for EventTable models
"""
import datetime

import pytest
from pydantic.error_wrappers import ValidationError

from featurebyte.enum import SourceType
from featurebyte.models.event_table import (
    DatabaseSource,
    EventTableModel,
    EventTableStatus,
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
    snowflake_source = DatabaseSource(type=SourceType.SNOWFLAKE, details=snowflake_details)
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


@pytest.fixture(name="event_table_model_dict")
def event_table_model_dict_fixture():
    """Fixture for a Event Table dict"""
    return {
        "name": "my_event_table",
        "table_name": "table",
        "source": {
            "type": "snowflake",
            "details": {
                "account": "account",
                "warehouse": "warehouse",
                "database": "database",
                "sf_schema": "schema",
            },
        },
        "event_timestamp_column": "event_date",
        "record_creation_date_column": "created_at",
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


def test_event_table_model(
    snowflake_source, feature_job_setting, feature_job_setting_history, event_table_model_dict
):
    """Test creation, serialization and deserialization of an EventTable"""
    event_table = EventTableModel(
        name="my_event_table",
        table_name="table",
        source=snowflake_source,
        event_timestamp_column="event_date",
        record_creation_date_column="created_at",
        default_feature_job_setting=feature_job_setting,
        created_at=datetime.datetime(2022, 2, 1),
        history=feature_job_setting_history,
        status=EventTableStatus.PUBLISHED,
    )
    assert event_table.dict() == event_table_model_dict
    event_table_json = event_table.json()
    event_table_loaded = event_table.parse_raw(event_table_json)
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
