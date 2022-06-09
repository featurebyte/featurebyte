"""
Tests for EventTable models
"""
import datetime

import pytest

from featurebyte.models.event_table import (
    EventTableModel,
    EventTableStatus,
    FeatureJobSetting,
    FeatureJobSettingHistoryEntry,
    SnowflakeSource,
)


@pytest.fixture(name="snowflake_source")
def snowflake_source_fixture():
    """Fixture for a Snowflake source"""
    snowflake_source = SnowflakeSource(
        account="account",
        warehouse="warehouse",
        database="database",
        sf_schema="schema",
    )
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


def test_event_table_model(snowflake_source, feature_job_setting, feature_job_setting_history):
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
    assert event_table.dict() == {
        "name": "my_event_table",
        "table_name": "table",
        "source": {
            "account": "account",
            "warehouse": "warehouse",
            "database": "database",
            "sf_schema": "schema",
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
    event_table_json = event_table.json()
    event_table_loaded = event_table.parse_raw(event_table_json)
    assert event_table_loaded == event_table
