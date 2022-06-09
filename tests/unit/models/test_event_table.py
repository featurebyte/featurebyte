"""
Tests for EventTable models
"""
import datetime

import pytest

from featurebyte.models.event_table import (
    EventTableModel,
    EventTableStatus,
    FeatureJobSettings,
    FeatureJobSettingsHistoryEntry,
    SnowflakeSource,
)


@pytest.fixture(name="snowflake_source")
def snowflake_source_fixture():
    snowflake_source = SnowflakeSource(
        account="account",
        warehouse="warehouse",
        database="database",
        sf_schema="schema",
    )
    return snowflake_source


@pytest.fixture(name="feature_job_settings")
def feature_job_settings_fixture():
    feature_job_settings = FeatureJobSettings(
        blind_spot="10m",
        frequency="30m",
        time_modulo_frequency="5m",
    )
    return feature_job_settings


@pytest.fixture(name="feature_job_settings_history")
def feature_job_settings_history_fixture(feature_job_settings):
    history = [
        FeatureJobSettingsHistoryEntry(
            creation_date=datetime.datetime(2022, 4, 1),
            settings=feature_job_settings,
        ),
        FeatureJobSettingsHistoryEntry(
            creation_date=datetime.datetime(2022, 2, 1),
            settings=feature_job_settings,
        ),
    ]
    return history


def test_event_table_model(snowflake_source, feature_job_settings, feature_job_settings_history):
    event_table = EventTableModel(
        name="my_event_table",
        table_name="table",
        source=snowflake_source,
        event_timestamp_column="event_date",
        record_creation_date_column="created_at",
        feature_job_settings=feature_job_settings,
        created_at=datetime.datetime(2022, 4, 1),
        feature_job_settings_creation_date=datetime.datetime(2022, 4, 1),
        history=feature_job_settings_history,
        status=EventTableStatus.published,
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
        "feature_job_settings": {
            "blind_spot": "10m",
            "frequency": "30m",
            "time_modulo_frequency": "5m",
        },
        "feature_job_settings_creation_date": datetime.datetime(2022, 4, 1),
        "created_at": datetime.datetime(2022, 4, 1),
        "history": [
            {
                "creation_date": datetime.datetime(2022, 4, 1),
                "settings": {
                    "blind_spot": "10m",
                    "frequency": "30m",
                    "time_modulo_frequency": "5m",
                },
            },
            {
                "creation_date": datetime.datetime(2022, 2, 1),
                "settings": {
                    "blind_spot": "10m",
                    "frequency": "30m",
                    "time_modulo_frequency": "5m",
                },
            },
        ],
        "status": "published",
    }
    event_table_json = event_table.json()
    event_table_loaded = event_table.parse_raw(event_table_json)
    assert event_table_loaded == event_table
