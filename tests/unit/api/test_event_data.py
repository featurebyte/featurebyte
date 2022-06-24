"""
Unit test for EventData class
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from featurebyte.api.event_data import EventData


@pytest.fixture(name="event_data_dict")
def event_data_dict_fixture():
    """
    EventData in serialized dictionary format
    """
    return {
        "name": "sf_event_data",
        "tabular_source": (
            {
                "type": "snowflake",
                "details": {
                    "account": "sf_account",
                    "database": "sf_database",
                    "sf_schema": "sf_schema",
                    "warehouse": "sf_warehouse",
                },
            },
            "sf_table",
        ),
        "event_timestamp_column": "event_timestamp",
        "record_creation_date_column": "created_at",
        "default_feature_job_setting": None,
        "created_at": None,
        "history": [],
        "status": None,
    }


def test_from_tabular_source(snowflake_database_table, config, event_data_dict):
    """
    Test EventData creation using tabular source
    """
    event_data = EventData.from_tabular_source(
        tabular_source=snowflake_database_table,
        name="sf_event_data",
        event_timestamp_column="event_timestamp",
        record_creation_date_column="created_at",
        credentials=config.credentials,
    )
    assert event_data.dict() == event_data_dict


@patch("featurebyte.api.event_data.Configurations")
def test_deserialization(
    mock_configurations,
    event_data_dict,
    config,
    snowflake_execute_query,
    expected_snowflake_table_preview_query,
):
    """
    Test deserialize event data dictionary
    """
    _ = snowflake_execute_query
    # setup proper configuration to deserialize the event data object
    mock_configurations.return_value = config
    event_data = EventData.parse_obj(event_data_dict)
    assert event_data.preview_sql() == expected_snowflake_table_preview_query
