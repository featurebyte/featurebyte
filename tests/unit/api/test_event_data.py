"""
Unit test for EventData class
"""
from __future__ import annotations

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


def test_deserialization(
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
    event_data_dict["credentials"] = config.credentials
    event_data = EventData.parse_obj(event_data_dict)
    assert event_data.preview_sql() == expected_snowflake_table_preview_query


def test_deserialization__column_name_not_found(event_data_dict, config, snowflake_execute_query):
    """
    Test column not found during deserialize event data
    """
    _ = snowflake_execute_query
    event_data_dict["credentials"] = config.credentials
    event_data_dict["record_creation_date_column"] = "some_random_name"
    with pytest.raises(ValueError) as exc:
        EventData.parse_obj(event_data_dict)
    assert 'Column "some_random_name" not found in the table!' in str(exc.value)
