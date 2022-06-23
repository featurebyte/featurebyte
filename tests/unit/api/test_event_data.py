"""
Unit test for EventData class
"""
from __future__ import annotations

import textwrap
from unittest.mock import patch

import pytest

from featurebyte.api.event_data import EventData


@pytest.fixture(name="event_data_dict")
def event_data_dict_fixture():
    """
    EventData in serialized dictionary format
    """
    return {
        "name": "event_data_name",
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
        "event_timestamp_column": "some_timestamp_column",
        "record_creation_date_column": None,
        "default_feature_job_setting": None,
        "created_at": None,
        "history": [],
        "status": None,
    }


@pytest.fixture(name="expected_query")
def expected_preview_query() -> str:
    """
    Expected preview_sql output
    """
    return textwrap.dedent(
        """
        SELECT
          "col_int",
          "col_float",
          "col_char",
          "col_text",
          "col_binary",
          "col_boolean"
        FROM "sf_table"
        LIMIT 10
        """
    ).strip()


def test_from_tabular_source(database_table, config, event_data_dict, expected_query):
    """
    Test event data creation from tabular source
    """
    event_data = EventData.from_tabular_source(
        tabular_source=database_table,
        name="event_data_name",
        event_timestamp_column="some_timestamp_column",
        credentials=config.credentials,
    )
    assert event_data.dict() == event_data_dict
    assert event_data.preview_sql() == event_data.preview_sql()


@patch("featurebyte.api.event_data.Configurations")
def test_deserialization(
    mock_configurations, event_data_dict, config, snowflake_execute_query, expected_query
):
    """
    Test deserialize event data dictionary
    """
    # setup proper configuration to deserialize the event data object
    mock_configurations.return_value = config
    event_data = EventData.parse_obj(event_data_dict)
    assert event_data.preview_sql() == expected_query
