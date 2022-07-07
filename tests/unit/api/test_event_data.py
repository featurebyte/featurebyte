"""
Unit test for EventData class
"""
from __future__ import annotations

import pytest

from featurebyte.api.entity import Entity
from featurebyte.api.event_data import EventData, EventDataColumn


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
            {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
        ),
        "event_timestamp_column": "event_timestamp",
        "record_creation_date_column": "created_at",
        "column_entity_map": {},
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


def test_event_data_column__not_exists(snowflake_event_data):
    """
    Test non-exist column retrieval
    """
    expected = 'Column "non_exist_column" does not exist!'
    with pytest.raises(KeyError) as exc:
        _ = snowflake_event_data["non_exist_column"]
    assert expected in str(exc.value)

    with pytest.raises(KeyError) as exc:
        _ = snowflake_event_data.non_exist_column
    assert expected in str(exc.value)


def test_event_data_column__as_entity(
    snowflake_event_data, mock_config_path_env, mock_get_persistent
):
    """
    Test setting a column in the event data as entity
    """
    _ = mock_config_path_env, mock_get_persistent

    # create entity
    Entity(name="customer", serving_name="cust_id")

    col_int = snowflake_event_data.col_int
    assert isinstance(col_int, EventDataColumn)
    snowflake_event_data.col_int.as_entity("customer")
    assert "col_int" in snowflake_event_data.column_entity_map

    with pytest.raises(TypeError) as exc:
        snowflake_event_data.col_int.as_entity(1234)
    assert 'Unsupported type "<class \'int\'>" for tag name "1234"!' in str(exc.value)

    with pytest.raises(ValueError) as exc:
        snowflake_event_data.col_int.as_entity("some_random_entity")
    assert 'Entity name "some_random_entity" not found!' in str(exc.value)

    # remove entity association
    snowflake_event_data.col_int.as_entity(None)
    assert snowflake_event_data.column_entity_map == {}


def test_event_data_column__get_entity_id(mock_config_path_env, mock_get_persistent):
    """
    Test get_entity_id given entity name
    """
    # create a list of entity for testing
    entity_id_map = {}
    for idx in range(10):
        entity_id_map[idx] = Entity(name=f"entity_{idx}", serving_name=f"entity_{idx}").id

    # test retrieval
    for page_size in {3, 5}:
        for idx, expected_entity_id in entity_id_map.items():
            entity_id = EventDataColumn.get_entity_id(
                entity_name=f"entity_{idx}", page_size=page_size
            )
            assert entity_id == str(expected_entity_id)

    # test unknown entity
    assert EventDataColumn.get_entity_id("unknown_entity") is None
