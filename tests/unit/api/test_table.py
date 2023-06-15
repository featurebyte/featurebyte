"""
Unit test for Table class
"""
from __future__ import annotations

import pytest

from featurebyte.api.dimension_table import DimensionTable
from featurebyte.api.event_table import EventTable
from featurebyte.api.item_table import ItemTable
from featurebyte.api.scd_table import SCDTable
from featurebyte.api.table import Table
from featurebyte.exception import RecordRetrievalException


def test_get_event_table(saved_event_table, snowflake_event_table):
    """
    Test Table.get function to retrieve EventTable
    """
    # load the event table from the persistent
    loaded_event_table = Table.get(snowflake_event_table.name)
    assert loaded_event_table.saved is True
    assert loaded_event_table == snowflake_event_table
    assert EventTable.get_by_id(id=snowflake_event_table.id) == snowflake_event_table

    # load the event table use get_by_id
    loaded_table = Table.get_by_id(snowflake_event_table.id)
    assert loaded_table == loaded_event_table

    with pytest.raises(RecordRetrievalException) as exc:
        Table.get("unknown_event_table")
    expected_msg = (
        'Table (name: "unknown_event_table") not found. ' "Please save the Table object first."
    )
    assert expected_msg in str(exc.value)


def test_get_item_table(snowflake_item_table, saved_item_table):
    """
    Test Table.get function to retrieve ItemTable
    """
    # load the item table from the persistent
    loaded_table = Table.get(saved_item_table.name)
    assert loaded_table.saved is True
    assert loaded_table == snowflake_item_table
    assert ItemTable.get_by_id(id=loaded_table.id) == snowflake_item_table

    with pytest.raises(RecordRetrievalException) as exc:
        Table.get("unknown_item_table")
    expected_msg = (
        'Table (name: "unknown_item_table") not found. ' "Please save the Table object first."
    )
    assert expected_msg in str(exc.value)


def test_get_scd_table(saved_scd_table, snowflake_scd_table):
    """
    Test Table.get function to retrieve SCDTable
    """
    # load the scd table from the persistent
    loaded_scd_table = Table.get(snowflake_scd_table.name)
    assert loaded_scd_table.saved is True
    assert loaded_scd_table == snowflake_scd_table
    assert SCDTable.get_by_id(id=snowflake_scd_table.id) == snowflake_scd_table

    with pytest.raises(RecordRetrievalException) as exc:
        Table.get("unknown_scd_table")
    expected_msg = (
        'Table (name: "unknown_scd_table") not found. ' "Please save the Table object first."
    )
    assert expected_msg in str(exc.value)


def test_get_dimension_table(saved_dimension_table, snowflake_dimension_table):
    """
    Test Table.get function to retrieve DimensionTable
    """
    # load the dimension table from the persistent
    loaded_scd_table = Table.get(snowflake_dimension_table.name)
    assert loaded_scd_table.saved is True
    assert loaded_scd_table == snowflake_dimension_table
    assert DimensionTable.get_by_id(id=snowflake_dimension_table.id) == snowflake_dimension_table

    with pytest.raises(RecordRetrievalException) as exc:
        Table.get("unknown_dimension_table")
    expected_msg = (
        'Table (name: "unknown_dimension_table") not found. ' "Please save the Table object first."
    )
    assert expected_msg in str(exc.value)
