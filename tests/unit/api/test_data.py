"""
Unit test for EventData class
"""
from __future__ import annotations

import pytest

from featurebyte.api.data import Data
from featurebyte.api.dimension_data import DimensionData
from featurebyte.api.event_data import EventData
from featurebyte.api.item_data import ItemData
from featurebyte.api.scd_data import SlowlyChangingData
from featurebyte.exception import RecordRetrievalException


def test_get_event_data(saved_event_data, snowflake_event_data):
    """
    Test Data.get function to retrieve EventData
    """
    # load the event data from the persistent
    loaded_event_data = Data.get(snowflake_event_data.name)
    assert loaded_event_data.saved is True
    assert loaded_event_data == snowflake_event_data
    assert EventData.get_by_id(id=snowflake_event_data.id) == snowflake_event_data

    with pytest.raises(RecordRetrievalException) as exc:
        lazy_event_data = Data.get("unknown_event_data")
        _ = lazy_event_data.name
    expected_msg = (
        'Data (name: "unknown_event_data") not found. ' "Please save the Data object first."
    )
    assert expected_msg in str(exc.value)


def test_get_item_data(snowflake_item_data, saved_item_data):
    """
    Test Data.get function to retrieve ItemData
    """
    # load the item data from the persistent
    loaded_data = Data.get(saved_item_data.name)
    assert loaded_data.saved is True
    assert loaded_data == snowflake_item_data
    assert ItemData.get_by_id(id=loaded_data.id) == snowflake_item_data

    with pytest.raises(RecordRetrievalException) as exc:
        lazy_item_data = Data.get("unknown_item_data")
        _ = lazy_item_data.name
    expected_msg = (
        'Data (name: "unknown_item_data") not found. ' "Please save the Data object first."
    )
    assert expected_msg in str(exc.value)


def test_get_scd_data(saved_scd_data, snowflake_scd_data):
    """
    Test Data.get function to retrieve SlowlyChangingData
    """
    # load the scd data from the persistent
    loaded_scd_data = Data.get(snowflake_scd_data.name)
    assert loaded_scd_data.saved is True
    assert loaded_scd_data == snowflake_scd_data
    assert SlowlyChangingData.get_by_id(id=snowflake_scd_data.id) == snowflake_scd_data

    with pytest.raises(RecordRetrievalException) as exc:
        lazy_scd_data = Data.get("unknown_scd_data")
        _ = lazy_scd_data.name
    expected_msg = (
        'Data (name: "unknown_scd_data") not found. ' "Please save the Data object first."
    )
    assert expected_msg in str(exc.value)


def test_get_dimension_data(saved_dimension_data, snowflake_dimension_data):
    """
    Test Data.get function to retrieve DimensionData
    """
    # load the dimension data from the persistent
    loaded_scd_data = Data.get(snowflake_dimension_data.name)
    assert loaded_scd_data.saved is True
    assert loaded_scd_data == snowflake_dimension_data
    assert DimensionData.get_by_id(id=snowflake_dimension_data.id) == snowflake_dimension_data

    with pytest.raises(RecordRetrievalException) as exc:
        lazy_dimension_data = Data.get("unknown_dimension_data")
        _ = lazy_dimension_data.name
    expected_msg = (
        'Data (name: "unknown_dimension_data") not found. ' "Please save the Data object first."
    )
    assert expected_msg in str(exc.value)
