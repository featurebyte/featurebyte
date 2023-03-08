"""
Unit test for EventData class
"""
from __future__ import annotations

from unittest.mock import patch

import pytest
import pytest_asyncio

from featurebyte.api.data import Data
from featurebyte.api.dimension_data import DimensionData
from featurebyte.api.entity import Entity
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

    # load the event data use get_by_id
    loaded_data = Data.get_by_id(snowflake_event_data.id)
    assert loaded_data == loaded_event_data

    with pytest.raises(RecordRetrievalException) as exc:
        Data.get("unknown_event_data")
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
        Data.get("unknown_item_data")
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
        Data.get("unknown_scd_data")
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
        Data.get("unknown_dimension_data")
    expected_msg = (
        'Data (name: "unknown_dimension_data") not found. ' "Please save the Data object first."
    )
    assert expected_msg in str(exc.value)


@pytest_asyncio.fixture(name="mock_list_columns")
async def mock_list_columns_fixture():
    with patch(
        "featurebyte.routes.feature_store.controller.FeatureStoreController.list_columns"
    ) as mock_list_columns:
        yield mock_list_columns


def test_update__schema_validation(saved_event_data, mock_api_client_fixture):
    """
    Test update data won't make addition API calls for schema validation
    """
    mock_request = mock_api_client_fixture
    cust_entity = Entity(name="customer", serving_names=["cust_id"])
    cust_entity.save()

    # check that call update won't make additional snowflake query (post feature store list columns) due to
    # (1) columns_info is available
    # (2) _validate_schema not set
    api_count = mock_request.call_count
    saved_event_data.cust_id.as_entity(cust_entity.name)
    assert mock_request.call_count == api_count + 2
    # get entity call
    assert mock_request.call_args_list[api_count][0][0] == "GET"
    assert mock_request.call_args_list[api_count][0][1].endswith("/entity")
    # patch event data columns_info call
    assert mock_request.call_args_list[api_count + 1][0][0] == "PATCH"
    assert mock_request.call_args_list[api_count + 1][0][1].endswith(
        f"/event_data/{saved_event_data.id}"
    )

    # check the case when there is an additional post call
    api_count = mock_request.call_count
    # use .columns to make sure actual call happens (due to proxy object)
    _ = EventData.get(name=saved_event_data.name).columns
    assert mock_request.call_count == api_count + 3
    # get event_data
    assert mock_request.call_args_list[api_count][0][0] == "GET"
    assert mock_request.call_args_list[api_count][0][1].endswith("/event_data")
    # get feature store
    assert mock_request.call_args_list[api_count + 1][0][0] == "GET"
    assert mock_request.call_args_list[api_count + 1][0][1].endswith(
        f"/feature_store/{saved_event_data.feature_store.id}"
    )
    # post feature store table schema
    assert mock_request.call_args_list[api_count + 2][0][0] == "POST"
    assert mock_request.call_args_list[api_count + 2][0][1].endswith(
        "feature_store/column?database_name=sf_database&schema_name=sf_schema&table_name=sf_table"
    )
