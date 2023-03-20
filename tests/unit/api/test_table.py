"""
Unit test for Table class
"""
from __future__ import annotations

from unittest.mock import patch

import pytest
import pytest_asyncio

from featurebyte.api.dimension_table import DimensionTable
from featurebyte.api.entity import Entity
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


@pytest_asyncio.fixture(name="mock_list_columns")
async def mock_list_columns_fixture():
    with patch(
        "featurebyte.routes.feature_store.controller.FeatureStoreController.list_columns"
    ) as mock_list_columns:
        yield mock_list_columns


def test_update__schema_validation(saved_event_table, mock_api_client_fixture):
    """
    Test update table won't make addition API calls for schema validation
    """
    mock_request = mock_api_client_fixture
    cust_entity = Entity(name="customer", serving_names=["cust_id"])
    cust_entity.save()

    # check that call update won't make additional snowflake query (post feature store list columns) due to
    # (1) columns_info is available
    # (2) _validate_schema not set
    api_count = mock_request.call_count
    saved_event_table.cust_id.as_entity(cust_entity.name)
    assert mock_request.call_count == api_count + 2
    # get entity call
    assert mock_request.call_args_list[api_count][0][0] == "GET"
    assert mock_request.call_args_list[api_count][0][1].endswith("/entity")
    # patch event table columns_info call
    assert mock_request.call_args_list[api_count + 1][0][0] == "PATCH"
    assert mock_request.call_args_list[api_count + 1][0][1].endswith(
        f"/event_table/{saved_event_table.id}"
    )

    # check the case when there is an additional post call
    api_count = mock_request.call_count
    # use .columns to make sure actual call happens (due to proxy object)
    _ = EventTable.get(name=saved_event_table.name).columns
    assert mock_request.call_count == api_count + 3
    # get event_table
    assert mock_request.call_args_list[api_count][0][0] == "GET"
    assert mock_request.call_args_list[api_count][0][1].endswith("/event_table")
    # get feature store
    assert mock_request.call_args_list[api_count + 1][0][0] == "GET"
    assert mock_request.call_args_list[api_count + 1][0][1].endswith(
        f"/feature_store/{saved_event_table.feature_store.id}"
    )
    # post feature store table schema
    assert mock_request.call_args_list[api_count + 2][0][0] == "POST"
    assert mock_request.call_args_list[api_count + 2][0][1].endswith(
        "feature_store/column?database_name=sf_database&schema_name=sf_schema&table_name=sf_table"
    )
