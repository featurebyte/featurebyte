"""
Tests for WarehouseTableService
"""

from datetime import datetime

import pytest
import pytest_asyncio
from bson import ObjectId
from freezegun import freeze_time
from sqlglot import parse_one

from featurebyte.query_graph.node.schema import TableDetails


@pytest.fixture(name="service")
def service_fixture(app_container):
    """
    WarehouseTableService fixture
    """
    return app_container.warehouse_table_service


@pytest.fixture(name="feature_store_id")
def feature_store_id_fixture():
    """
    Feature store id fixture
    """
    return ObjectId()


@pytest.fixture(name="table_name")
def table_name_fixture() -> str:
    """
    Table name fixture
    """
    return "temp_tile_table"


@pytest_asyncio.fixture(name="saved_warehouse_table")
@freeze_time("2021-01-01 10:00:00")
async def saved_warehouse_table_fixture(
    service, mock_snowflake_session, feature_store_id, table_name
):
    """
    Save a warehouse table model
    """
    warehouse_table = await service.create_table_as_with_session(
        session=mock_snowflake_session,
        feature_store_id=feature_store_id,
        tag="my_tag",
        time_to_live_seconds=86400,
        table_details=table_name,
        select_expr=parse_one("SELECT * FROM MY_TABLE"),
    )
    return warehouse_table


def test_create_table_as_with_session(saved_warehouse_table, feature_store_id):
    """
    Test create_table_as_with_session method
    """
    assert saved_warehouse_table.location.model_dump() == {
        "feature_store_id": feature_store_id,
        "table_details": {
            "database_name": "sf_db",
            "schema_name": "sf_schema",
            "table_name": "temp_tile_table",
        },
    }
    assert saved_warehouse_table.expires_at == datetime(2021, 1, 2, 10, 0, 0)
    assert saved_warehouse_table.tag == "my_tag"
    assert saved_warehouse_table.warehouse_tables == [
        TableDetails(**{
            "database_name": "sf_db",
            "schema_name": "sf_schema",
            "table_name": "temp_tile_table",
        })
    ]


@pytest.mark.asyncio
async def test_get_warehouse_table_by_location(service, saved_warehouse_table):
    """
    Test get_warehouse_table_by_location method
    """
    location = saved_warehouse_table.location
    warehouse_table = await service.get_warehouse_table_by_location(location)
    assert warehouse_table == saved_warehouse_table


@pytest.mark.asyncio
async def test_drop_table_with_session(
    service, saved_warehouse_table, feature_store_id, table_name, mock_snowflake_session
):
    """
    Test drop_table_with_session method
    """
    # Check that the table exists
    warehouse_table = await service.get_warehouse_table_by_location(saved_warehouse_table.location)
    assert warehouse_table is not None

    # Drop the table
    await service.drop_table_with_session(
        session=mock_snowflake_session,
        feature_store_id=feature_store_id,
        table_name=table_name,
    )

    # Check that the table no longer exists
    warehouse_table = await service.get_warehouse_table_by_location(warehouse_table.location)
    assert warehouse_table is None


@pytest.mark.asyncio
async def test_list_warehouse_tables_by_tag(service, saved_warehouse_table):
    """
    Test list_warehouse_tables_by_tag method
    """
    result = [
        warehouse_table async for warehouse_table in service.list_warehouse_tables_by_tag("my_tag")
    ]
    assert len(result) == 1
    assert result[0] == saved_warehouse_table

    result = [
        warehouse_table
        async for warehouse_table in service.list_warehouse_tables_by_tag("non_existent_tag")
    ]
    assert len(result) == 0


@pytest.mark.asyncio
@freeze_time("2021-01-02 12:00:00")  # After expiration
async def test_list_warehouse_tables_due_for_cleanup(
    service, saved_warehouse_table, feature_store_id
):
    """
    Test list_warehouse_tables_due_for_cleanup method
    """
    # The saved_warehouse_table expires at 2021-01-02 10:00:00
    # Current time is 2021-01-02 12:00:00, so it should be included
    result = [
        warehouse_table
        async for warehouse_table in service.list_warehouse_tables_due_for_cleanup(feature_store_id)
    ]
    assert len(result) == 1
    assert result[0] == saved_warehouse_table


@pytest.mark.asyncio
@freeze_time("2021-01-01 08:00:00")  # Before expiration
async def test_list_warehouse_tables_due_for_cleanup_not_expired(
    service, saved_warehouse_table, feature_store_id
):
    """
    Test list_warehouse_tables_due_for_cleanup method when tables are not expired
    """
    # The saved_warehouse_table expires at 2021-01-02 10:00:00
    # Current time is 2021-01-01 08:00:00, so it should not be included
    result = [
        warehouse_table
        async for warehouse_table in service.list_warehouse_tables_due_for_cleanup(feature_store_id)
    ]
    assert len(result) == 0
