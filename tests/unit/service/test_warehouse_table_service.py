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


@pytest.fixture(name="app_container_with_catalog")
def app_container_with_catalog_fixture(persistent, user, storage, temp_storage):
    """
    Return a function that creates an app container with a specific catalog_id.
    This allows tests to create multiple app containers with different catalog contexts.
    """
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from featurebyte.routes.lazy_app_container import LazyAppContainer
    from featurebyte.routes.registry import app_container_config
    from featurebyte.worker import get_celery
    from tests.unit.conftest import TEST_REDIS_URI

    def create_app_container(catalog_id):
        return LazyAppContainer(
            app_container_config=app_container_config,
            instance_map={
                "user": user,
                "persistent": persistent,
                "temp_storage": temp_storage,
                "celery": get_celery(),
                "storage": storage,
                "catalog_id": catalog_id,
                "task_id": uuid4(),
                "progress": AsyncMock(),
                "redis_uri": TEST_REDIS_URI,
            },
        )

    return create_app_container


@pytest.mark.asyncio
@freeze_time("2021-01-01 10:00:00")
async def test_list_warehouse_tables_due_for_cleanup_non_catalog_specific(
    app_container,
    app_container_with_catalog,
    feature_store,
):
    """
    Test that list_warehouse_tables_due_for_cleanup works across different catalog contexts
    since WarehouseTableModel is not catalog-specific.
    """
    from datetime import timedelta

    from featurebyte.models.warehouse_table import WarehouseTableModel
    from featurebyte.query_graph.model.common_table import TabularSource
    from featurebyte.query_graph.node.schema import TableDetails

    # Create app containers with different catalog IDs
    catalog_1_id = ObjectId("646f6c1c0ed28a5271fb02d1")
    catalog_2_id = ObjectId("646f6c1c0ed28a5271fb02d2")

    app_container_cat1 = app_container_with_catalog(catalog_1_id)
    app_container_cat2 = app_container_with_catalog(catalog_2_id)

    # Get warehouse table services from different catalog contexts
    warehouse_service_cat1 = app_container_cat1.warehouse_table_service
    warehouse_service_cat2 = app_container_cat2.warehouse_table_service

    # Create expired tables from different catalog contexts but same feature store
    table_cat1 = WarehouseTableModel(
        location=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="sf_db",
                schema_name="sf_schema",
                table_name="expired_table_cat1",
            ),
        ),
        tag="test_non_catalog_tag",
        expires_at=datetime.utcnow() - timedelta(hours=1),  # expired 1 hour ago
        cleanup_failed_count=0,
    )

    table_cat2 = WarehouseTableModel(
        location=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="sf_db",
                schema_name="sf_schema",
                table_name="expired_table_cat2",
            ),
        ),
        tag="test_non_catalog_tag",
        expires_at=datetime.utcnow() - timedelta(hours=1),  # expired 1 hour ago
        cleanup_failed_count=0,
    )

    # Create tables from different catalog contexts
    created_table_cat1 = await warehouse_service_cat1.create_document(table_cat1)
    created_table_cat2 = await warehouse_service_cat2.create_document(table_cat2)

    # Test that any warehouse service can see all tables since they're non-catalog-specific
    test_service = app_container.warehouse_table_service

    discovered_tables = []
    table_names = set()
    async for table in test_service.list_warehouse_tables_due_for_cleanup(feature_store.id):
        discovered_tables.append(table)
        table_names.add(table.location.table_details.table_name)

    # Should find both tables since WarehouseTableModel is not catalog-specific
    assert len(discovered_tables) == 2
    assert "expired_table_cat1" in table_names
    assert "expired_table_cat2" in table_names

    # Cleanup the test data
    await warehouse_service_cat1.delete_document(created_table_cat1.id)
    await warehouse_service_cat2.delete_document(created_table_cat2.id)
