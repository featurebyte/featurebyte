"""
Tests for FeatureStoreTableCleanupService
"""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from freezegun import freeze_time

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.warehouse_table import WarehouseTableModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.table import TableSpec
from featurebyte.query_graph.node.schema import TableDetails


@pytest.fixture(name="service")
def service_fixture(app_container):
    """
    FeatureStoreTableCleanupService fixture
    """
    return app_container.feature_store_table_cleanup_service


@pytest.fixture(name="warehouse_table_service")
def warehouse_table_service_fixture(app_container):
    """
    WarehouseTableService fixture
    """
    return app_container.warehouse_table_service


@pytest.fixture(name="mock_session_manager", autouse=True)
def mock_session_manager_fixture(service):
    """
    Mock only the session manager dependency - feature store get_document can work normally
    """
    mock_session = AsyncMock()

    with patch.object(
        service.session_manager_service, "get_feature_store_session", return_value=mock_session
    ):
        yield mock_session


@pytest_asyncio.fixture(name="expired_warehouse_table")
@freeze_time("2021-01-01 10:00:00")
async def expired_warehouse_table_fixture(warehouse_table_service, feature_store):
    """
    Create an actual expired warehouse table document in the database
    """
    table = WarehouseTableModel(
        location=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="sf_db",
                schema_name="sf_schema",
                table_name="expired_test_table",
            ),
        ),
        tag="test_cleanup_tag",
        expires_at=datetime.utcnow() - timedelta(hours=1),  # expired 1 hour ago
        cleanup_failed_count=0,
    )
    return await warehouse_table_service.create_document(table)


@pytest_asyncio.fixture(name="non_expired_warehouse_table")
@freeze_time("2021-01-01 10:00:00")
async def non_expired_warehouse_table_fixture(warehouse_table_service, feature_store):
    """
    Create an actual non-expired warehouse table document in the database
    """
    table = WarehouseTableModel(
        location=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="sf_db",
                schema_name="sf_schema",
                table_name="non_expired_test_table",
            ),
        ),
        tag="test_cleanup_tag",
        expires_at=datetime.utcnow() + timedelta(hours=1),  # expires in 1 hour
        cleanup_failed_count=0,
    )
    return await warehouse_table_service.create_document(table)


@pytest.mark.asyncio
@freeze_time("2021-01-01 10:00:00")
async def test_run_cleanup_success(
    service,
    warehouse_table_service,
    feature_store,
    expired_warehouse_table,
    non_expired_warehouse_table,
):
    """
    Test run_cleanup method successfully cleans up expired tables
    """
    # Before cleanup - both tables should exist
    assert await warehouse_table_service.get_document(expired_warehouse_table.id) is not None
    assert await warehouse_table_service.get_document(non_expired_warehouse_table.id) is not None

    # Run cleanup
    await service.run_cleanup(feature_store_id=feature_store.id)

    # After cleanup - expired table should be deleted, non-expired should remain
    with pytest.raises(DocumentNotFoundError):
        await warehouse_table_service.get_document(expired_warehouse_table.id)
    assert await warehouse_table_service.get_document(non_expired_warehouse_table.id) is not None


@pytest.mark.asyncio
@freeze_time("2021-01-01 10:00:00")
async def test_run_cleanup_with_drop_failure_increments_counter(
    service,
    warehouse_table_service,
    feature_store,
    expired_warehouse_table,
    mock_session_manager,
):
    """
    Test run_cleanup method when drop_table_with_session fails - should increment failure counter
    """
    with patch.object(
        warehouse_table_service,
        "drop_table_with_session",
        side_effect=Exception("Database connection failed"),
    ) as mock_drop:
        # Before cleanup - table should exist with 0 failures
        original_table = await warehouse_table_service.get_document(expired_warehouse_table.id)
        assert original_table is not None
        assert original_table.cleanup_failed_count == 0

        # Run cleanup - should not raise exception even though drop fails
        await service.run_cleanup(feature_store_id=feature_store.id)

        # Verify drop_table_with_session was called with correct parameters including exists=True
        mock_drop.assert_called_once_with(
            session=mock_session_manager,
            warehouse_table=expired_warehouse_table,
            exists=True,
        )

        # After cleanup - document should still exist with incremented failure counter
        table_after_cleanup = await warehouse_table_service.get_document(expired_warehouse_table.id)
        assert table_after_cleanup is not None
        assert table_after_cleanup.id == expired_warehouse_table.id
        assert table_after_cleanup.cleanup_failed_count == 1


@pytest.mark.asyncio
@freeze_time("2021-01-01 10:00:00")
async def test_run_cleanup_force_delete_after_max_failures(
    service,
    warehouse_table_service,
    feature_store,
    mock_session_manager,
):
    """
    Test run_cleanup method force deletes document after max failures
    """
    # Create a table with 5 failed attempts (at the limit)
    table = WarehouseTableModel(
        location=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="sf_db",
                schema_name="sf_schema",
                table_name="max_failures_table",
            ),
        ),
        tag="test_cleanup_tag",
        expires_at=datetime(2021, 1, 1, 9, 0, 0),  # expired 1 hour ago from frozen time
        cleanup_failed_count=4,  # One away from max
    )
    warehouse_table = await warehouse_table_service.create_document(table)

    with patch.object(
        warehouse_table_service,
        "drop_table_with_session",
        side_effect=Exception("Credentials no longer valid"),
    ) as mock_drop:
        # Before cleanup - table should exist
        assert await warehouse_table_service.get_document(warehouse_table.id) is not None

        # Run cleanup - this should force delete the document
        await service.run_cleanup(feature_store_id=feature_store.id)

        # Verify drop_table_with_session was called with correct parameters including exists=True
        mock_drop.assert_called_once_with(
            session=mock_session_manager,
            warehouse_table=warehouse_table,
            exists=True,
        )

        # After cleanup - document should be force deleted
        with pytest.raises(DocumentNotFoundError):
            await warehouse_table_service.get_document(warehouse_table.id)


def test_is_temp_table(service):
    """
    Test temp table detection heuristics
    """
    # Should detect temp tables (case insensitive)
    assert service.is_temp_table("__temp_something")
    assert service.is_temp_table("__TEMP_SOMETHING")
    assert service.is_temp_table("my_table__temp")
    assert service.is_temp_table("__TEMP_FEATURE_QUERY_abc123")
    assert service.is_temp_table("__temp_feature_query_xyz")
    assert service.is_temp_table("table__temp_suffix")

    # Should not detect regular tables
    assert not service.is_temp_table("regular_table")
    assert not service.is_temp_table("my_feature_table")
    assert not service.is_temp_table("data_temp")  # contains "temp" but not the pattern
    assert not service.is_temp_table("temporary_table")  # different pattern
    assert not service.is_temp_table("temp_tile_table")  # doesn't contain "__TEMP"


def test_make_table_key(service):
    """
    Test table key generation with case insensitivity
    """
    # Test case insensitivity
    key1 = service._make_table_key("SF_DB", "SF_SCHEMA", "MY_TABLE")
    key2 = service._make_table_key("sf_db", "sf_schema", "my_table")
    key3 = service._make_table_key("Sf_Db", "Sf_Schema", "My_Table")

    assert key1 == key2 == key3
    assert key1 == ("SF_DB", "SF_SCHEMA", "MY_TABLE")

    # Test with None values
    key_with_none = service._make_table_key(None, "schema", "table")
    assert key_with_none == ("", "SCHEMA", "TABLE")

    # Test empty strings
    key_with_empty = service._make_table_key("", "schema", "table")
    assert key_with_empty == ("", "SCHEMA", "TABLE")


@pytest.mark.asyncio
@freeze_time("2021-01-01 10:00:00")
async def test_discover_and_adopt_orphaned_tables(
    service,
    warehouse_table_service,
    feature_store,
    mock_session_manager,
):
    """
    Test discovery and adoption of orphaned temp tables
    """

    # Mock session.list_tables to return some tables including temp tables
    mock_session = AsyncMock()
    mock_session.database_name = "sf_db"
    mock_session.schema_name = "sf_schema"
    mock_session.list_tables.return_value = [
        TableSpec(name="regular_table"),
        TableSpec(name="__temp_orphaned_1"),
        TableSpec(name="__TEMP_FEATURE_QUERY_123"),
        TableSpec(name="__temp_tile_orphaned"),
        TableSpec(name="__temp_tracked"),  # This one will already have a record
    ]

    # Mock the session manager to return our mock session
    service.session_manager_service.get_feature_store_session = AsyncMock(return_value=mock_session)

    # Create a tracked table record for one of the temp tables

    existing_tracked_table = WarehouseTableModel(
        location=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="sf_db",
                schema_name="sf_schema",
                table_name="__temp_tracked",
            ),
        ),
        tag="existing_tag",
        expires_at=datetime(2021, 1, 2, 10, 0, 0),
        cleanup_failed_count=0,
    )
    await warehouse_table_service.create_document(existing_tracked_table)

    # Run discovery
    adopted_count = await service.discover_and_adopt_orphaned_tables(feature_store.id)

    # Should adopt 3 orphaned temp tables (excluding the regular table and already tracked table)
    assert adopted_count == 3

    # Verify the adopted tables were created with correct attributes
    orphaned_tables = []
    async for table in warehouse_table_service.list_warehouse_tables_by_tag("orphaned_temp_table"):
        orphaned_tables.append(table)

    assert len(orphaned_tables) == 3

    # Check that all adopted tables have 72-hour expiration and correct tag
    expected_expiry = datetime(2021, 1, 4, 10, 0, 0)  # 72 hours from frozen time
    for table in orphaned_tables:
        assert table.tag == "orphaned_temp_table"
        assert table.expires_at == expected_expiry
        assert table.cleanup_failed_count == 0
        assert table.location.feature_store_id == feature_store.id
        assert table.location.table_details.database_name == "sf_db"
        assert table.location.table_details.schema_name == "sf_schema"
        assert table.location.table_details.table_name in [
            "__temp_orphaned_1",
            "__TEMP_FEATURE_QUERY_123",
            "__temp_tile_orphaned",
        ]


@pytest.mark.asyncio
@freeze_time("2021-01-01 10:00:00")
async def test_run_cleanup_no_expired_tables(
    service,
    warehouse_table_service,
    feature_store,
    non_expired_warehouse_table,
):
    """
    Test run_cleanup method when no tables are expired
    """
    # Before cleanup - table should exist
    assert await warehouse_table_service.get_document(non_expired_warehouse_table.id) is not None

    # Run cleanup
    await service.run_cleanup(feature_store_id=feature_store.id)

    # After cleanup - table should still exist (not expired)
    assert await warehouse_table_service.get_document(non_expired_warehouse_table.id) is not None
