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
async def test_run_cleanup_with_drop_failure(
    service,
    warehouse_table_service,
    feature_store,
    expired_warehouse_table,
):
    """
    Test run_cleanup method when drop_table_with_session fails - document should NOT be deleted
    """
    with patch.object(
        warehouse_table_service,
        "drop_table_with_session",
        side_effect=Exception("Database connection failed"),
    ):
        # Before cleanup - table should exist
        assert await warehouse_table_service.get_document(expired_warehouse_table.id) is not None

        # Run cleanup - should not raise exception even though drop fails
        await service.run_cleanup(feature_store_id=feature_store.id)

        # After cleanup - document should still exist because drop failed
        table_after_cleanup = await warehouse_table_service.get_document(expired_warehouse_table.id)
        assert table_after_cleanup is not None
        assert table_after_cleanup.id == expired_warehouse_table.id


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
