"""
Tests for FeatureStoreTableCleanupService
"""

from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest
import pytest_asyncio
from bson import ObjectId
from freezegun import freeze_time

from featurebyte.models.warehouse_table import WarehouseTableModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails


@pytest.fixture(name="service")
def service_fixture(app_container):
    """
    FeatureStoreTableCleanupService fixture
    """
    return app_container.feature_store_table_cleanup_service


@pytest.fixture(name="feature_store_id")
def feature_store_id_fixture():
    """
    Feature store id fixture
    """
    return ObjectId()


@pytest_asyncio.fixture(name="expired_warehouse_table")
@freeze_time("2021-01-01 10:00:00")
async def expired_warehouse_table_fixture(feature_store_id):
    """
    Create an expired warehouse table model
    """
    return WarehouseTableModel(
        location=TabularSource(
            feature_store_id=feature_store_id,
            table_details=TableDetails(
                database_name="test_db",
                schema_name="test_schema",
                table_name="expired_table",
            ),
        ),
        tag="test_tag",
        expires_at=datetime(2020, 12, 31, 10, 0, 0),  # expired
    )


@pytest.mark.asyncio
@freeze_time("2021-01-01 10:00:00")
async def test_run_cleanup(service, feature_store_id, expired_warehouse_table):
    """
    Test run_cleanup method
    """
    # Mock the warehouse table service to return the expired table
    with (
        patch.object(
            service.warehouse_table_service, "list_warehouse_tables_due_for_cleanup"
        ) as mock_list,
        patch.object(service.warehouse_table_service, "drop_table_with_session") as mock_drop,
    ):
        # Configure mock to yield the expired table
        async def mock_async_generator():
            yield expired_warehouse_table

        mock_list.return_value = mock_async_generator()
        mock_drop.return_value = None

        # Mock feature store service
        mock_feature_store = Mock()
        service.feature_store_service.get_document = AsyncMock(return_value=mock_feature_store)

        # Mock session manager
        mock_session = Mock()
        service.session_manager_service.get_feature_store_session = AsyncMock(
            return_value=mock_session
        )

        # Run cleanup
        await service.run_cleanup(feature_store_id=feature_store_id)

        # Verify calls
        mock_list.assert_called_once_with(feature_store_id)
        mock_drop.assert_called_once_with(
            session=mock_session,
            feature_store_id=feature_store_id,
            table_name="expired_table",
            schema_name="test_schema",
            database_name="test_db",
        )


@pytest.mark.asyncio
@freeze_time("2021-01-01 10:00:00")
async def test_run_cleanup_with_exception(service, feature_store_id, expired_warehouse_table):
    """
    Test run_cleanup method when drop_table_with_session raises an exception
    """
    # Mock the warehouse table service to return the expired table
    with (
        patch.object(
            service.warehouse_table_service, "list_warehouse_tables_due_for_cleanup"
        ) as mock_list,
        patch.object(service.warehouse_table_service, "drop_table_with_session") as mock_drop,
    ):
        # Configure mock to yield the expired table
        async def mock_async_generator():
            yield expired_warehouse_table

        mock_list.return_value = mock_async_generator()
        mock_drop.side_effect = Exception("Table not found")

        # Mock feature store service
        mock_feature_store = Mock()
        service.feature_store_service.get_document = AsyncMock(return_value=mock_feature_store)

        # Mock session manager
        mock_session = Mock()
        service.session_manager_service.get_feature_store_session = AsyncMock(
            return_value=mock_session
        )

        # Run cleanup - should not raise exception
        await service.run_cleanup(feature_store_id=feature_store_id)

        # Verify calls
        mock_list.assert_called_once_with(feature_store_id)
        mock_drop.assert_called_once_with(
            session=mock_session,
            feature_store_id=feature_store_id,
            table_name="expired_table",
            schema_name="test_schema",
            database_name="test_db",
        )
