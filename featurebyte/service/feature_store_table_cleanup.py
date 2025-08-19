"""
FeatureStoreTableCleanupService class
"""

from __future__ import annotations

from bson import ObjectId

from featurebyte.logging import get_logger
from featurebyte.models.warehouse_table import WarehouseTableServiceUpdate
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.warehouse_table_service import WarehouseTableService

logger = get_logger(__name__)

# Maximum number of failed cleanup attempts before deleting the document
MAX_CLEANUP_FAILURES = 5


class FeatureStoreTableCleanupService:
    """
    FeatureStoreTableCleanupService is responsible for cleaning up temporary tables that are due
    for cleanup in the feature store warehouse across all catalogs.

    Unlike catalog-specific services, this cleanup operates across all catalogs for a given
    feature store to ensure comprehensive cleanup of warehouse tables.
    """

    def __init__(
        self,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        warehouse_table_service: WarehouseTableService,
    ):
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.warehouse_table_service = warehouse_table_service

    async def run_cleanup(self, feature_store_id: ObjectId) -> None:
        """
        Run cleanup on the feature store warehouse tables across all catalogs

        This method operates across all catalogs for the given feature store,
        using raw query filters to bypass catalog-specific filtering.

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store id
        """
        feature_store = await self.feature_store_service.get_document(document_id=feature_store_id)
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)

        logger.info(
            "Starting feature store table cleanup",
            extra={"feature_store_id": str(feature_store_id)},
        )

        cleanup_count = 0
        force_deleted_count = 0
        async for (
            warehouse_table
        ) in self.warehouse_table_service.list_warehouse_tables_due_for_cleanup(feature_store_id):
            table_details = warehouse_table.location.table_details
            logger.info(
                "Cleaning up warehouse table",
                extra={
                    "table_name": table_details.table_name,
                    "schema_name": table_details.schema_name,
                    "database_name": table_details.database_name,
                    "expires_at": warehouse_table.expires_at,
                    "failed_attempts": warehouse_table.cleanup_failed_count,
                },
            )

            try:
                await self.warehouse_table_service.drop_table_with_session(
                    session=db_session,
                    feature_store_id=feature_store_id,
                    table_name=table_details.table_name,
                    schema_name=table_details.schema_name,
                    database_name=table_details.database_name,
                    exists=True,
                )
                cleanup_count += 1
                logger.info(
                    "Successfully cleaned up warehouse table",
                    extra={
                        "table_name": table_details.table_name,
                        "failed_attempts": warehouse_table.cleanup_failed_count,
                    },
                )
            except Exception as ex:
                # Increment failure counter
                new_failed_count = warehouse_table.cleanup_failed_count + 1

                if new_failed_count >= MAX_CLEANUP_FAILURES:
                    # Too many failures - force delete the document
                    logger.warning(
                        "Force deleting warehouse table document after max cleanup failures",
                        extra={
                            "table_name": table_details.table_name,
                            "failed_attempts": new_failed_count,
                            "max_failures": MAX_CLEANUP_FAILURES,
                            "error": str(ex),
                        },
                    )
                    await self.warehouse_table_service.delete_document(
                        document_id=warehouse_table.id
                    )
                    force_deleted_count += 1
                else:
                    # Update failure counter and keep document
                    logger.warning(
                        "Failed to cleanup warehouse table - incrementing failure counter",
                        extra={
                            "table_name": table_details.table_name,
                            "failed_attempts": new_failed_count,
                            "max_failures": MAX_CLEANUP_FAILURES,
                            "error": str(ex),
                        },
                    )
                    await self.warehouse_table_service.update_document(
                        document_id=warehouse_table.id,
                        data=WarehouseTableServiceUpdate(cleanup_failed_count=new_failed_count),
                    )

        logger.info(
            "Completed feature store table cleanup",
            extra={
                "feature_store_id": str(feature_store_id),
                "tables_cleaned": cleanup_count,
                "force_deleted_count": force_deleted_count,
            },
        )
