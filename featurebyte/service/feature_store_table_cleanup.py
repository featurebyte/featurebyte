"""
FeatureStoreTableCleanupService class
"""

from __future__ import annotations

from datetime import datetime, timedelta

from bson import ObjectId

from featurebyte.logging import get_logger
from featurebyte.models.warehouse_table import WarehouseTableModel, WarehouseTableServiceUpdate
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.warehouse_table_service import WarehouseTableService

logger = get_logger(__name__)

# Maximum number of failed cleanup attempts before deleting the document
MAX_CLEANUP_FAILURES = 5

# Orphaned tables get 72-hour grace period before cleanup
ORPHANED_TABLE_TTL_HOURS = 72


class FeatureStoreTableCleanupService:
    """
    FeatureStoreTableCleanupService is responsible for cleaning up temporary tables that are due
    for cleanup in the feature store warehouse.

    Since WarehouseTableModel is not catalog-specific, this service naturally operates
    across all catalogs for a given feature store.
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

    @staticmethod
    def _make_table_key(
        database_name: str, schema_name: str, table_name: str
    ) -> tuple[str, str, str]:
        """
        Create a case-insensitive table key for lookup

        Parameters
        ----------
        database_name: str
            Database name
        schema_name: str
            Schema name
        table_name: str
            Table name

        Returns
        -------
        tuple[str, str, str]
            Case-insensitive table key tuple
        """
        return (
            database_name.upper() if database_name else "",
            schema_name.upper() if schema_name else "",
            table_name.upper(),
        )

    @staticmethod
    def is_temp_table(table_name: str) -> bool:
        """
        Detect if a table is likely a temporary table using heuristics

        Parameters
        ----------
        table_name: str
            Name of the table to check

        Returns
        -------
        bool
            True if the table appears to be a temporary table
        """
        return table_name.upper().startswith("__TEMP")

    async def discover_and_adopt_orphaned_tables(self, feature_store_id: ObjectId) -> int:
        """
        Discover orphaned temp tables in the warehouse and create tracking records

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store ID

        Returns
        -------
        int
            Number of orphaned tables adopted
        """
        feature_store = await self.feature_store_service.get_document(document_id=feature_store_id)
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)

        logger.info(
            "Starting orphaned table discovery",
            extra={"feature_store_id": str(feature_store_id)},
        )

        adopted_count = 0

        # Get all tracked tables for this feature store in one query
        tracked_tables = {}
        query_filter = {"location.feature_store_id": feature_store_id}
        async for warehouse_table in self.warehouse_table_service.list_documents_iterator(
            query_filter=query_filter
        ):
            table_details = warehouse_table.location.table_details
            # Create a case-insensitive key that uniquely identifies the table
            table_key = self._make_table_key(
                table_details.database_name or db_session.database_name,
                table_details.schema_name or db_session.schema_name,
                table_details.table_name,
            )
            tracked_tables[table_key] = warehouse_table

        # List all tables in the working schema
        tables = await db_session.list_tables(
            database_name=db_session.database_name,
            schema_name=db_session.schema_name,
        )

        for table_spec in tables:
            if self.is_temp_table(table_spec.name):
                table_key = self._make_table_key(
                    db_session.database_name,
                    db_session.schema_name,
                    table_spec.name,
                )

                # Check if we already track this table using in-memory lookup
                if table_key not in tracked_tables:
                    # Convert TableSpec to TableDetails
                    table_details = TableDetails(
                        database_name=db_session.database_name,
                        schema_name=db_session.schema_name,
                        table_name=table_spec.name,
                    )
                    location = TabularSource(
                        feature_store_id=feature_store_id,
                        table_details=table_details,
                    )

                    await self._adopt_orphaned_table(location)
                    adopted_count += 1

                    logger.info(
                        "Adopted orphaned temp table",
                        extra={
                            "table_name": table_spec.name,
                            "schema_name": db_session.schema_name,
                            "database_name": db_session.database_name,
                        },
                    )

        logger.info(
            "Completed orphaned table discovery",
            extra={
                "feature_store_id": str(feature_store_id),
                "adopted_count": adopted_count,
            },
        )

        return adopted_count

    async def _adopt_orphaned_table(self, location: TabularSource) -> None:
        """
        Create tracking record for orphaned table with grace period expiration

        Parameters
        ----------
        location: TabularSource
            Location of the orphaned table
        """
        warehouse_table = WarehouseTableModel(
            location=location,
            tag="orphaned_temp_table",
            expires_at=datetime.utcnow() + timedelta(hours=ORPHANED_TABLE_TTL_HOURS),
            cleanup_failed_count=0,
        )
        await self.warehouse_table_service.create_document(warehouse_table)

    async def run_cleanup(self, feature_store_id: ObjectId) -> None:
        """
        Run cleanup on the feature store warehouse tables

        Since WarehouseTableModel is not catalog-specific, this method naturally
        operates across all catalogs for the given feature store.

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

        # First, discover and adopt any orphaned temp tables
        adopted_count = await self.discover_and_adopt_orphaned_tables(feature_store_id)

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
                    warehouse_table=warehouse_table,
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
                    # Too many failures: force delete the document
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
                "orphaned_tables_adopted": adopted_count,
            },
        )
