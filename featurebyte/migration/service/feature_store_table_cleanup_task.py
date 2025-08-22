"""
Feature store migration service
"""

from typing import List

from featurebyte.logging import get_logger
from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import (
    BaseDocumentServiceT,
    BaseMongoCollectionMigration,
)
from featurebyte.models.base import User
from featurebyte.models.persistent import Document
from featurebyte.persistent import Persistent
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_table_cleanup_scheduler import (
    FeatureStoreTableCleanupSchedulerService,
)
from featurebyte.service.task_manager import TaskManager

logger = get_logger(__name__)


class FeatureStoreTableCleanupTaskMigrationServiceV22(BaseMongoCollectionMigration):
    """
    FeatureStoreTableCleanupTaskMigrationServiceV22 class

    This class is used to schedule table cleanup tasks for existing feature stores.
    """

    # skip audit migration for this migration since we don't modify documents
    skip_audit_migration = True

    def __init__(
        self,
        persistent: Persistent,
        feature_store_service: FeatureStoreService,
        feature_store_table_cleanup_scheduler_service: FeatureStoreTableCleanupSchedulerService,
    ):
        super().__init__(persistent)
        self.feature_store_service = feature_store_service
        self.feature_store_table_cleanup_scheduler_service = (
            feature_store_table_cleanup_scheduler_service
        )

    @property
    def delegate_service(self) -> BaseDocumentServiceT:
        return self.feature_store_service  # type: ignore[return-value]

    async def batch_preprocess_document(self, documents: List[Document]) -> List[Document]:
        """
        Process feature stores to schedule cleanup tasks without modifying the documents

        Parameters
        ----------
        documents: List[Document]
            Feature store documents

        Returns
        -------
        List[Document]
            Unmodified documents (we don't change feature store documents)
        """
        scheduled_count = 0

        for document in documents:
            feature_store_id = document["_id"]
            feature_store_user_id = document["user_id"]

            # Create a scheduler service with the actual feature store owner's user ID
            # (similar to how DataWarehouseMigrationMixin handles user override)
            feature_store_user = User(id=feature_store_user_id)

            # Create TaskManager with user override
            original_task_manager = self.feature_store_table_cleanup_scheduler_service.task_manager
            task_manager_with_user_override = TaskManager(
                user=feature_store_user,
                persistent=self.persistent,
                celery=original_task_manager.celery,
                catalog_id=original_task_manager.catalog_id,
                storage=original_task_manager.storage,
                redis=original_task_manager.redis,
            )

            scheduler_service = FeatureStoreTableCleanupSchedulerService(
                persistent=self.persistent,
                user=feature_store_user,
                task_manager=task_manager_with_user_override,
            )

            await scheduler_service.start_job_if_not_exist(feature_store_id=feature_store_id)
            scheduled_count += 1
            logger.info(
                "Scheduled cleanup task for feature store with correct user",
                extra={
                    "feature_store_id": str(feature_store_id),
                    "user_id": str(feature_store_user_id),
                },
            )

        if scheduled_count > 0:
            logger.info(
                "Scheduled cleanup tasks for %d feature stores in batch",
                scheduled_count,
            )

        # Return an empty list since we don't need to modify the documents with migrate_record
        return []

    @migrate(
        version=22,
        description="Schedule FeatureStoreTableCleanupTask for existing feature stores.",
    )
    async def schedule_table_cleanup_tasks(self) -> None:
        """Schedule table cleanup tasks for all existing feature stores"""
        logger.info("Starting migration to schedule cleanup tasks for all feature stores")

        # Construct query filter to ensure catalog-specific migration
        query_filter = await self.delegate_service.construct_list_query_filter()

        await self.migrate_all_records(
            query_filter=query_filter,
            batch_preprocess_document_func=self.batch_preprocess_document,
        )
        logger.info("Completed scheduling cleanup tasks for all feature stores")
