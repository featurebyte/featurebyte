"""
Deployment migration service
"""

from typing import List

from bson import ObjectId

from featurebyte.logging import get_logger
from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import BaseDocumentServiceT, BaseMongoCollectionMigration
from featurebyte.persistent import Persistent
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.feature_list import FeatureListService

logger = get_logger(__name__)


class DeploymentMigrationServiceV14(BaseMongoCollectionMigration):
    """
    DeploymentMigrationService class

    This class is used to migrate the deployment records.
    """

    # skip audit migration for this migration
    skip_audit_migration = True

    def __init__(
        self,
        persistent: Persistent,
        deployment_service: DeploymentService,
        feature_list_service: FeatureListService,
    ):
        super().__init__(persistent)
        self.deployment_service = deployment_service
        self.feature_list_service = feature_list_service

    @property
    def delegate_service(self) -> BaseDocumentServiceT:
        return self.deployment_service  # type: ignore[return-value]

    @migrate(
        version=15,
        description="Move store info from feature list to deployment record.",
    )
    async def move_store_info_from_feature_list_to_deployment(self) -> None:
        """Move store info from feature list to deployment record"""
        query_filter = await self.delegate_service.construct_list_query_filter()
        total = await self.get_total_record(query_filter=query_filter)
        sample_ids: List[ObjectId] = []

        async for deployment_dict in self.delegate_service.list_documents_as_dict_iterator(
            query_filter=query_filter
        ):
            feature_list_dict = await self.feature_list_service.get_document_as_dict(
                deployment_dict["feature_list_id"]
            )
            store_info = feature_list_dict.get("store_info")
            if store_info:
                await self.delegate_service.update_documents(
                    query_filter={"_id": deployment_dict["_id"]},
                    update={"$set": {"store_info": store_info}},
                )
                if len(sample_ids) < 10:
                    sample_ids.append(deployment_dict["_id"])

        # check that store info is migrated successfully
        async for deployment in self.delegate_service.list_documents_iterator(
            query_filter={"_id": {"$in": sample_ids}}
        ):
            assert deployment.store_info, deployment  # type: ignore[attr-defined]

        logger.info("Migrated all records successfully (total: %d)", total)
