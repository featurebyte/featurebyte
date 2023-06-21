"""
DeploymentService class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.models.deployment import DeploymentModel
from featurebyte.persistent import Persistent
from featurebyte.schema.deployment import DeploymentUpdate
from featurebyte.schema.info import DeploymentInfo
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.feature_list import FeatureListService


class DeploymentService(BaseDocumentService[DeploymentModel, DeploymentModel, DeploymentUpdate]):
    """
    DeploymentService class
    """

    document_class = DeploymentModel
    document_update_class = DeploymentUpdate

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        feature_list_service: FeatureListService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.feature_list_service = feature_list_service

    async def get_deployment_info(self, document_id: ObjectId, verbose: bool) -> DeploymentInfo:
        """
        Get deployment info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        DeploymentInfo
        """
        _ = verbose
        deployment = await self.get_document(document_id=document_id)
        feature_list = await self.feature_list_service.get_document(
            document_id=deployment.feature_list_id
        )
        return DeploymentInfo(
            name=deployment.name,
            feature_list_name=feature_list.name,
            feature_list_version=feature_list.version.to_str(),
            num_feature=len(feature_list.feature_ids),
            enabled=deployment.enabled,
            serving_endpoint=(
                f"/deployment/{deployment.id}/online_features" if deployment.enabled else None
            ),
            created_at=deployment.created_at,
            updated_at=deployment.updated_at,
        )
