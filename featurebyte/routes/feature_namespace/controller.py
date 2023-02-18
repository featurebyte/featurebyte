"""
FeatureNamespace API route controller
"""
from __future__ import annotations

from bson.objectid import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature import DefaultVersionMode, FeatureNamespaceModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceList,
    FeatureNamespaceServiceUpdate,
    FeatureNamespaceUpdate,
)
from featurebyte.schema.info import FeatureNamespaceInfo
from featurebyte.service.default_version_mode import DefaultVersionModeService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.info import InfoService


class FeatureNamespaceController(
    BaseDocumentController[FeatureNamespaceModel, FeatureNamespaceService, FeatureNamespaceList]
):
    """
    FeatureName controller
    """

    paginated_document_class = FeatureNamespaceList

    def __init__(
        self,
        service: FeatureNamespaceService,
        default_version_mode_service: DefaultVersionModeService,
        feature_readiness_service: FeatureReadinessService,
        info_service: InfoService,
    ):
        super().__init__(service)
        self.default_version_mode_service = default_version_mode_service
        self.feature_readiness_service = feature_readiness_service
        self.info_service = info_service

    async def update_feature_namespace(
        self,
        feature_namespace_id: ObjectId,
        data: FeatureNamespaceUpdate,
    ) -> FeatureNamespaceModel:
        """
        Update FeatureNamespace stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        feature_namespace_id: ObjectId
            FeatureNamespace ID
        data: FeatureNamespaceUpdate
            FeatureNamespace update payload

        Returns
        -------
        FeatureNamespaceModel
            FeatureNamespace object with updated attribute(s)

        Raises
        ------
        DocumentUpdateError
            When the new feature version creation fails
        """
        if data.default_version_mode:
            await self.default_version_mode_service.update_feature_namespace(
                feature_namespace_id=feature_namespace_id,
                default_version_mode=data.default_version_mode,
                return_document=False,
            )

        if data.default_feature_id:
            feature_namespace = await self.service.get_document(document_id=feature_namespace_id)
            if feature_namespace.default_version_mode != DefaultVersionMode.MANUAL:
                raise DocumentUpdateError(
                    "Cannot set default feature ID when default version mode is not MANUAL"
                )

            # update feature namespace default feature ID and update feature readiness
            await self.service.update_document(
                document_id=feature_namespace_id,
                data=FeatureNamespaceServiceUpdate(default_feature_id=data.default_feature_id),
            )
            await self.feature_readiness_service.update_feature_namespace(
                feature_namespace_id=feature_namespace_id
            )

        return await self.get(document_id=feature_namespace_id)

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> FeatureNamespaceInfo:
        """
        Get document info given document ID

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        InfoDocument
        """
        info_document = await self.info_service.get_feature_namespace_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
