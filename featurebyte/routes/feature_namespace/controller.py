"""
FeatureNamespace API route controller
"""
from __future__ import annotations

from bson.objectid import ObjectId

from featurebyte.models.feature import FeatureNamespaceModel
from featurebyte.routes.app_container import register_controller_constructor
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.feature_namespace import FeatureNamespaceList, FeatureNamespaceUpdate
from featurebyte.schema.info import FeatureNamespaceInfo
from featurebyte.service.default_version_mode import DefaultVersionModeService
from featurebyte.service.feature_namespace import FeatureNamespaceService
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
        info_service: InfoService,
    ):
        super().__init__(service)
        self.default_version_mode_service = default_version_mode_service
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
        """
        if data.default_version_mode:
            await self.default_version_mode_service.update_feature_namespace(
                feature_namespace_id=feature_namespace_id,
                default_version_mode=data.default_version_mode,
                return_document=False,
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


register_controller_constructor(
    FeatureNamespaceController, [FeatureNamespaceService, DefaultVersionModeService, InfoService]
)
