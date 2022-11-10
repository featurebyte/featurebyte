"""
FeatureListNamespace API route controller
"""
from __future__ import annotations

from bson.objectid import ObjectId

from featurebyte.models.feature_list import FeatureListNamespaceModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.feature_list_namespace import (
    FeatureListNamespaceList,
    FeatureListNamespaceServiceUpdate,
    FeatureListNamespaceUpdate,
)
from featurebyte.schema.info import FeatureListNamespaceInfo
from featurebyte.service.default_version_mode import DefaultVersionModeService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.info import InfoService


class FeatureListNamespaceController(
    BaseDocumentController[
        FeatureListNamespaceModel, FeatureListNamespaceService, FeatureListNamespaceList
    ]
):
    """
    FeatureList controller
    """

    paginated_document_class = FeatureListNamespaceList

    def __init__(
        self,
        service: FeatureListNamespaceService,
        default_version_mode_service: DefaultVersionModeService,
        info_service: InfoService,
    ):
        super().__init__(service)
        self.default_version_mode_service = default_version_mode_service
        self.info_service = info_service

    async def update_feature_list_namespace(
        self,
        feature_list_namespace_id: ObjectId,
        data: FeatureListNamespaceUpdate,
    ) -> FeatureListNamespaceModel:
        """
        Update FeatureListNamespace stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        feature_list_namespace_id: ObjectId
            FeatureListNamespace ID
        data: FeatureListNamespaceUpdate
            FeatureListNamespace update payload

        Returns
        -------
        FeatureListNamespaceModel
            FeatureListNamespace object with updated attribute(s)
        """
        if data.default_version_mode:
            await self.default_version_mode_service.update_feature_list_namespace(
                feature_list_namespace_id=feature_list_namespace_id,
                default_version_mode=data.default_version_mode,
                return_document=False,
            )
        if data.status:
            await self.service.update_document(
                document_id=feature_list_namespace_id,
                data=FeatureListNamespaceServiceUpdate(status=data.status),
                return_document=False,
            )
        return await self.get(document_id=feature_list_namespace_id)

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> FeatureListNamespaceInfo:
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
        info_document = await self.info_service.get_feature_list_namespace_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
