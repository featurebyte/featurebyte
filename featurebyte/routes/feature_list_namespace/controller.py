"""
FeatureListNamespace API route controller
"""
from __future__ import annotations

from bson.objectid import ObjectId

from featurebyte.models.feature_list import FeatureListNamespaceModel
from featurebyte.routes.common.base import BaseDocumentController, GetInfoControllerMixin
from featurebyte.schema.feature_list_namespace import (
    FeatureListNamespaceInfo,
    FeatureListNamespaceList,
    FeatureListNamespaceServiceUpdate,
    FeatureListNamespaceUpdate,
)
from featurebyte.service.default_version_mode import DefaultVersionModeService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService


class FeatureListNamespaceController(  # type: ignore[misc]
    BaseDocumentController[FeatureListNamespaceModel, FeatureListNamespaceList],
    GetInfoControllerMixin[FeatureListNamespaceInfo],
):
    """
    FeatureList controller
    """

    paginated_document_class = FeatureListNamespaceList

    def __init__(
        self,
        service: FeatureListNamespaceService,
        default_version_mode_service: DefaultVersionModeService,
    ):
        super().__init__(service)  # type: ignore[arg-type]
        self.service = service  # type: ignore[assignment]
        self.default_version_mode_service = default_version_mode_service

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
