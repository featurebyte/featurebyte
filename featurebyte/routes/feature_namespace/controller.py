"""
FeatureNamespace API route controller
"""
from __future__ import annotations

from bson.objectid import ObjectId

from featurebyte.models.feature import FeatureNamespaceModel
from featurebyte.routes.common.base import BaseDocumentController, GetInfoControllerMixin
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceInfo,
    FeatureNamespaceList,
    FeatureNamespaceUpdate,
)
from featurebyte.service.default_version_mode import DefaultVersionModeService
from featurebyte.service.feature_namespace import FeatureNamespaceService


class FeatureNamespaceController(  # type: ignore[misc]
    BaseDocumentController[FeatureNamespaceModel, FeatureNamespaceList],
    GetInfoControllerMixin[FeatureNamespaceInfo],
):
    """
    FeatureName controller
    """

    paginated_document_class = FeatureNamespaceList

    def __init__(
        self,
        service: FeatureNamespaceService,
        default_version_mode_service: DefaultVersionModeService,
    ):
        super().__init__(service)  # type: ignore[arg-type]
        self.service = service  # type: ignore[assignment]
        self.default_version_mode_service = default_version_mode_service

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
