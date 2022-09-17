"""
FeatureNamespace API route controller
"""
from __future__ import annotations

from typing import Any, Type

from bson.objectid import ObjectId

from featurebyte.models.feature import FeatureNamespaceModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseDocumentController, GetInfoControllerMixin
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceInfo,
    FeatureNamespaceList,
    FeatureNamespaceUpdate,
)
from featurebyte.service.default_version_mode import DefaultVersionModeService
from featurebyte.service.feature_namespace import FeatureNamespaceService


class FeatureNamespaceController(
    BaseDocumentController[FeatureNamespaceModel, FeatureNamespaceList],
    GetInfoControllerMixin[FeatureNamespaceInfo],
):
    """
    FeatureName controller
    """

    paginated_document_class = FeatureNamespaceList
    document_service_class: Type[FeatureNamespaceService] = FeatureNamespaceService  # type: ignore[assignment]

    @classmethod
    async def update_feature_namespace(
        cls,
        user: Any,
        persistent: Persistent,
        feature_namespace_id: ObjectId,
        data: FeatureNamespaceUpdate,
    ) -> FeatureNamespaceModel:
        """
        Update FeatureNamespace stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
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
            default_version_mode_service = DefaultVersionModeService(
                user=user, persistent=persistent
            )
            await default_version_mode_service.update_feature_default_version_mode(
                feature_namespace_id=feature_namespace_id,
                default_version_mode=data.default_version_mode,
                return_document=False,
            )
        return await cls.get(user=user, persistent=persistent, document_id=feature_namespace_id)
