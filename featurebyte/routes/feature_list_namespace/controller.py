"""
FeatureListNamespace API route controller
"""
from __future__ import annotations

from typing import Any, Type

from bson.objectid import ObjectId

from featurebyte.models.feature_list import FeatureListNamespaceModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseDocumentController, GetInfoControllerMixin
from featurebyte.schema.feature_list_namespace import (
    FeatureListNamespaceInfo,
    FeatureListNamespaceList,
    FeatureListNamespaceServiceUpdate,
    FeatureListNamespaceUpdate,
)
from featurebyte.service.default_version_mode import DefaultVersionModeService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService


class FeatureListNamespaceController(
    BaseDocumentController[FeatureListNamespaceModel, FeatureListNamespaceList],
    GetInfoControllerMixin[FeatureListNamespaceInfo],
):
    """
    FeatureList controller
    """

    paginated_document_class = FeatureListNamespaceList
    document_service_class: Type[FeatureListNamespaceService] = FeatureListNamespaceService  # type: ignore[assignment]

    @classmethod
    async def update_feature_list_namespace(
        cls,
        user: Any,
        persistent: Persistent,
        feature_list_namespace_id: ObjectId,
        data: FeatureListNamespaceUpdate,
    ) -> FeatureListNamespaceModel:
        """
        Update FeatureListNamespace stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
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
            default_version_mode_service = DefaultVersionModeService(
                user=user, persistent=persistent
            )
            await default_version_mode_service.update_feature_list_namespace(
                feature_list_namespace_id=feature_list_namespace_id,
                default_version_mode=data.default_version_mode,
                return_document=False,
            )
        if data.status:
            await cls.document_service_class(user=user, persistent=persistent).update_document(
                document_id=feature_list_namespace_id,
                data=FeatureListNamespaceServiceUpdate(status=data.status),
                return_document=False,
            )
        return await cls.get(
            user=user, persistent=persistent, document_id=feature_list_namespace_id
        )
