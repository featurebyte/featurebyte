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
    FeatureNamespaceCreate,
    FeatureNamespaceInfo,
    FeatureNamespaceList,
    FeatureNamespaceServiceUpdate,
    FeatureNamespaceUpdate,
)
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
    async def create_feature_namespace(
        cls, user: Any, persistent: Persistent, data: FeatureNamespaceCreate
    ) -> FeatureNamespaceModel:
        """
        Create Feature Namespace at persistent

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that feature namespace will be saved to
        data: FeatureNamespaceCreate
            FeatureNamespace creation payload

        Returns
        -------
        FeatureNamespaceModel
            Newly created feature store document
        """
        document = await cls.document_service_class(
            user=user, persistent=persistent
        ).create_document(data)
        return document

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
        document = await cls.document_service_class(
            user=user, persistent=persistent
        ).update_document(
            document_id=feature_namespace_id,
            data=FeatureNamespaceServiceUpdate(**data.dict()),
        )
        assert document is not None
        return document
