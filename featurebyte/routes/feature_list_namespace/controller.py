"""
FeatureListNamespace API route controller
"""
from __future__ import annotations

from typing import Any, Type

from bson.objectid import ObjectId

from featurebyte.models.feature_list import FeatureListNamespaceModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.feature_list_namespace import (
    FeatureListNamespaceCreate,
    FeatureListNamespaceList,
    FeatureListNamespaceUpdate,
)
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService


class FeatureListNamespaceController(
    BaseDocumentController[FeatureListNamespaceModel, FeatureListNamespaceList]
):
    """
    FeatureList controller
    """

    paginated_document_class = FeatureListNamespaceList
    document_service_class: Type[FeatureListNamespaceService] = FeatureListNamespaceService  # type: ignore[assignment]

    @classmethod
    async def create_feature_list_namespace(
        cls,
        user: Any,
        persistent: Persistent,
        get_credential: Any,
        data: FeatureListNamespaceCreate,
    ) -> FeatureListNamespaceModel:
        """
        Create FeatureListNamespace at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that feature list will be saved to
        get_credential: Any
            Get credential handler function
        data: FeatureListNamespaceCreate
            Feature list namespace creation payload

        Returns
        -------
        FeatureListNamespaceModel
            Newly created feature list namespace object
        """
        async with cls._creation_context():
            document = await cls.document_service_class(
                user=user, persistent=persistent
            ).create_document(data=data, get_credential=get_credential)
            return document

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
        async with cls._update_context():
            document = await cls.document_service_class(
                user=user, persistent=persistent
            ).update_document(document_id=feature_list_namespace_id, data=data)
            return document
