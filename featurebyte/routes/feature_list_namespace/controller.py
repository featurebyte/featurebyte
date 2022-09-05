"""
FeatureListNamespace API route controller
"""
from __future__ import annotations

from typing import Any, Type

from featurebyte.models.feature_list import FeatureListNamespaceModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.feature_list_namespace import (
    FeatureListNamespaceCreate,
    FeatureListNamespaceList,
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
