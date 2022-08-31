"""
Feature API route controller
"""
from __future__ import annotations

from typing import Any, Type

from featurebyte.models.feature import FeatureModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.feature import FeatureCreate, FeatureList
from featurebyte.service.feature import FeatureService


class FeatureController(BaseDocumentController[FeatureModel, FeatureList]):
    """
    Feature controller
    """

    paginated_document_class = FeatureList
    document_service_class: Type[FeatureService] = FeatureService  # type: ignore[assignment]

    @classmethod
    async def create_feature(
        cls, user: Any, persistent: Persistent, get_credential: Any, data: FeatureCreate
    ) -> FeatureModel:
        """
        Create Feature at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that feature will be saved to
        get_credential: Any
            Get credential handler function
        data: FeatureCreate
            Feature creation payload

        Returns
        -------
        FeatureModel
            Newly created feature object
        """
        async with cls._creation_context():
            document = await cls.document_service_class(
                user=user, persistent=persistent
            ).create_document(data=data, get_credential=get_credential)
            return document
