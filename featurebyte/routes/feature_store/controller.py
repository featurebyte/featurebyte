"""
FeatureStore API route controller
"""
from __future__ import annotations

from typing import Any, Type

from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent.base import Persistent
from featurebyte.routes.common.base import BaseDocumentController, GetInfoControllerMixin
from featurebyte.schema.feature_store import FeatureStoreCreate, FeatureStoreInfo, FeatureStoreList
from featurebyte.service.feature_store import FeatureStoreService


class FeatureStoreController(
    BaseDocumentController[FeatureStoreModel, FeatureStoreList],
    GetInfoControllerMixin[FeatureStoreInfo],
):
    """
    FeatureStore controller
    """

    paginated_document_class = FeatureStoreList
    document_service_class: Type[FeatureStoreService] = FeatureStoreService  # type: ignore[assignment]

    @classmethod
    async def create_feature_store(
        cls,
        user: Any,
        persistent: Persistent,
        data: FeatureStoreCreate,
    ) -> FeatureStoreModel:
        """
        Create Feature Store at persistent

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that feature store will be saved to
        data: FeatureStoreCreate
            FeatureStore creation payload

        Returns
        -------
        FeatureStoreModel
            Newly created feature store document
        """
        document = await cls.document_service_class(
            user=user, persistent=persistent
        ).create_document(data)
        return document
