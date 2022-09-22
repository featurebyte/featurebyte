"""
FeatureStore API route controller
"""
from __future__ import annotations

from typing import Type

from featurebyte.models.feature_store import FeatureStoreModel
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

    def __init__(self, service: FeatureStoreService):
        self.service = service

    async def create_feature_store(
        self,
        data: FeatureStoreCreate,
    ) -> FeatureStoreModel:
        """
        Create Feature Store at persistent

        Parameters
        ----------
        data: FeatureStoreCreate
            FeatureStore creation payload

        Returns
        -------
        FeatureStoreModel
            Newly created feature store document
        """
        document = await self.service.create_document(data)
        return document
