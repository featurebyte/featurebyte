"""
FeatureStoreService class
"""

from __future__ import annotations

from typing import Type

from bson import ObjectId

from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.schema.feature_store import FeatureStoreCreate
from featurebyte.schema.info import FeatureStoreInfo
from featurebyte.service.base_document import BaseDocumentService


class FeatureStoreService(
    BaseDocumentService[FeatureStoreModel, FeatureStoreCreate, BaseDocumentServiceUpdateSchema],
):
    """
    FeatureStoreService class
    """

    document_class: Type[FeatureStoreModel] = FeatureStoreModel

    async def get_feature_store_info(
        self, document_id: ObjectId, verbose: bool
    ) -> FeatureStoreInfo:
        """
        Get feature store info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        FeatureStoreInfo
        """
        _ = verbose
        feature_store = await self.get_document(document_id=document_id)
        return FeatureStoreInfo(
            name=feature_store.name,
            created_at=feature_store.created_at,
            updated_at=feature_store.updated_at,
            source=feature_store.type,
            database_details=feature_store.details,
            description=feature_store.description,
        )
