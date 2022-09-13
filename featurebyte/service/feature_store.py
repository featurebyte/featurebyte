"""
FeatureStoreService class
"""
from __future__ import annotations

from typing import Any, Type

from bson.objectid import ObjectId

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.schema.feature_store import FeatureStoreCreate, FeatureStoreInfo
from featurebyte.service.base_document import BaseDocumentService, GetInfoServiceMixin


class FeatureStoreService(
    BaseDocumentService[FeatureStoreModel], GetInfoServiceMixin[FeatureStoreInfo]
):
    """
    FeatureStoreService class
    """

    document_class: Type[FeatureStoreModel] = FeatureStoreModel

    async def create_document(  # type: ignore[override]
        self, data: FeatureStoreCreate, get_credential: Any = None
    ) -> FeatureStoreModel:
        _ = get_credential
        document = FeatureStoreModel(**data.json_dict(), user_id=self.user.id)

        # check any conflict with existing documents
        await self._check_document_unique_constraints(document=document)
        insert_id = await self.persistent.insert_one(
            collection_name=self.collection_name,
            document=document.dict(by_alias=True),
            user_id=self.user.id,
        )
        assert insert_id == document.id
        return await self.get_document(document_id=insert_id)

    async def update_document(
        self, document_id: ObjectId, data: FeatureByteBaseModel
    ) -> FeatureStoreModel:
        # TODO: implement proper logic to update feature store document
        return await self.get_document(document_id=document_id)

    async def get_info(self, document_id: ObjectId, verbose: bool) -> FeatureStoreInfo:
        _ = verbose
        feature_store = await self.get_document(document_id=document_id)
        return FeatureStoreInfo(
            name=feature_store.name,
            created_at=feature_store.created_at,
            updated_at=feature_store.updated_at,
            source=feature_store.type,
            database_details=feature_store.details,
        )
