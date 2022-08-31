"""
FeatureStoreService class
"""
from __future__ import annotations

from typing import Any

from bson.objectid import ObjectId

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.schema.feature_store import FeatureStoreCreate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.common.operation import DictProject, DictTransform


class FeatureStoreService(BaseDocumentService[FeatureStoreModel]):
    """
    FeatureStoreService class
    """

    document_class = FeatureStoreModel
    info_transform = DictTransform(
        rule={
            **BaseDocumentService.base_info_transform_rule,
            "__root__": DictProject(rule=["type", "details"]),
        }
    )

    async def create_document(
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
