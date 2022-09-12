"""
EntityService class
"""
from __future__ import annotations

from typing import Any

from bson.objectid import ObjectId

from featurebyte.models.base import UniqueConstraintResolutionSignature
from featurebyte.models.entity import EntityModel
from featurebyte.schema.entity import EntityCreate, EntityInfo, EntityUpdate
from featurebyte.service.base_document import BaseDocumentService, GetInfoServiceMixin


class EntityService(BaseDocumentService[EntityModel], GetInfoServiceMixin[EntityInfo]):
    """
    EntityService class
    """

    document_class = EntityModel

    async def create_document(  # type: ignore[override]
        self, data: EntityCreate, get_credential: Any = None
    ) -> EntityModel:
        _ = get_credential
        document = EntityModel(
            **data.json_dict(), user_id=self.user.id, serving_names=[data.serving_name]
        )

        # check any conflict with existing documents
        await self._check_document_unique_constraints(document=document)
        insert_id = await self.persistent.insert_one(
            collection_name=self.collection_name,
            document=document.dict(by_alias=True),
            user_id=self.user.id,
        )
        assert insert_id == document.id
        return await self.get_document(document_id=insert_id)

    async def update_document(  # type: ignore[override]
        self, document_id: ObjectId, data: EntityUpdate
    ) -> EntityModel:
        # check any conflict with existing documents
        await self._check_document_unique_constraint(
            query_filter={"name": data.name},
            conflict_signature={"name": data.name},
            resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
        )
        query_filter = self._construct_get_query_filter(document_id=document_id)
        await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter=query_filter,
            update={"$set": {"name": data.name}},
            user_id=self.user.id,
        )
        return await self.get_document(document_id=document_id)

    async def get_info(self, document_id: ObjectId, verbose: bool) -> EntityInfo:
        _ = verbose
        entity = await self.get_document(document_id=document_id)
        return EntityInfo(
            name=entity.name,
            created_at=entity.created_at,
            updated_at=entity.updated_at,
            serving_names=entity.serving_names,
        )
