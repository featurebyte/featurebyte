"""
EntityService class
"""
from __future__ import annotations

from typing import Any

from bson.objectid import ObjectId

from featurebyte.models.entity import EntityModel
from featurebyte.schema.entity import EntityCreate, EntityInfo, EntityServiceUpdate
from featurebyte.service.base_document import BaseDocumentService, GetInfoServiceMixin


class EntityService(
    BaseDocumentService[EntityModel, EntityCreate, EntityServiceUpdate],
    GetInfoServiceMixin[EntityInfo],
):
    """
    EntityService class
    """

    document_class = EntityModel

    @staticmethod
    def _extract_additional_creation_kwargs(data: EntityCreate) -> dict[str, Any]:
        return {"serving_names": [data.serving_name]}

    async def get_info(self, document_id: ObjectId, verbose: bool) -> EntityInfo:
        _ = verbose
        entity = await self.get_document(document_id=document_id)
        return EntityInfo(
            name=entity.name,
            created_at=entity.created_at,
            updated_at=entity.updated_at,
            serving_names=entity.serving_names,
        )
