"""
ContextService class
"""
from __future__ import annotations

from featurebyte.models.context import ContextModel
from featurebyte.schema.context import ContextCreate, ContextUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.entity import EntityService


class ContextService(BaseDocumentService[ContextModel, ContextCreate, ContextUpdate]):
    """
    ContextService class
    """

    document_class = ContextModel

    async def create_document(self, data: ContextCreate) -> ContextModel:
        entity_service = EntityService(user=self.user, persistent=self.persistent)
        entities = await entity_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": data.entity_ids}}
        )
        found_entity_ids = set(doc["_id"] for doc in entities["data"])
        not_found_entity_ids = set(data.entity_ids).difference(found_entity_ids)
        if not_found_entity_ids:
            # trigger entity not found error
            await entity_service.get_document(document_id=list(not_found_entity_ids)[0])
        return await super().create_document(data=data)
