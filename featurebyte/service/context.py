"""
ContextService class
"""
from __future__ import annotations

from typing import Optional

from bson import ObjectId

from featurebyte.models.context import ContextModel
from featurebyte.schema.context import ContextCreate, ContextUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.entity import EntityService
from featurebyte.service.tabular_data import DataService


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

    async def update_document(
        self,
        document_id: ObjectId,
        data: ContextUpdate,
        exclude_none: bool = True,
        document: Optional[ContextModel] = None,
        return_document: bool = True,
    ) -> Optional[ContextModel]:
        if data.graph and data.node_name:
            node = data.graph.get_node_by_name(data.node_name)
            operation_structure = data.graph.extract_operation_structure(node=node)

            data_service = DataService(user=self.user, persistent=self.persistent)

        document = await super().update_document(
            document_id=document_id,
            data=data,
            exclude_none=exclude_none,
            return_document=return_document,
        )
        return document
