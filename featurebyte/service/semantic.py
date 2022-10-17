"""
SemanticService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson.objectid import ObjectId

from featurebyte.models.semantic import SemanticModel
from featurebyte.schema.semantic import SemanticCreate, SemanticServiceUpdate
from featurebyte.service.base_document import BaseDocumentService


class SemanticService(BaseDocumentService[SemanticModel]):
    """
    SemanticService class
    """

    document_class = SemanticModel

    async def create_document(  # type: ignore[override]
        self, data: SemanticCreate, get_credential: Any = None
    ) -> SemanticModel:
        _ = get_credential
        document = SemanticModel(**data.json_dict(), user_id=self.user.id)

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
        self,
        document_id: ObjectId,
        data: SemanticServiceUpdate,
        exclude_none: bool = True,
        document: Optional[SemanticModel] = None,
        return_document: bool = True,
    ) -> Optional[SemanticModel]:
        if document is None:
            await self.get_document(document_id=document_id)

        await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter=self._construct_get_query_filter(document_id=document_id),
            update={"$set": data.dict(exclude_none=exclude_none)},
            user_id=self.user.id,
        )

        if return_document:
            return await self.get_document(document_id=document_id)
        return None
