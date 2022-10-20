"""
BaseDataDocumentService class
"""
from __future__ import annotations

from typing import Any, Optional, TypeVar

from bson.objectid import ObjectId

from featurebyte.models.feature_store import DataStatus
from featurebyte.schema.data import DataCreate, DataUpdate
from featurebyte.service.base_document import BaseDocumentService, Document
from featurebyte.service.feature_store import FeatureStoreService

DocumentCreate = TypeVar("DocumentCreate", bound=DataCreate)
DocumentUpdate = TypeVar("DocumentUpdate", bound=DataUpdate)


class BaseDataDocumentService(BaseDocumentService[Document, DocumentCreate, DocumentUpdate]):
    """
    BaseDataDocumentService class
    """

    async def create_document(self, data: DocumentCreate, get_credential: Any = None) -> Document:
        _ = get_credential
        _ = await FeatureStoreService(user=self.user, persistent=self.persistent).get_document(
            document_id=data.tabular_source.feature_store_id
        )
        document = self.document_class(
            user_id=self.user.id, status=DataStatus.DRAFT, **data.json_dict()
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

    async def update_document(
        self,
        document_id: ObjectId,
        data: DocumentUpdate,
        exclude_none: bool = True,
        document: Optional[Document] = None,
        return_document: bool = True,
    ) -> Optional[Document]:
        if document is None:
            await self.get_document(document_id=document_id)

        update_dict = data.dict(exclude_none=exclude_none)
        if data.columns_info:
            # do not exclude None in columns_info
            update_dict["columns_info"] = data.dict()["columns_info"]

        await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter=self._construct_get_query_filter(document_id=document_id),
            update={"$set": update_dict},
            user_id=self.user.id,
        )

        if return_document:
            return await self.get_document(document_id=document_id)
        return None
