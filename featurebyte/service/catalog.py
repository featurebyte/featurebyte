"""
CatalogService class
"""

from __future__ import annotations

from typing import Any, Optional

from bson import ObjectId

from featurebyte.models.catalog import CatalogModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.schema.catalog import CatalogCreate, CatalogOnlineStoreUpdate, CatalogServiceUpdate
from featurebyte.schema.worker.task.online_store_initialize import (
    CatalogOnlineStoreInitializeTaskPayload,
)
from featurebyte.service.base_document import BaseDocumentService


class CatalogService(BaseDocumentService[CatalogModel, CatalogCreate, CatalogServiceUpdate]):
    """
    CatalogService class
    """

    document_class = CatalogModel

    def _construct_get_query_filter(
        self, document_id: ObjectId, use_raw_query_filter: bool = False, **kwargs: Any
    ) -> QueryFilter:
        query_filter = super()._construct_get_query_filter(
            document_id=document_id, use_raw_query_filter=use_raw_query_filter, **kwargs
        )
        if not use_raw_query_filter:
            query_filter["is_deleted"] = {"$ne": True}
        return query_filter

    def construct_list_query_filter(
        self,
        query_filter: Optional[QueryFilter] = None,
        use_raw_query_filter: bool = False,
        **kwargs: Any,
    ) -> QueryFilter:
        query_filter = super().construct_list_query_filter(
            query_filter=query_filter, use_raw_query_filter=use_raw_query_filter, **kwargs
        )
        if not use_raw_query_filter:
            query_filter["is_deleted"] = {"$ne": True}
        return query_filter

    async def soft_delete(
        self,
        document_id: ObjectId,
    ) -> None:
        """
        Soft delete document

        Parameters
        ----------
        document_id: ObjectId
            ID of document to delete
        """
        await self.update_document(
            document_id=document_id,
            data=CatalogServiceUpdate(is_deleted=True),
            return_document=False,
        )

    async def update_online_store(
        self,
        document_id: ObjectId,
        data: CatalogOnlineStoreUpdate,
    ) -> None:
        """
        Update catalog online store at persistent

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        data: CatalogOnlineStoreUpdate
            Catalog online store update payload object
        """
        document = await self.get_document(document_id=document_id)

        # update document to persistent
        await self._update_document(
            document=document,
            update_dict=data.dict(),
            update_document_class=None,
        )

    async def get_update_online_store_task_payload(
        self,
        document_id: ObjectId,
        data: CatalogOnlineStoreUpdate,
    ) -> Optional[CatalogOnlineStoreInitializeTaskPayload]:
        """
        Get OnlineStoreInitializeTask payload

        Parameters
        ----------
        document_id: ObjectId
            Catalog id
        data: CatalogOnlineStoreUpdate
            Online store update payload

        Returns
        -------
        Optional[CatalogOnlineStoreInitializeTaskPayload]
        """
        document = await self.get_document(document_id=document_id)
        if document.online_store_id == data.online_store_id:
            return None
        return CatalogOnlineStoreInitializeTaskPayload(
            user_id=self.user.id,
            catalog_id=document_id,
            online_store_id=data.online_store_id,
            output_document_id=document.id,
        )


class AllCatalogService(CatalogService):
    """
    AllCatalogService class
    """

    @property
    def is_catalog_specific(self) -> bool:
        return False
