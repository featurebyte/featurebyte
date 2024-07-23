"""
CatalogService class
"""

from __future__ import annotations

from typing import Optional

from bson import ObjectId

from featurebyte.models.catalog import CatalogModel
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
            update_dict=data.model_dump(),
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
