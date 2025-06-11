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

        update_dict = data.model_dump()

        # Handle fields that should not be updated if None (fields that should not be unset; for
        # example, online_store_id can be unset)
        if update_dict["populate_offline_feature_tables"] is None:
            update_dict.pop("populate_offline_feature_tables")

        # update document to persistent
        await self._update_document(
            document=document,
            update_dict=update_dict,
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
        payload = CatalogOnlineStoreInitializeTaskPayload(
            user_id=self.user.id,
            catalog_id=document_id,
            online_store_id=data.online_store_id,
            populate_offline_feature_tables=data.populate_offline_feature_tables,
            output_document_id=document.id,
        )
        # Only return task payload if there is work to do (i.e., online store is being set or unset
        # or populate_offline_feature_tables is being set or unset)
        if data.online_store_id != document.online_store_id:
            return payload
        if (
            data.populate_offline_feature_tables is not None
            and data.populate_offline_feature_tables != document.populate_offline_feature_tables
        ):
            return payload
        return None


class AllCatalogService(CatalogService):
    """
    AllCatalogService class
    """

    @property
    def is_catalog_specific(self) -> bool:
        return False
