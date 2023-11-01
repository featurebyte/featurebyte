"""
CatalogService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson import ObjectId

from featurebyte.models.catalog import CatalogModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.schema.catalog import CatalogCreate, CatalogServiceUpdate
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


class AllCatalogService(CatalogService):
    """
    AllCatalogService class
    """

    @property
    def is_catalog_specific(self) -> bool:
        return False
