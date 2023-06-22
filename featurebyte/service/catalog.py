"""
CatalogService class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.models.catalog import CatalogModel
from featurebyte.schema.catalog import CatalogCreate, CatalogServiceUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.mixin import SortDir


class CatalogService(BaseDocumentService[CatalogModel, CatalogCreate, CatalogServiceUpdate]):
    """
    CatalogService class
    """

    document_class = CatalogModel

    async def _ensure_default_catalog_available(self) -> None:
        """
        Ensure default catalog exists, and create it if it doesn't
        """
        try:
            await super().get_document(document_id=DEFAULT_CATALOG_ID)
        except DocumentNotFoundError:
            await super().create_document(
                CatalogCreate(_id=DEFAULT_CATALOG_ID, default_feature_store_ids=[], name="default")
            )

    async def create_document(self, data: CatalogCreate) -> CatalogModel:
        await self._ensure_default_catalog_available()
        return await super().create_document(data)

    async def get_document(
        self,
        document_id: ObjectId,
        exception_detail: str | None = None,
        use_raw_query_filter: bool = False,
        **kwargs: Any,
    ) -> CatalogModel:
        await self._ensure_default_catalog_available()
        return await super().get_document(
            document_id=document_id,
            exception_detail=exception_detail,
            use_raw_query_filter=use_raw_query_filter,
            **kwargs,
        )

    async def list_documents(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: SortDir = "desc",
        use_raw_query_filter: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        await self._ensure_default_catalog_available()
        return await super().list_documents(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            use_raw_query_filter=use_raw_query_filter,
            **kwargs,
        )
