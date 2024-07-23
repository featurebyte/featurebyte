"""
OnlineStoreService class
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Type

from bson import ObjectId
from redis import Redis

from featurebyte.models.online_store import OnlineStoreModel
from featurebyte.persistent import Persistent
from featurebyte.persistent.base import SortDir
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.schema.info import CatalogBriefInfo, OnlineStoreInfo
from featurebyte.schema.online_store import OnlineStoreCreate
from featurebyte.service.base_document import BaseDocumentService, DocumentUpdateSchema
from featurebyte.service.catalog import CatalogService
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE
from featurebyte.storage import Storage


class OnlineStoreService(
    BaseDocumentService[OnlineStoreModel, OnlineStoreCreate, BaseDocumentServiceUpdateSchema],
):
    """
    OnlineStoreService class
    """

    document_class: Type[OnlineStoreModel] = OnlineStoreModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        catalog_service: CatalogService,
        block_modification_handler: BlockModificationHandler,
        storage: Storage,
        redis: Redis[Any],
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
            storage=storage,
            redis=redis,
        )
        self.catalog_service = catalog_service

    async def create_document(self, data: OnlineStoreCreate) -> OnlineStoreModel:
        """
        Create document at persistent

        Parameters
        ----------
        data: OnlineStoreCreate
            OnlineStore creation payload object

        Returns
        -------
        OnlineStoreModel
        """
        online_store = self.document_class(**data.model_dump(by_alias=True))
        if online_store.details.credential:
            online_store.details.credential.encrypt_values()
        return await super().create_document(
            data=OnlineStoreCreate(**online_store.model_dump(by_alias=True))
        )

    async def get_online_store_info(self, document_id: ObjectId, verbose: bool) -> OnlineStoreInfo:
        """
        Get online store info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        OnlineStoreInfo
        """
        _ = verbose
        online_store = await self.get_document(document_id=document_id)
        results = await self.catalog_service.list_documents_as_dict(
            query_filter={"online_store_id": document_id}
        )
        return OnlineStoreInfo(
            name=online_store.name,
            created_at=online_store.created_at,
            updated_at=online_store.updated_at,
            details=online_store.details,
            description=online_store.description,
            catalogs=[CatalogBriefInfo(**doc).model_dump() for doc in results["data"]],
        )

    async def get_document_as_dict(
        self,
        document_id: ObjectId,
        exception_detail: str | None = None,
        use_raw_query_filter: bool = False,
        projection: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        document_dict = await super().get_document_as_dict(
            document_id=document_id,
            exception_detail=exception_detail,
            use_raw_query_filter=use_raw_query_filter,
            projection=projection,
            **kwargs,
        )
        document = self.document_class(**document_dict)
        if document.details.credential:
            document.details.credential.decrypt_values()
        return dict(document.model_dump(by_alias=True))

    async def list_documents_as_dict(
        self,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: Optional[list[tuple[str, SortDir]]] = None,
        use_raw_query_filter: bool = False,
        projection: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        sort_by = sort_by or [("created_at", "desc")]
        documents = await super().list_documents_as_dict(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            use_raw_query_filter=use_raw_query_filter,
            projection=projection,
            **kwargs,
        )
        for document_dict in documents["data"]:
            document = self.document_class(**document_dict)
            if document.details.credential:
                document.details.credential.decrypt_values()
            document_dict["details"] = document.details.model_dump(by_alias=True)
        return documents

    async def _update_document(
        self,
        document: OnlineStoreModel,
        update_dict: Dict[str, Any],
        update_document_class: Optional[Type[DocumentUpdateSchema]],
        skip_block_modification_check: bool = False,
    ) -> None:
        # encrypt credential if present in update_dict
        if "details" in update_dict:
            document_dict = document.model_dump(by_alias=True)
            document_dict.update(update_dict)
            new_document = self.document_class(**document_dict)
            if new_document.details.credential:
                new_document.details.credential.encrypt_values()
                update_dict["details"]["credential"] = new_document.details.credential.model_dump(
                    by_alias=True
                )

        return await super()._update_document(
            document=document,
            update_dict=update_dict,
            update_document_class=update_document_class,
            skip_block_modification_check=skip_block_modification_check,
        )
