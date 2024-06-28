"""
OnlineStore API route controller
"""

from __future__ import annotations

from typing import Any, List, Tuple, cast

from bson import ObjectId

from featurebyte.logging import get_logger
from featurebyte.models.online_store import OnlineStoreModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.info import OnlineStoreInfo
from featurebyte.schema.online_store import OnlineStoreCreate, OnlineStoreList, OnlineStoreUpdate
from featurebyte.service.catalog import AllCatalogService
from featurebyte.service.online_store import OnlineStoreService

logger = get_logger(__name__)


class OnlineStoreController(
    BaseDocumentController[OnlineStoreModel, OnlineStoreService, OnlineStoreList]
):
    """
    OnlineStore controller
    """

    paginated_document_class = OnlineStoreList

    def __init__(
        self,
        online_store_service: OnlineStoreService,
        all_catalog_service: AllCatalogService,
    ):
        super().__init__(online_store_service)
        self.all_catalog_service = all_catalog_service

    async def create_online_store(
        self,
        data: OnlineStoreCreate,
    ) -> OnlineStoreModel:
        """
        Create Online Store at persistent

        Parameters
        ----------
        data: OnlineStoreCreate
            OnlineStore creation payload

        Returns
        -------
        OnlineStoreModel
            Newly created feature store document
        """
        document = await self.service.create_document(data)
        return document

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return [(self.all_catalog_service, {"online_store_id": document_id})]

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> OnlineStoreInfo:
        """
        Get document info given document ID

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        InfoDocument
        """
        info_document = await self.service.get_online_store_info(
            document_id=document_id, verbose=verbose
        )
        return info_document

    async def update_online_store(
        self, online_store_id: ObjectId, data: OnlineStoreUpdate
    ) -> OnlineStoreModel:
        """
        Update online store

        Parameters
        ----------
        online_store_id: ObjectId
            OnlineStore ID
        data: OnlineStoreUpdate
            OnlineStore update payload

        Returns
        -------
        OnlineStoreModel
            Updated online store document
        """
        document = cast(OnlineStoreModel, await self.service.update_document(online_store_id, data))
        return document
