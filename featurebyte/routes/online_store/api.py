"""
OnlineStore API routes
"""

from __future__ import annotations

from typing import Optional, cast

from bson import ObjectId
from fastapi import Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseApiRouter
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortDirQuery,
    VerboseQuery,
)
from featurebyte.routes.online_store.controller import OnlineStoreController
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.info import OnlineStoreInfo
from featurebyte.schema.online_store import (
    OnlineStoreCreate,
    OnlineStoreList,
    OnlineStoreRead,
    OnlineStoreUpdate,
)


class OnlineStoreRouter(
    BaseApiRouter[OnlineStoreRead, OnlineStoreList, OnlineStoreCreate, OnlineStoreController]
):
    """
    Feature Store API router
    """

    object_model = OnlineStoreRead
    list_object_model = OnlineStoreList
    create_object_schema = OnlineStoreCreate
    controller = OnlineStoreController

    def __init__(self) -> None:
        super().__init__("/online_store")
        self.router.add_api_route(
            "/{online_store_id}/info",
            self.get_online_store_info,
            methods=["GET"],
            response_model=OnlineStoreInfo,
        )
        # Update online store
        self.router.add_api_route(
            "/{online_store_id}",
            self.update_online_store,
            methods=["PATCH"],
            response_model=self.object_model,
        )

    async def create_object(
        self,
        request: Request,
        data: OnlineStoreCreate,
    ) -> OnlineStoreRead:
        """
        Create Feature Store
        """
        controller = request.state.app_container.online_store_controller
        online_store = await controller.create_online_store(data=data)
        return OnlineStoreRead(**online_store.model_dump(by_alias=True))

    async def get_object(
        self, request: Request, online_store_id: PydanticObjectId
    ) -> OnlineStoreRead:
        online_store = await super().get_object(request, online_store_id)
        return OnlineStoreRead(**online_store.model_dump(by_alias=True))

    async def list_audit_logs(
        self,
        request: Request,
        online_store_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        return await super().list_audit_logs(
            request,
            online_store_id,
            page,
            page_size,
            sort_by,
            sort_dir,
            search,
        )

    @staticmethod
    async def update_online_store(
        request: Request, online_store_id: PydanticObjectId, data: OnlineStoreUpdate
    ) -> OnlineStoreRead:
        """
        Update online store
        """
        controller = request.state.app_container.online_store_controller
        document = await controller.update_online_store(online_store_id, data)
        return OnlineStoreRead(**document.model_dump(by_alias=True))

    async def update_description(
        self, request: Request, online_store_id: PydanticObjectId, data: DescriptionUpdate
    ) -> OnlineStoreRead:
        online_store = await super().update_description(request, online_store_id, data)
        return OnlineStoreRead(**online_store.model_dump(by_alias=True))

    @staticmethod
    async def get_online_store_info(
        request: Request,
        online_store_id: PydanticObjectId,
        verbose: bool = VerboseQuery,
    ) -> OnlineStoreInfo:
        """
        Retrieve OnlineStore info
        """
        controller = request.state.app_container.online_store_controller
        info = await controller.get_info(
            document_id=ObjectId(online_store_id),
            verbose=verbose,
        )
        return cast(OnlineStoreInfo, info)

    async def delete_object(
        self, request: Request, online_store_id: PydanticObjectId
    ) -> DeleteResponse:
        controller = self.get_controller_for_request(request)
        await controller.delete(document_id=ObjectId(online_store_id))
        return DeleteResponse()
