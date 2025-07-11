"""
ManagedView API routes
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
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
    VerboseQuery,
)
from featurebyte.routes.managed_view.controller import ManagedViewController
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.managed_view import (
    ManagedViewCreate,
    ManagedViewInfo,
    ManagedViewList,
    ManagedViewRead,
    ManagedViewUpdate,
)


class ManagedViewRouter(
    BaseApiRouter[ManagedViewRead, ManagedViewList, ManagedViewCreate, ManagedViewController]
):
    """
    Feature Store API router
    """

    object_model = ManagedViewRead
    list_object_model = ManagedViewList
    create_object_schema = ManagedViewCreate
    controller = ManagedViewController

    def __init__(self) -> None:
        super().__init__("/managed_view")

        self.router.add_api_route(
            "/{managed_view_id}",
            self.update_managed_view,
            methods=["PATCH"],
            response_model=self.object_model,
        )
        self.router.add_api_route(
            "/{managed_view_id}/info",
            self.get_managed_view_info,
            methods=["GET"],
            response_model=ManagedViewInfo,
        )

    async def create_object(
        self,
        request: Request,
        data: ManagedViewCreate,
    ) -> ManagedViewRead:
        """
        Create Feature Store
        """
        controller = request.state.app_container.managed_view_controller
        managed_view = await controller.create_managed_view(data=data)
        return ManagedViewRead(**managed_view.model_dump(by_alias=True))

    async def get_object(
        self, request: Request, managed_view_id: PydanticObjectId
    ) -> ManagedViewRead:
        managed_view = await super().get_object(request, managed_view_id)
        return ManagedViewRead(**managed_view.model_dump(by_alias=True))

    async def list_objects(
        self,
        request: Request,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = SortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
        name: Optional[str] = NameQuery,
        feature_store_id: Optional[PydanticObjectId] = None,
    ) -> ManagedViewList:
        """
        List objects
        """
        controller = self.get_controller_for_request(request)
        return await controller.list_managed_views(
            page=page,
            page_size=page_size,
            sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
            search=search,
            name=name,
            feature_store_id=feature_store_id,
        )

    async def list_audit_logs(
        self,
        request: Request,
        managed_view_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        return await super().list_audit_logs(
            request,
            managed_view_id,
            page,
            page_size,
            sort_by,
            sort_dir,
            search,
        )

    @staticmethod
    async def update_managed_view(
        request: Request, managed_view_id: PydanticObjectId, data: ManagedViewUpdate
    ) -> ManagedViewRead:
        """
        Update online store
        """
        controller = request.state.app_container.managed_view_controller
        document = await controller.update_managed_view(managed_view_id, data)
        return ManagedViewRead(**document.model_dump(by_alias=True))

    async def update_description(
        self, request: Request, managed_view_id: PydanticObjectId, data: DescriptionUpdate
    ) -> ManagedViewRead:
        managed_view = await super().update_description(request, managed_view_id, data)
        return ManagedViewRead(**managed_view.model_dump(by_alias=True))

    @staticmethod
    async def get_managed_view_info(
        request: Request,
        managed_view_id: PydanticObjectId,
        verbose: bool = VerboseQuery,
    ) -> ManagedViewInfo:
        """
        Retrieve ManagedView info
        """
        controller = request.state.app_container.managed_view_controller
        info = await controller.get_info(
            document_id=ObjectId(managed_view_id),
            verbose=verbose,
        )
        return cast(ManagedViewInfo, info)

    async def delete_object(
        self, request: Request, managed_view_id: PydanticObjectId
    ) -> DeleteResponse:
        controller = self.get_controller_for_request(request)
        await controller.delete(document_id=ObjectId(managed_view_id))
        return DeleteResponse()
