"""
ExposureNamespace API routes
"""

from __future__ import annotations

from typing import Optional

from fastapi import Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.exposure_namespace import ExposureNamespaceModel
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
from featurebyte.routes.exposure_namespace.controller import ExposureNamespaceController
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.exposure_namespace import (
    ExposureNamespaceCreate,
    ExposureNamespaceInfo,
    ExposureNamespaceList,
    ExposureNamespaceUpdate,
)


class ExposureNamespaceRouter(
    BaseApiRouter[
        ExposureNamespaceModel,
        ExposureNamespaceList,
        ExposureNamespaceCreate,
        ExposureNamespaceController,
    ]
):
    """
    Exposure namespace router
    """

    object_model = ExposureNamespaceModel
    list_object_model = ExposureNamespaceList
    create_object_schema = ExposureNamespaceCreate
    controller = ExposureNamespaceController

    def __init__(self) -> None:
        super().__init__("/exposure_namespace")

        self.router.add_api_route(
            "/{exposure_namespace_id}",
            self.update_exposure_namespace,
            methods=["PATCH"],
            response_model=ExposureNamespaceModel,
        )
        self.router.add_api_route(
            "/{exposure_namespace_id}/info",
            self.get_exposure_namespace_info,
            methods=["GET"],
            response_model=ExposureNamespaceInfo,
        )

    async def create_object(
        self, request: Request, data: ExposureNamespaceCreate
    ) -> ExposureNamespaceModel:
        """
        Create exposure namespace
        """
        controller = self.get_controller_for_request(request)
        exposure_namespace: ExposureNamespaceModel = await controller.create_exposure_namespace(
            data=data
        )
        return exposure_namespace

    async def get_object(
        self, request: Request, exposure_namespace_id: PydanticObjectId
    ) -> ExposureNamespaceModel:
        """
        Retrieve Exposure Namespace
        """
        controller = self.get_controller_for_request(request)
        exposure_namespace: ExposureNamespaceModel = await controller.get(
            document_id=exposure_namespace_id,
            exception_detail=(
                f'ExposureNamespace (id: "{exposure_namespace_id}") not found. '
                f"Please save the ExposureNamespace object first."
            ),
        )
        return exposure_namespace

    async def list_objects(
        self,
        request: Request,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = SortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
        name: Optional[str] = NameQuery,
    ) -> ExposureNamespaceList:
        """
        List ExposureNamespace
        """
        controller = self.get_controller_for_request(request)
        exposure_namespace_list: ExposureNamespaceList = await controller.list(
            page=page,
            page_size=page_size,
            sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
            search=search,
            name=name,
        )
        return exposure_namespace_list

    async def delete_object(
        self, request: Request, exposure_namespace_id: PydanticObjectId
    ) -> DeleteResponse:
        """
        Delete ExposureNamespace
        """
        return await super().delete_object(request, exposure_namespace_id)

    async def list_audit_logs(
        self,
        request: Request,
        exposure_namespace_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        """
        List Exposure Namespace audit logs
        """
        return await super().list_audit_logs(
            request, exposure_namespace_id, page, page_size, sort_by, sort_dir, search
        )

    async def update_description(
        self, request: Request, exposure_namespace_id: PydanticObjectId, data: DescriptionUpdate
    ) -> ExposureNamespaceModel:
        """
        Update exposure_namespace description
        """
        return await super().update_description(request, exposure_namespace_id, data)

    async def update_exposure_namespace(
        self,
        request: Request,
        exposure_namespace_id: PydanticObjectId,
        data: ExposureNamespaceUpdate,
    ) -> ExposureNamespaceModel:
        """
        Update ExposureNamespace
        """
        controller = self.get_controller_for_request(request)
        exposure_namespace: ExposureNamespaceModel = await controller.update_exposure_namespace(
            exposure_namespace_id, data
        )
        return exposure_namespace

    async def get_exposure_namespace_info(
        self,
        request: Request,
        exposure_namespace_id: PydanticObjectId,
        verbose: bool = VerboseQuery,
    ) -> ExposureNamespaceInfo:
        """
        Retrieve ExposureNamespace info
        """
        controller = self.get_controller_for_request(request)
        info = await controller.get_info(
            document_id=exposure_namespace_id,
            verbose=verbose,
        )
        return info
