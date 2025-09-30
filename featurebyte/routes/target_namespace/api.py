"""
TargetNamespace API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Optional

from fastapi import Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.target_namespace import TargetNamespaceModel
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
from featurebyte.routes.target_namespace.controller import TargetNamespaceController
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.target_namespace import (
    TargetNamespaceClassificationMetadataUpdate,
    TargetNamespaceCreate,
    TargetNamespaceInfo,
    TargetNamespaceList,
    TargetNamespaceUpdate,
)
from featurebyte.schema.task import Task


class TargetNamespaceRouter(
    BaseApiRouter[
        TargetNamespaceModel, TargetNamespaceList, TargetNamespaceCreate, TargetNamespaceController
    ]
):
    """
    Target namespace router
    """

    object_model = TargetNamespaceModel
    list_object_model = TargetNamespaceList
    create_object_schema = TargetNamespaceCreate
    controller = TargetNamespaceController

    def __init__(self) -> None:
        super().__init__("/target_namespace")

        self.router.add_api_route(
            "/{target_namespace_id}/classification_metadata",
            self.update_target_namespace_classification_metadata,
            methods=["PATCH"],
            response_model=Task,
            status_code=HTTPStatus.ACCEPTED,
        )
        self.router.add_api_route(
            "/{target_namespace_id}",
            self.update_target_namespace,
            methods=["PATCH"],
            response_model=TargetNamespaceModel,
        )
        self.router.add_api_route(
            "/{target_namespace_id}/info",
            self.get_target_namespace_info,
            methods=["GET"],
            response_model=TargetNamespaceInfo,
        )

    async def create_object(
        self, request: Request, data: TargetNamespaceCreate
    ) -> TargetNamespaceModel:
        """
        Create target namespace
        """
        controller = self.get_controller_for_request(request)
        target_namespace: TargetNamespaceModel = await controller.create_target_namespace(data=data)
        return target_namespace

    async def get_object(
        self, request: Request, target_namespace_id: PydanticObjectId
    ) -> TargetNamespaceModel:
        """
        Retrieve Target Namespace
        """
        controller = self.get_controller_for_request(request)
        target_namespace: TargetNamespaceModel = await controller.get(
            document_id=target_namespace_id,
            exception_detail=(
                f'TargetNamespace (id: "{target_namespace_id}") not found. Please save the TargetNamespace object first.'
            ),
        )
        return target_namespace

    async def list_objects(
        self,
        request: Request,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = SortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
        name: Optional[str] = NameQuery,
    ) -> TargetNamespaceList:
        """
        List TargetNamespace
        """
        controller = self.get_controller_for_request(request)
        target_namespace_list: TargetNamespaceList = await controller.list(
            page=page,
            page_size=page_size,
            sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
            search=search,
            name=name,
        )
        return target_namespace_list

    async def delete_object(
        self, request: Request, target_namespace_id: PydanticObjectId
    ) -> DeleteResponse:
        """
        Delete TargetNamespace
        """
        return await super().delete_object(request, target_namespace_id)

    async def list_audit_logs(
        self,
        request: Request,
        target_namespace_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        """
        List Target Namespace audit logs
        """
        return await super().list_audit_logs(
            request, target_namespace_id, page, page_size, sort_by, sort_dir, search
        )

    async def update_description(
        self, request: Request, target_namespace_id: PydanticObjectId, data: DescriptionUpdate
    ) -> TargetNamespaceModel:
        """
        Update target_namespace description
        """
        return await super().update_description(request, target_namespace_id, data)

    async def update_target_namespace_classification_metadata(
        self,
        request: Request,
        target_namespace_id: PydanticObjectId,
        data: TargetNamespaceClassificationMetadataUpdate,
    ) -> Task:
        """
        Update TargetNamespace classification metadata
        """
        controller = self.get_controller_for_request(request)
        task: Task = await controller.create_target_namespace_classification_metadata_update_task(
            target_namespace_id=target_namespace_id, data=data
        )
        return task

    async def update_target_namespace(
        self, request: Request, target_namespace_id: PydanticObjectId, data: TargetNamespaceUpdate
    ) -> TargetNamespaceModel:
        """
        Update TargetNamespace
        """
        controller = self.get_controller_for_request(request)
        target_namespace: TargetNamespaceModel = await controller.update_target_namespace(
            target_namespace_id, data
        )
        return target_namespace

    async def get_target_namespace_info(
        self,
        request: Request,
        target_namespace_id: PydanticObjectId,
        verbose: bool = VerboseQuery,
    ) -> TargetNamespaceInfo:
        """
        Retrieve TargetNamespace info
        """
        controller = self.get_controller_for_request(request)
        info = await controller.get_info(
            document_id=target_namespace_id,
            verbose=verbose,
        )
        return info
