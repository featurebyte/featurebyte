"""
TargetNamespace API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import cast

from fastapi import APIRouter, Request

from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.target_namespace import TargetNamespaceModel
from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseRouter
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    NameQuery,
    PageQuery,
    PageSizeQuery,
    PyObjectId,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
    VerboseQuery,
)
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.target_namespace import (
    TargetNamespaceCreate,
    TargetNamespaceInfo,
    TargetNamespaceList,
    TargetNamespaceUpdate,
)

router = APIRouter(prefix="/target_namespace")


class TargetNamespaceRouter(BaseRouter):
    """
    Target namespace router
    """

    def __init__(self) -> None:
        super().__init__(router=router)


@router.post("", response_model=TargetNamespaceModel, status_code=HTTPStatus.CREATED)
async def create_target_namespace(
    request: Request, data: TargetNamespaceCreate
) -> TargetNamespaceModel:
    """
    Create target namespace
    """
    controller = request.state.app_container.target_namespace_controller
    target_namespace: TargetNamespaceModel = await controller.create_target_namespace(data=data)
    return target_namespace


@router.get("/{target_namespace_id}", response_model=TargetNamespaceModel)
async def get_target_namespace(
    request: Request, target_namespace_id: PyObjectId
) -> TargetNamespaceModel:
    """
    Retrieve Target Namespace
    """
    controller = request.state.app_container.target_namespace_controller
    target_namespace: TargetNamespaceModel = await controller.get(
        document_id=target_namespace_id,
        exception_detail=(
            f'TargetNamespace (id: "{target_namespace_id}") not found. Please save the TargetNamespace object first.'
        ),
    )
    return target_namespace


@router.patch("/{target_namespace_id}", response_model=TargetNamespaceModel)
async def update_target_namespace(
    request: Request, target_namespace_id: PyObjectId, data: TargetNamespaceUpdate
) -> TargetNamespaceModel:
    """
    Update TargetNamespace
    """
    controller = request.state.app_container.target_namespace_controller
    target_namespace: TargetNamespaceModel = (
        await controller.target_namespace_service.update_document(target_namespace_id, data)
    )
    return target_namespace


@router.get("", response_model=TargetNamespaceList)
async def list_target_namespaces(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: str | None = SortByQuery,
    sort_dir: SortDir | None = SortDirQuery,
    search: str | None = SearchQuery,
    name: str | None = NameQuery,
) -> TargetNamespaceList:
    """
    List TargetNamespace
    """
    controller = request.state.app_container.target_namespace_controller
    target_namespace_list: TargetNamespaceList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
        name=name,
    )
    return target_namespace_list


@router.get("/audit/{target_namespace_id}", response_model=AuditDocumentList)
async def list_target_namespace_audit_logs(
    request: Request,
    target_namespace_id: PyObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: str | None = AuditLogSortByQuery,
    sort_dir: SortDir | None = SortDirQuery,
    search: str | None = SearchQuery,
) -> AuditDocumentList:
    """
    List Target Namespace audit logs
    """
    controller = request.state.app_container.target_namespace_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=target_namespace_id,
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
    )
    return audit_doc_list


@router.get("/{target_namespace_id}/info", response_model=TargetNamespaceInfo)
async def get_target_namespace_info(
    request: Request,
    target_namespace_id: PyObjectId,
    verbose: bool = VerboseQuery,
) -> TargetNamespaceInfo:
    """
    Retrieve TargetNamespace info
    """
    controller = request.state.app_container.target_namespace_controller
    info = await controller.get_info(
        document_id=target_namespace_id,
        verbose=verbose,
    )
    return cast(TargetNamespaceInfo, info)


@router.patch("/{target_namespace_id}/description", response_model=TargetNamespaceModel)
async def update_target_namespace_description(
    request: Request,
    target_namespace_id: PyObjectId,
    data: DescriptionUpdate,
) -> TargetNamespaceModel:
    """
    Update target_namespace description
    """
    controller = request.state.app_container.target_namespace_controller
    target_namespace: TargetNamespaceModel = await controller.update_description(
        document_id=target_namespace_id,
        description=data.description,
    )
    return target_namespace


@router.delete("/{target_namespace_id}")
async def delete_target_namespace(
    request: Request,
    target_namespace_id: PyObjectId,
) -> None:
    """
    Delete TargetNamespace
    """
    controller = request.state.app_container.target_namespace_controller
    await controller.delete(document_id=target_namespace_id)
