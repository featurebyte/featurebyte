"""
Context API routes
"""
from __future__ import annotations

from typing import Optional

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.context import ContextModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
)
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.context import ContextCreate, ContextList, ContextUpdate

router = APIRouter(prefix="/context")


@router.post("", response_model=ContextModel, status_code=HTTPStatus.CREATED)
async def create_context(request: Request, data: ContextCreate) -> ContextModel:
    """
    Create Context
    """
    controller = request.state.app_container.context_controller
    context: ContextModel = await controller.create_context(data=data)
    return context


@router.get("/{context_id}", response_model=ContextModel)
async def get_context(request: Request, context_id: PydanticObjectId) -> ContextModel:
    """
    Get Context
    """
    controller = request.state.app_container.context_controller
    context: ContextModel = await controller.get(document_id=context_id)
    return context


@router.get("", response_model=ContextList)
async def list_contexts(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> ContextList:
    """
    List Context
    """
    controller = request.state.app_container.context_controller
    context_list: ContextList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return context_list


@router.patch("/{context_id}", response_model=ContextModel)
async def update_context(
    request: Request, context_id: PydanticObjectId, data: ContextUpdate
) -> ContextModel:
    """
    Update Context
    """
    controller = request.state.app_container.context_controller
    context: ContextModel = await controller.update_context(context_id=context_id, data=data)
    return context


@router.get("/audit/{context_id}", response_model=AuditDocumentList)
async def list_context_audit_logs(
    request: Request,
    context_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Context audit logs
    """
    controller = request.state.app_container.context_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=context_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.patch("/{context_id}/description", response_model=ContextModel)
async def update_context_description(
    request: Request,
    context_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> ContextModel:
    """
    Update context description
    """
    controller = request.state.app_container.context_controller
    context: ContextModel = await controller.update_description(
        document_id=context_id,
        description=data.description,
    )
    return context
