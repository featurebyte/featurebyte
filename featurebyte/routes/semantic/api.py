"""
Semantic API routes
"""
from __future__ import annotations

from typing import Optional

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.relationship import Parent
from featurebyte.models.semantic import SemanticModel
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
from featurebyte.schema.semantic import SemanticCreate, SemanticList

router = APIRouter(prefix="/semantic")


@router.post("", response_model=SemanticModel, status_code=HTTPStatus.CREATED)
async def create_semantic(request: Request, data: SemanticCreate) -> SemanticModel:
    """
    Create Semantic
    """
    controller = request.state.app_container.semantic_controller
    semantic: SemanticModel = await controller.create_semantic(data=data)
    return semantic


@router.get("/{semantic_id}", response_model=SemanticModel)
async def get_semantic(request: Request, semantic_id: PydanticObjectId) -> SemanticModel:
    """
    Get Semantic
    """
    controller = request.state.app_container.semantic_controller
    semantic: SemanticModel = await controller.get(document_id=semantic_id)
    return semantic


@router.get("", response_model=SemanticList)
async def list_semantics(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> SemanticList:
    """
    List Semantic
    """
    controller = request.state.app_container.semantic_controller
    semantic_list: SemanticList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return semantic_list


@router.post("/{semantic_id}/parent", response_model=SemanticModel, status_code=HTTPStatus.CREATED)
async def add_parent(
    request: Request, semantic_id: PydanticObjectId, data: Parent
) -> SemanticModel:
    """
    Create semantic relationship
    """
    controller = request.state.app_container.semantic_controller
    semantic: SemanticModel = await controller.create_relationship(data=data, child_id=semantic_id)
    return semantic


@router.delete("/{semantic_id}/parent/{parent_semantic_id}", response_model=SemanticModel)
async def remove_parent(
    request: Request, semantic_id: PydanticObjectId, parent_semantic_id: PydanticObjectId
) -> SemanticModel:
    """
    Remove semantic relationship
    """
    controller = request.state.app_container.semantic_controller
    semantic: SemanticModel = await controller.remove_relationship(
        parent_id=parent_semantic_id, child_id=semantic_id
    )
    return semantic


@router.get("/audit/{semantic_id}", response_model=AuditDocumentList)
async def list_semantic_audit_logs(
    request: Request,
    semantic_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Semantic audit logs
    """
    controller = request.state.app_container.semantic_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=semantic_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.patch("/{semantic_id}/description", response_model=SemanticModel)
async def update_semantic_description(
    request: Request,
    semantic_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> SemanticModel:
    """
    Update semantic description
    """
    controller = request.state.app_container.semantic_controller
    semantic: SemanticModel = await controller.update_description(
        document_id=semantic_id,
        description=data.description,
    )
    return semantic
