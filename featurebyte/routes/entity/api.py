"""
Entity API routes
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, cast

from http import HTTPStatus

from beanie import PydanticObjectId
from fastapi import APIRouter, Request

from featurebyte.models.entity import EntityModel, EntityNameHistoryEntry
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
from featurebyte.schema.entity import EntityCreate, EntityList, EntityUpdate

router = APIRouter(prefix="/entity")


@router.post("", response_model=EntityModel, status_code=HTTPStatus.CREATED)
async def create_entity(request: Request, data: EntityCreate) -> EntityModel:
    """
    Create Entity
    """
    entity: EntityModel = await request.state.controller.create_entity(
        user=request.state.user, persistent=request.state.persistent, data=data
    )
    return entity


@router.get("/{entity_id}", response_model=EntityModel)
async def get_entity(request: Request, entity_id: str) -> EntityModel:
    """
    Get Entity
    """
    entity: EntityModel = await request.state.controller.get(
        user=request.state.user, persistent=request.state.persistent, document_id=entity_id
    )
    return entity


@router.get("", response_model=EntityList)
async def list_entities(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> EntityList:
    """
    List Entity
    """
    entity_list: EntityList = await request.state.controller.list(
        user=request.state.user,
        persistent=request.state.persistent,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return entity_list


@router.patch("/{entity_id}", response_model=EntityModel)
async def update_entity(request: Request, entity_id: str, data: EntityUpdate) -> EntityModel:
    """
    Update Entity
    """
    entity: EntityModel = await request.state.controller.update_entity(
        user=request.state.user,
        persistent=request.state.persistent,
        entity_id=entity_id,
        data=data,
    )
    return entity


@router.get("/audit/{entity_id}", response_model=AuditDocumentList)
async def list_entity_audit_logs(
    request: Request,
    entity_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Entity audit logs
    """
    audit_doc_list: AuditDocumentList = await request.state.controller.list_audit(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=entity_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get(
    "/history/name/{entity_id}",
    response_model=List[EntityNameHistoryEntry],
)
async def list_name_history(
    request: Request,
    entity_id: PydanticObjectId,
) -> List[EntityNameHistoryEntry]:
    """
    List Entity name history
    """
    history_values = await request.state.controller.list_field_history(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=entity_id,
        field="name",
    )

    return [
        EntityNameHistoryEntry(
            created_at=record.created_at,
            name=record.value,
        )
        for record in history_values
    ]


@router.get("/{entity_id}/info")
async def get_entity_info(request: Request, entity_id: str, verbose: bool = True) -> dict[str, Any]:
    """
    Retrieve EventData info
    """
    info = await request.state.controller.get_info(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=entity_id,
        verbose=bool(verbose),
    )
    return cast(Dict[str, Any], info)
