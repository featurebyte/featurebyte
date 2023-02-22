"""
Entity API routes
"""
from __future__ import annotations

from typing import List, Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.entity import EntityModel, EntityNameHistoryEntry, ParentEntity
from featurebyte.models.persistent import AuditDocumentList
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
from featurebyte.schema.entity import EntityCreate, EntityList, EntityUpdate
from featurebyte.schema.info import EntityInfo

router = APIRouter(prefix="/entity")


@router.post("", response_model=EntityModel, status_code=HTTPStatus.CREATED)
async def create_entity(request: Request, data: EntityCreate) -> EntityModel:
    """
    Create Entity
    """
    controller = request.state.app_container.entity_controller
    entity: EntityModel = await controller.create_entity(data=data)
    return entity


@router.get("/{entity_id}", response_model=EntityModel)
async def get_entity(request: Request, entity_id: PydanticObjectId) -> EntityModel:
    """
    Get Entity
    """
    controller = request.state.app_container.entity_controller
    entity: EntityModel = await controller.get(document_id=entity_id)
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
    controller = request.state.app_container.entity_controller
    entity_list: EntityList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return entity_list


@router.patch("/{entity_id}", response_model=EntityModel)
async def update_entity(
    request: Request,
    entity_id: PydanticObjectId,
    data: EntityUpdate,
) -> EntityModel:
    """
    Update Entity
    """
    controller = request.state.app_container.entity_controller
    entity: EntityModel = await controller.update_entity(
        entity_id=entity_id,
        data=data,
    )
    return entity


@router.post("/{entity_id}/parent", response_model=EntityModel, status_code=HTTPStatus.CREATED)
async def add_parent(
    request: Request, entity_id: PydanticObjectId, data: ParentEntity
) -> EntityModel:
    """
    Create entity relationship
    """
    controller = request.state.app_container.entity_controller
    entity: EntityModel = await controller.create_relationship(data=data, child_id=entity_id)
    return entity


@router.delete("/{entity_id}/parent/{parent_entity_id}", response_model=EntityModel)
async def remove_parent(
    request: Request, entity_id: PydanticObjectId, parent_entity_id: PydanticObjectId
) -> EntityModel:
    """
    Remove entity relationship
    """
    controller = request.state.app_container.entity_controller
    entity: EntityModel = await controller.remove_relationship(
        parent_id=parent_entity_id, child_id=entity_id
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
    controller = request.state.app_container.entity_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
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
    controller = request.state.app_container.entity_controller
    history_values = await controller.list_field_history(
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


@router.get("/{entity_id}/info", response_model=EntityInfo)
async def get_entity_info(
    request: Request,
    entity_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> EntityInfo:
    """
    Retrieve Entity info
    """
    controller = request.state.app_container.entity_controller
    info = await controller.get_info(
        document_id=entity_id,
        verbose=verbose,
    )
    return cast(EntityInfo, info)
