"""
Entity API routes
"""
from __future__ import annotations

from typing import Literal, Optional

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.routes.entity.schema import Entity, EntityCreate, EntityList, EntityUpdate

router = APIRouter(prefix="/entity")


@router.post(
    "", response_model=Entity, response_model_by_alias=False, status_code=HTTPStatus.CREATED
)
def create_entity(request: Request, data: EntityCreate) -> Entity:
    """
    Create Entity
    """
    entity: Entity = request.state.controller.create_entity(
        user=request.state.user, persistent=request.state.persistent, data=data
    )
    return entity


@router.get("", response_model=EntityList, response_model_by_alias=False)
def list_entities(
    request: Request,
    page: int = 1,
    page_size: int = 10,
    sort_by: Optional[str] = "created_at",
    sort_dir: Literal["asc", "desc"] = "desc",
    name: Optional[str] = None,
) -> EntityList:
    """
    List Entity
    """
    entity_list: EntityList = request.state.controller.list_entities(
        user=request.state.user,
        persistent=request.state.persistent,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        name=name,
    )
    return entity_list


@router.patch("/{entity_id}", response_model=Entity, response_model_by_alias=False)
def update_entity(request: Request, entity_id: str, data: EntityUpdate) -> Entity:
    """
    Update Entity
    """
    entity: Entity = request.state.controller.update_entity(
        user=request.state.user,
        persistent=request.state.persistent,
        entity_id=entity_id,
        data=data,
    )
    return entity
