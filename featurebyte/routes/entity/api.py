"""
Entity API routes
"""
from __future__ import annotations

from typing import Literal, Optional

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.entity import EntityModel
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
    page: int = 1,
    page_size: int = 10,
    sort_by: Optional[str] = "created_at",
    sort_dir: Literal["asc", "desc"] = "desc",
    name: Optional[str] = None,
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
