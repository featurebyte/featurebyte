"""
Entity API routes
"""
from __future__ import annotations

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.routes.entity.schema import Entity, EntityCreate

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
