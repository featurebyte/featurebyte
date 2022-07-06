"""
Entity API routes
"""

from typing import Any

from http import HTTPStatus

from fastapi import HTTPException

from featurebyte.persistent import DuplicateDocumentError, Persistent
from featurebyte.routes.common.helpers import get_utc_now
from featurebyte.routes.entity.schema import Entity, EntityCreate
from featurebyte.routes.enum import CollectionName


class EntityController:
    """
    Entity Controller
    """

    collection_name = CollectionName.ENTITY

    @classmethod
    def create_entity(
        cls,
        user: Any,
        persistent: Persistent,
        data: EntityCreate,
    ) -> Entity:
        """
        Create Entity
        """
        document = Entity(user_id=user.id, created_at=get_utc_now(), **data.dict())
        try:
            insert_id = persistent.insert_one(
                collection_name=cls.collection_name, document=document.dict(by_alias=True)
            )
            assert insert_id == document.id
        except DuplicateDocumentError as exc:
            raise HTTPException(
                status_code=HTTPStatus.CONFLICT,
                detail=f'Entity "{data.name}" already exists.',
            ) from exc

        return document
