"""
Entity API routes
"""
from __future__ import annotations

from typing import Any, Literal

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.enum import CollectionName
from featurebyte.persistent import DuplicateDocumentError, Persistent
from featurebyte.routes.common.helpers import get_utc_now
from featurebyte.routes.entity.schema import Entity, EntityCreate, EntityList, EntityUpdate


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

    @classmethod
    def list_entities(
        cls,
        user: Any,
        persistent: Persistent,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
    ) -> EntityList:
        """
        List Entities
        """
        query_filter = {"user_id": user.id}
        docs, total = persistent.find(
            collection_name=cls.collection_name,
            query_filter=query_filter,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            page_size=page_size,
        )
        return EntityList(page=page, page_size=page_size, total=total, data=list(docs))

    @classmethod
    def update_entity(
        cls, user: Any, persistent: Persistent, entity_id: ObjectId, data: EntityUpdate
    ) -> Entity:
        """
        Update Entity
        """
        query_filter = {"_id": ObjectId(entity_id), "user_id": user.id}
        entity = persistent.find_one(collection_name=cls.collection_name, query_filter=query_filter)
        not_found_exception = HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=f'Entity ID "{entity_id}" not found.'
        )
        if not entity:
            raise not_found_exception

        updated_cnt = persistent.update_one(
            collection_name=cls.collection_name,
            query_filter=query_filter,
            update={"$set": data.dict()},
        )
        if not updated_cnt:
            raise not_found_exception

        entity = persistent.find_one(collection_name=cls.collection_name, query_filter=data.dict())
        if entity is None:
            raise not_found_exception
        return Entity(**entity)
