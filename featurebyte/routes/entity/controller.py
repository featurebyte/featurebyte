"""
Entity API routes
"""
from __future__ import annotations

from typing import Any, Literal, Optional

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.enum import CollectionName
from featurebyte.models.entity import EntityNameHistoryEntry
from featurebyte.persistent import Persistent
from featurebyte.routes.common.util import get_utc_now
from featurebyte.schema.entity import Entity, EntityCreate, EntityList, EntityUpdate


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
        Create Entity at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        data: EntityCreate
            Entity creation payload

        Returns
        -------
        Entity
            Newly created entity object

        Raises
        ------
        HTTPException
            If the entity name conflicts with existing entity name
        """
        document = Entity(
            name=data.name,
            serving_names=[data.serving_name],
            user_id=user.id,
            created_at=get_utc_now(),
        )

        conflict_entity = persistent.find_one(
            collection_name=cls.collection_name, query_filter={"name": data.name}
        )
        if conflict_entity:
            raise HTTPException(
                status_code=HTTPStatus.CONFLICT,
                detail=f'Entity name "{data.name}" already exists.',
            )

        conflict_entity = persistent.find_one(
            collection_name=cls.collection_name,
            query_filter={"serving_names": document.serving_names},
        )
        if conflict_entity:
            raise HTTPException(
                status_code=HTTPStatus.CONFLICT,
                detail=f'Entity serving name "{data.serving_name}" already exists.',
            )

        insert_id = persistent.insert_one(
            collection_name=cls.collection_name, document=document.dict(by_alias=True)
        )
        assert insert_id == document.id
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
        name: Optional[str] = None,
    ) -> EntityList:
        """
        List Entities stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_by: str | None
            Key used to sort the returning entities
        sort_dir: "asc" or "desc"
            Sorting the returning entities in ascending order or descending order
        name: str | None
            Entity name used to filter the entities

        Returns
        -------
        EntityList
            List of entities fulfilled the filtering condition
        """
        query_filter = {"user_id": user.id}
        if name is not None:
            query_filter["name"] = name
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
    def get_entity(cls, user: Any, persistent: Persistent, entity_id: ObjectId) -> Entity:
        """
        Get Entity
        """
        query_filter = {"_id": ObjectId(entity_id), "user_id": user.id}
        entity = persistent.find_one(collection_name=cls.collection_name, query_filter=query_filter)
        # check that entity id exists
        if not entity:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND, detail=f'Entity ID "{entity_id}" not found.'
            )
        return Entity(**entity)

    @classmethod
    def update_entity(
        cls, user: Any, persistent: Persistent, entity_id: ObjectId, data: EntityUpdate
    ) -> Entity:
        """
        Update Entity stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        entity_id: ObjectId
            Entity ID
        data: EntityUpdate
            Entity update payload

        Returns
        -------
        Entity
            Entity object with updated attribute(s)

        Raises
        ------
        not_found_exception
            If the entity not found
        HTTPException
            If the entity name already exists in persistent
        """
        query_filter = {"_id": ObjectId(entity_id), "user_id": user.id}
        entity = persistent.find_one(collection_name=cls.collection_name, query_filter=query_filter)

        # check that entity id exists
        not_found_exception = HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=f'Entity ID "{entity_id}" not found.'
        )
        if not entity:
            raise not_found_exception

        # store current name & name_history
        cur_name = entity["name"]
        name_history = entity["name_history"]

        # check whether conflict with other entity name
        entities, total_cnt = persistent.find(
            collection_name=cls.collection_name,
            query_filter={"user_id": user.id, "name": data.name},
            page_size=2,
        )
        if total_cnt:
            for entity in entities:
                if str(entity["_id"]) == entity_id:
                    # update the same entity with the same name
                    return Entity(**entity)
                raise HTTPException(
                    status_code=HTTPStatus.CONFLICT,
                    detail=f'Entity name "{data.name}" already exists.',
                )

        name_history.append(EntityNameHistoryEntry(created_at=get_utc_now(), name=cur_name).dict())
        updated_cnt = persistent.update_one(
            collection_name=cls.collection_name,
            query_filter=query_filter,
            update={"$set": {"name": data.name, "name_history": name_history}},
        )
        if not updated_cnt:
            raise not_found_exception

        entity = persistent.find_one(collection_name=cls.collection_name, query_filter=data.dict())
        if entity is None:
            raise not_found_exception
        return Entity(**entity)
