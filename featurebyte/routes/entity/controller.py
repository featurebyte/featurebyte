"""
Entity API routes
"""
from __future__ import annotations

from typing import Any, Literal, Optional

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.enum import CollectionName
from featurebyte.models.entity import EntityModel, EntityNameHistoryEntry
from featurebyte.persistent import Persistent
from featurebyte.routes.common.util import get_utc_now
from featurebyte.schema.entity import EntityCreate, EntityList, EntityUpdate


class EntityController:
    """
    Entity Controller
    """

    collection_name = CollectionName.ENTITY

    @classmethod
    async def create_entity(
        cls,
        user: Any,
        persistent: Persistent,
        data: EntityCreate,
    ) -> EntityModel:
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
        EntityModel
            Newly created entity object

        Raises
        ------
        HTTPException
            If the entity name conflicts with existing entity name
        """
        document = EntityModel(
            name=data.name,
            serving_names=[data.serving_name],
            user_id=user.id,
        )

        conflict_entity = await persistent.find_one(
            collection_name=cls.collection_name, query_filter={"name": data.name}
        )
        if conflict_entity:
            raise HTTPException(
                status_code=HTTPStatus.CONFLICT,
                detail=f'Entity name (entity.name: "{data.name}") already exists.',
            )

        conflict_entity = await persistent.find_one(
            collection_name=cls.collection_name,
            query_filter={"serving_names": document.serving_names},
        )
        if conflict_entity:
            raise HTTPException(
                status_code=HTTPStatus.CONFLICT,
                detail=f'Entity serving name (entity.serving_names: "{data.serving_name}") already exists.',
            )

        insert_id = await persistent.insert_one(
            collection_name=cls.collection_name, document=document.dict(by_alias=True)
        )
        assert insert_id == document.id

        return await cls.get_entity(
            user=user,
            persistent=persistent,
            entity_id=insert_id,
        )

    @classmethod
    async def list_entities(
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
        docs, total = await persistent.find(
            collection_name=cls.collection_name,
            query_filter=query_filter,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            page_size=page_size,
        )
        return EntityList(page=page, page_size=page_size, total=total, data=list(docs))

    @classmethod
    async def get_entity(
        cls, user: Any, persistent: Persistent, entity_id: ObjectId
    ) -> EntityModel:
        """
        Get Entity from the persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        entity_id: ObjectId
            Entity ID

        Returns
        -------
        EntityModel
            Retrieve entity object

        Raises
        ------
        HTTPException
            If the entity not found
        """
        query_filter = {"_id": ObjectId(entity_id), "user_id": user.id}
        entity = await persistent.find_one(
            collection_name=cls.collection_name, query_filter=query_filter
        )
        # check that entity id exists
        if not entity:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=f'Entity (entity.id: "{entity_id}") not found! Please save the Entity object first.',
            )
        return EntityModel(**entity)

    @classmethod
    async def update_entity(
        cls, user: Any, persistent: Persistent, entity_id: ObjectId, data: EntityUpdate
    ) -> EntityModel:
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
        EntityModel
            Entity object with updated attribute(s)

        Raises
        ------
        HTTPException
            If the entity name already exists in persistent
        """
        query_filter = {"_id": ObjectId(entity_id), "user_id": user.id}
        entity_obj = await cls.get_entity(
            user=user,
            persistent=persistent,
            entity_id=entity_id,
        )

        # store current name & name_history
        cur_name = entity_obj.name
        name_history = [record.dict() for record in entity_obj.name_history]

        # check whether conflict with other entity name
        entities, total_cnt = await persistent.find(
            collection_name=cls.collection_name,
            query_filter={"user_id": user.id, "name": data.name},
            page_size=2,
        )
        if total_cnt:
            for entity in entities:
                if str(entity["_id"]) == entity_id:
                    # update the same entity with the same name
                    return EntityModel(**entity)
                raise HTTPException(
                    status_code=HTTPStatus.CONFLICT,
                    detail=f'Entity name (entity.name: "{data.name}") already exists.',
                )

        name_history.append(EntityNameHistoryEntry(created_at=get_utc_now(), name=cur_name).dict())
        await persistent.update_one(
            collection_name=cls.collection_name,
            query_filter=query_filter,
            update={"$set": {"name": data.name, "name_history": name_history}},
        )

        return await cls.get_entity(
            user=user,
            persistent=persistent,
            entity_id=entity_id,
        )
