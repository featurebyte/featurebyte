"""
Entity API routes
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.enum import CollectionName
from featurebyte.models.entity import EntityModel, EntityNameHistoryEntry
from featurebyte.persistent.base import Persistent
from featurebyte.routes.common.base import BaseController
from featurebyte.routes.common.util import get_utc_now
from featurebyte.schema.entity import EntityCreate, EntityList, EntityUpdate


class EntityController(BaseController[EntityModel, EntityList]):
    """
    Entity Controller
    """

    collection_name = CollectionName.ENTITY
    document_class = EntityModel
    paginated_document_class = EntityList

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
            _id=data.id,
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

        return await cls.get(user=user, persistent=persistent, document_id=insert_id)

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
        entity_obj = await cls.get(
            user=user,
            persistent=persistent,
            document_id=entity_id,
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

        return await cls.get(user=user, persistent=persistent, document_id=entity_id)
