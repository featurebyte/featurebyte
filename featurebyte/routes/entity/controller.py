"""
Entity API routes
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.models.base import UniqueConstraintResolutionSignature
from featurebyte.models.entity import EntityModel
from featurebyte.persistent.base import Persistent
from featurebyte.routes.common.base import BaseController
from featurebyte.routes.common.operation import DictProject, DictTransform
from featurebyte.schema.entity import EntityCreate, EntityList, EntityUpdate


class EntityController(BaseController[EntityModel, EntityList]):
    """
    Entity Controller
    """

    collection_name = EntityModel.collection_name()
    document_class = EntityModel
    paginated_document_class = EntityList
    info_transform = DictTransform(
        rule={
            **BaseController.base_info_transform_rule,
            "__root__": DictProject(rule=["serving_names"]),
        }
    )

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
        """
        # pylint: disable=duplicate-code
        document = EntityModel(
            **data.json_dict(), user_id=user.id, serving_names=[data.serving_name]
        )

        # check any conflict with existing documents
        await cls.check_document_unique_constraints(
            persistent=persistent,
            user_id=user.id,
            document=document,
        )

        insert_id = await persistent.insert_one(
            collection_name=cls.collection_name,
            document=document.dict(by_alias=True),
            user_id=user.id,
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

        # check whether conflict with other entity name
        entities, total_cnt = await persistent.find(
            collection_name=cls.collection_name,
            query_filter={"user_id": user.id, "name": data.name},
            page_size=2,
            user_id=user.id,
        )
        if total_cnt:
            for entity in entities:
                if str(entity["_id"]) == entity_id:
                    # update the same entity with the same name
                    return EntityModel(**entity)
                raise HTTPException(
                    status_code=HTTPStatus.CONFLICT,
                    detail=cls.get_conflict_message(
                        conflict_doc=data.json_dict(),
                        conflict_signature={"name": data.name},
                        resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
                        class_name=cls.to_class_name(),
                    ),
                )

        await persistent.update_one(
            collection_name=cls.collection_name,
            query_filter=query_filter,
            update={"$set": {"name": data.name}},
            user_id=user.id,
        )

        return await cls.get(user=user, persistent=persistent, document_id=entity_id)
