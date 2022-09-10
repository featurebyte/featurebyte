"""
Entity API routes
"""
from __future__ import annotations

from typing import Any, Type

from bson.objectid import ObjectId

from featurebyte.models.entity import EntityModel
from featurebyte.persistent.base import Persistent
from featurebyte.routes.common.base import BaseDocumentController, GetInfoMixin
from featurebyte.schema.entity import EntityCreate, EntityInfo, EntityList, EntityUpdate
from featurebyte.service.entity import EntityService


class EntityController(BaseDocumentController[EntityModel, EntityList], GetInfoMixin[EntityInfo]):
    """
    Entity Controller
    """

    paginated_document_class = EntityList
    document_service_class: Type[EntityService] = EntityService  # type: ignore[assignment]

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
        document = await cls.document_service_class(
            user=user, persistent=persistent
        ).create_document(data)
        return document

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
        """
        document = await cls.document_service_class(
            user=user, persistent=persistent
        ).update_document(document_id=entity_id, data=data)
        return document
