"""
Entity API routes
"""
from __future__ import annotations

from typing import cast

from bson.objectid import ObjectId

from featurebyte.models.entity import EntityModel
from featurebyte.routes.common.base import BaseDocumentController, GetInfoControllerMixin
from featurebyte.schema.entity import (
    EntityCreate,
    EntityInfo,
    EntityList,
    EntityServiceUpdate,
    EntityUpdate,
)
from featurebyte.service.entity import EntityService
from featurebyte.service.relationship import EntityRelationshipService


class EntityController(  # type: ignore[misc]
    GetInfoControllerMixin[EntityInfo],
    BaseDocumentController[EntityModel, EntityList],
):
    """
    Entity Controller
    """

    paginated_document_class = EntityList

    def __init__(
        self,
        service: EntityService,
        entity_relationship_service: EntityRelationshipService,
    ):
        super().__init__(service)
        self.entity_relationship_service = entity_relationship_service

    async def create_entity(
        self,
        data: EntityCreate,
    ) -> EntityModel:
        """
        Create Entity at persistent (GitDB or MongoDB)

        Parameters
        ----------
        data: EntityCreate
            Entity creation payload

        Returns
        -------
        EntityModel
            Newly created entity object
        """
        document = await self.service.create_document(data)  # type: ignore[attr-defined]
        return cast(EntityModel, document)

    async def update_entity(
        self,
        entity_id: ObjectId,
        data: EntityUpdate,
    ) -> EntityModel:
        """
        Update Entity stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        entity_id: ObjectId
            Entity ID
        data: EntityUpdate
            Entity update payload

        Returns
        -------
        EntityModel
            Entity object with updated attribute(s)
        """
        if data.name is not None:
            await self.service.update_document(  # type: ignore[attr-defined]
                document_id=entity_id, data=EntityServiceUpdate(name=data.name)
            )

        if data.add_parent:
            await self.entity_relationship_service.add_relationship(
                parent=data.add_parent, child_id=entity_id
            )

        if data.remove_parent:
            await self.entity_relationship_service.remove_relationship(
                parent=data.remove_parent, child_id=entity_id
            )

        return await self.get(document_id=entity_id)
