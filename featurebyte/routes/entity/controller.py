"""
Entity API routes
"""
from __future__ import annotations

from typing import cast

from bson.objectid import ObjectId

from featurebyte.models.entity import EntityModel, ParentEntity
from featurebyte.routes.common.base import BaseDocumentController, GetInfoControllerMixin
from featurebyte.schema.entity import EntityCreate, EntityInfo, EntityList, EntityUpdate
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
        Create Entity at persistent

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
        Update Entity stored at persistent

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
        document: EntityModel = await self.service.update_document(  # type: ignore[attr-defined]
            document_id=entity_id, data=data
        )
        assert document is not None
        return document

    async def create_entity_relationship(
        self, entity_id: ObjectId, data: ParentEntity
    ) -> EntityModel:
        """
        Create entity relationship at persistent

        Parameters
        ----------
        entity_id: ObjectId
            Child entity ID
        data: ParentEntity
            Parent entity payload

        Returns
        -------
        EntityModel
            Entity model with newly created relationship
        """
        document = await self.entity_relationship_service.add_relationship(
            parent=data, child_id=entity_id
        )
        return cast(EntityModel, document)

    async def remove_entity_relationship(
        self,
        entity_id: ObjectId,
        parent_entity_id: ObjectId,
    ) -> EntityModel:
        """
        Remove Entity relationship at persistent

        Parameters
        ----------
        entity_id: ObjectId
            Child entity ID
        parent_entity_id: ObjectId
            Parent entity ID

        Returns
        -------
        EntityModel
            Entity model with specified relationship get removed
        """
        document = await self.entity_relationship_service.remove_relationship(
            parent_id=parent_entity_id,
            child_id=entity_id,
        )
        return cast(EntityModel, document)
