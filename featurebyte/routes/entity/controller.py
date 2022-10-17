"""
Entity API routes
"""
from __future__ import annotations

from typing import cast

from bson.objectid import ObjectId

from featurebyte.models.entity import EntityModel, ParentEntity
from featurebyte.routes.common.base import (
    BaseDocumentController,
    GetInfoControllerMixin,
    RelationshipMixin,
)
from featurebyte.schema.entity import EntityCreate, EntityInfo, EntityList, EntityUpdate
from featurebyte.service.entity import EntityService
from featurebyte.service.relationship import EntityRelationshipService


class EntityController(  # type: ignore[misc]
    GetInfoControllerMixin[EntityInfo],
    BaseDocumentController[EntityModel, EntityList],
    RelationshipMixin[EntityModel, ParentEntity],
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
        self.relationship_service = entity_relationship_service

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
