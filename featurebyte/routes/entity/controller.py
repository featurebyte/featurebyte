"""
Entity API routes
"""
from __future__ import annotations

from typing import Type

from bson.objectid import ObjectId

from featurebyte.models.entity import EntityModel
from featurebyte.routes.common.base import BaseDocumentController, GetInfoControllerMixin
from featurebyte.schema.entity import EntityCreate, EntityInfo, EntityList, EntityUpdate
from featurebyte.service.entity import EntityService


class EntityController(
    GetInfoControllerMixin[EntityInfo],
    BaseDocumentController[EntityModel, EntityList],
):
    """
    Entity Controller
    """

    paginated_document_class = EntityList
    document_service_class: Type[EntityService] = EntityService  # type: ignore[assignment]

    def __init__(self, service: EntityService):
        self.service = service

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
        document = await self.service.create_document(data)
        return document

    async def update_entity(self, entity_id: ObjectId, data: EntityUpdate) -> EntityModel:
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
        document = await self.service.update_document(document_id=entity_id, data=data)
        assert document is not None
        return document
