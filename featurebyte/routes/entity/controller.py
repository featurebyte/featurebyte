"""
Entity API route controller
"""
from __future__ import annotations

from bson.objectid import ObjectId

from featurebyte.models.entity import EntityModel, ParentEntity
from featurebyte.routes.common.base import BaseDocumentController, RelationshipMixin
from featurebyte.schema.entity import EntityCreate, EntityList, EntityServiceUpdate, EntityUpdate
from featurebyte.schema.info import EntityInfo
from featurebyte.service.entity import EntityService
from featurebyte.service.info import InfoService
from featurebyte.service.relationship import EntityRelationshipService


class EntityController(
    BaseDocumentController[EntityModel, EntityService, EntityList],
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
        info_service: InfoService,
    ):
        super().__init__(service)
        self.relationship_service = entity_relationship_service
        self.info_service = info_service

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
        return await self.service.create_document(data)

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
        await self.service.update_document(
            document_id=entity_id, data=EntityServiceUpdate(**data.dict()), return_document=False
        )
        return await self.get(document_id=entity_id)

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> EntityInfo:
        """
        Get document info given document ID

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        InfoDocument
        """
        info_document = await self.info_service.get_entity_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
