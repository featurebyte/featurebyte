"""
Entity API route controller
"""
from __future__ import annotations

from bson.objectid import ObjectId

from featurebyte.models.entity import EntityModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.entity import EntityCreate, EntityList, EntityServiceUpdate, EntityUpdate
from featurebyte.schema.info import EntityInfo
from featurebyte.service.catalog import CatalogService
from featurebyte.service.entity import EntityService
from featurebyte.service.relationship import EntityRelationshipService


class EntityController(BaseDocumentController[EntityModel, EntityService, EntityList]):
    """
    Entity Controller
    """

    paginated_document_class = EntityList

    def __init__(
        self,
        entity_service: EntityService,
        entity_relationship_service: EntityRelationshipService,
        catalog_service: CatalogService,
    ):
        super().__init__(entity_service)
        self.relationship_service = entity_relationship_service
        self.catalog_service = catalog_service

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
        Get entity info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        EntityInfo
        """
        _ = verbose
        entity = await self.service.get_document(document_id=document_id)

        # get catalog info
        catalog = await self.catalog_service.get_document(entity.catalog_id)

        return EntityInfo(
            name=entity.name,
            created_at=entity.created_at,
            updated_at=entity.updated_at,
            serving_names=entity.serving_names,
            catalog_name=catalog.name,
            description=catalog.description,
        )
