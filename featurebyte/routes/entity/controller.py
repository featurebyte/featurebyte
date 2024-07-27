"""
Entity API route controller
"""

from __future__ import annotations

from typing import Any, List, Tuple

from bson import ObjectId

from featurebyte.models.entity import EntityModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.entity import EntityCreate, EntityList, EntityServiceUpdate, EntityUpdate
from featurebyte.schema.info import EntityInfo
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.catalog import CatalogService
from featurebyte.service.context import ContextService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.relationship import EntityRelationshipService
from featurebyte.service.table import TableService
from featurebyte.service.target import TargetService


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
        context_service: ContextService,
        table_service: TableService,
        observation_table_service: ObservationTableService,
        historical_feature_table_service: HistoricalFeatureTableService,
        batch_request_table_service: BatchRequestTableService,
        batch_feature_table_service: BatchFeatureTableService,
        feature_service: FeatureService,
        target_service: TargetService,
        feature_list_service: FeatureListService,
    ):
        super().__init__(entity_service)
        self.relationship_service = entity_relationship_service
        self.catalog_service = catalog_service
        self.context_service = context_service
        self.table_service = table_service
        self.observation_table_service = observation_table_service
        self.historical_feature_table_service = historical_feature_table_service
        self.batch_request_table_service = batch_request_table_service
        self.batch_feature_table_service = batch_feature_table_service
        self.feature_service = feature_service
        self.target_service = target_service
        self.feature_list_service = feature_list_service

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
            document_id=entity_id,
            data=EntityServiceUpdate(**data.model_dump()),
            return_document=False,
        )
        return await self.get(document_id=entity_id)

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return [
            (
                self.feature_service,
                {
                    "$or": [
                        {"entity_ids": document_id},
                        {"relationships_info.entity_id": document_id},
                        {"relationships_info.related_entity_id": document_id},
                    ]
                },
            ),
            (
                self.target_service,
                {
                    "$or": [
                        {"entity_ids": document_id},
                        {"relationships_info.entity_id": document_id},
                        {"relationships_info.related_entity_id": document_id},
                    ]
                },
            ),
            (
                self.feature_list_service,
                {
                    "$or": [
                        {"relationships_info.entity_id": document_id},
                        {"relationships_info.related_entity_id": document_id},
                    ]
                },
            ),
            (self.context_service, {"primary_entity_ids": document_id}),
            (self.table_service, {"columns_info.entity_id": document_id}),
            (self.observation_table_service, {"columns_info.entity_id": document_id}),
            (self.historical_feature_table_service, {"columns_info.entity_id": document_id}),
            (self.batch_request_table_service, {"columns_info.entity_id": document_id}),
            (self.batch_feature_table_service, {"columns_info.entity_id": document_id}),
        ]

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
