"""
Module to support serving parent features using child entities
"""

from __future__ import annotations

from collections import OrderedDict
from typing import List, Optional

from featurebyte.enum import TableDataType
from featurebyte.models.entity_validation import EntityInfo
from featurebyte.models.parent_serving import EntityLookupStep, EntityLookupStepCreator
from featurebyte.query_graph.model.entity_lookup_plan import EntityLookupPlanner
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.service.entity import EntityService
from featurebyte.service.item_table import ExtendedItemTableService
from featurebyte.service.relationship_info import RelationshipInfoService
from featurebyte.service.table import TableService


class ParentEntityLookupService:
    """
    ParentEntityLookupService is responsible for identifying the joins required to lookup parent
    entities in order to serve parent features given child entities
    """

    def __init__(
        self,
        entity_service: EntityService,
        table_service: TableService,
        extended_item_table_service: ExtendedItemTableService,
        relationship_info_service: RelationshipInfoService,
    ):
        self.entity_service = entity_service
        self.table_service = table_service
        self.extended_item_table_service = extended_item_table_service
        self.relationship_info_service = relationship_info_service

    async def get_required_join_steps(
        self,
        entity_info: EntityInfo,
        relationships_info: Optional[list[EntityRelationshipInfo]] = None,
    ) -> list[EntityLookupStep]:
        """
        Get the list of required JoinStep to lookup the missing entities in the request

        Parameters
        ----------
        entity_info: EntityInfo
            Entity information
        relationships_info: Optional[list[EntityRelationshipInfo]]
            Relationships that can be used to derive the join steps. If not provided, the currently
            available relationships will be queried from persistent and used instead.

        Returns
        -------
        list[EntityLookupStep]
        """

        if entity_info.are_all_required_entities_provided():
            return []

        if relationships_info is None:
            # Use currently available relationships if frozen relationships are not available
            relationships_info = []
            async for info in self.relationship_info_service.list_documents_iterator(
                query_filter={},
            ):
                relationships_info.append(EntityRelationshipInfo(**info.model_dump(by_alias=True)))

        lookup_steps = EntityLookupPlanner.generate_lookup_steps(
            available_entity_ids=list(entity_info.provided_entity_ids),
            required_entity_ids=list(entity_info.required_entity_ids),
            relationships_info=relationships_info,
        )

        # all_join_steps is a mapping from parent serving name to JoinStep. Each parent serving name
        # should be looked up exactly once and then reused.
        all_join_steps: dict[str, EntityLookupStep] = OrderedDict()

        for join_step in await self.get_entity_lookup_steps(lookup_steps, entity_info):
            if join_step.parent.serving_name not in all_join_steps:
                all_join_steps[join_step.parent.serving_name] = join_step

        return list(all_join_steps.values())

    async def get_entity_lookup_step_creator(
        self, entity_relationships_info: List[EntityRelationshipInfo]
    ) -> EntityLookupStepCreator:
        """
        Creates an instance of EntityLookupStepCreator

        Parameters
        ---------
        entity_relationships_info: List[EntityRelationshipInfo]
            List of EntityRelationshipInfo objects

        Returns
        -------
        EntityLookupStepCreator
        """
        # Retrieve all required models in batch
        all_entity_ids = set()
        all_table_ids = set()
        for info in entity_relationships_info:
            all_entity_ids.update([info.entity_id, info.related_entity_id])
            all_table_ids.add(info.relation_table_id)

        entities_by_id = {
            entity.id: entity
            async for entity in self.entity_service.list_documents_iterator(
                query_filter={"_id": {"$in": list(all_entity_ids)}}
            )
        }
        tables_by_id = {
            table.id: table
            async for table in self.table_service.list_documents_iterator(
                query_filter={"_id": {"$in": list(all_table_ids)}}
            )
        }
        for table_id, table in tables_by_id.items():
            if table.type == TableDataType.ITEM_TABLE:
                item_table = (
                    await self.extended_item_table_service.get_document_with_event_table_model(
                        table_id
                    )
                )
                tables_by_id[table_id] = item_table

        return EntityLookupStepCreator(
            entity_relationships_info=entity_relationships_info,
            entities_by_id=entities_by_id,
            tables_by_id=tables_by_id,
        )

    async def get_entity_lookup_steps(
        self,
        entity_relationships_info: List[EntityRelationshipInfo],
        entity_info: Optional[EntityInfo] = None,
    ) -> list[EntityLookupStep]:
        """
        Convert a list of join path (list of EntityRelationshipInfo) into a list of EntityLookupStep

        Parameters
        ---------
        entity_relationships_info: List[EntityRelationshipInfo]
            List of EntityRelationshipInfo objects
        entity_info: EntityInfo
            Entity information

        Returns
        -------
        list[EntityLookupStep]
        """
        entity_lookup_step_creator = await self.get_entity_lookup_step_creator(
            entity_relationships_info
        )

        join_steps = []
        for info in entity_relationships_info:
            parent_entity = entity_lookup_step_creator.entities_by_id[info.related_entity_id]
            child_entity = entity_lookup_step_creator.entities_by_id[info.entity_id]
            join_step = entity_lookup_step_creator.get_entity_lookup_step(
                info.id,
                child_serving_name_override=(
                    entity_info.get_effective_serving_name(child_entity)
                    if entity_info is not None
                    else None
                ),
                parent_serving_name_override=(
                    entity_info.get_effective_serving_name(parent_entity)
                    if entity_info is not None
                    else None
                ),
            )
            join_steps.append(join_step)

        return join_steps
