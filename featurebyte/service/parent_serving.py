"""
Module to support serving parent features using child entities
"""
from __future__ import annotations

from typing import List

from collections import OrderedDict

from featurebyte.models.entity_validation import EntityInfo
from featurebyte.models.parent_serving import JoinStep
from featurebyte.query_graph.model.entity_lookup_plan import EntityLookupPlanner
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.service.entity import EntityService
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
        relationship_info_service: RelationshipInfoService,
    ):
        self.entity_service = entity_service
        self.table_service = table_service
        self.relationship_info_service = relationship_info_service

    async def get_required_join_steps(self, entity_info: EntityInfo) -> list[JoinStep]:
        """
        Get the list of required JoinStep to lookup the missing entities in the request

        Parameters
        ----------
        entity_info: EntityInfo
            Entity information

        Returns
        -------
        list[JoinStep]
        """

        if entity_info.are_all_required_entities_provided():
            return []

        relationships_info = []
        async for info in self.relationship_info_service.list_documents_iterator(
            query_filter={},
        ):
            relationships_info.append(EntityRelationshipInfo(**info.dict(by_alias=True)))

        lookup_steps = EntityLookupPlanner.generate_lookup_steps(
            available_entity_ids=list(entity_info.provided_entity_ids),
            required_entity_ids=list(entity_info.required_entity_ids),
            relationships_info=relationships_info,
        )

        # all_join_steps is a mapping from parent serving name to JoinStep. Each parent serving name
        # should be looked up exactly once and then reused.
        all_join_steps: dict[str, JoinStep] = OrderedDict()

        for join_step in await self._get_join_steps(entity_info, lookup_steps):
            if join_step.parent_serving_name not in all_join_steps:
                all_join_steps[join_step.parent_serving_name] = join_step

        return list(all_join_steps.values())

    async def _get_join_steps(
        self,
        entity_info: EntityInfo,
        entity_relationships_info: List[EntityRelationshipInfo],
    ) -> list[JoinStep]:
        """
        Convert a list of join path (list of EntityRelationshipInfo) into a list of JoinStep

        Parameters
        ---------
        entity_relationships_info: List[EntityRelationshipInfo]
            List of EntityRelationshipInfo objects

        Returns
        -------
        Dict[PydanticObjectId, EntityLookupStep]
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

        join_steps = []
        for info in entity_relationships_info:
            relation_table = tables_by_id[info.relation_table_id]
            parent_entity = entities_by_id[info.related_entity_id]
            child_entity = entities_by_id[info.entity_id]

            child_column_name = None
            parent_column_name = None
            for column_info in relation_table.columns_info:
                if column_info.entity_id == child_entity.id:
                    child_column_name = column_info.name
                elif column_info.entity_id == parent_entity.id:
                    parent_column_name = column_info.name
            assert child_column_name is not None
            assert parent_column_name is not None

            join_step = JoinStep(
                table=relation_table.dict(by_alias=True),
                parent_key=parent_column_name,
                parent_serving_name=entity_info.get_effective_serving_name(parent_entity),
                child_key=child_column_name,
                child_serving_name=entity_info.get_effective_serving_name(child_entity),
            )
            join_steps.append(join_step)

        return join_steps
