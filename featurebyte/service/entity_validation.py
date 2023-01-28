"""
Module to support serving using parent-child relationship
"""
from __future__ import annotations

from typing import Any, List, Tuple

from collections import defaultdict

from bson import ObjectId
from pydantic import validator

from featurebyte.exception import RequiredEntityNotProvidedError
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.entity import EntityModel
from featurebyte.models.serving import JoinStep
from featurebyte.persistent import Persistent
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner
from featurebyte.service.base_service import BaseService
from featurebyte.service.entity import EntityService
from featurebyte.service.tabular_data import DataService


class EntityInfo(FeatureByteBaseModel):
    """
    EntityInfo captures the entity information provided in the request

    required_entities: List[EntityModel]
        List of entities required to by the feature / feature list
    provided_entities: List[EntityModel]
        List of entities provided in the request
    """

    required_entities: List[EntityModel]
    provided_entities: List[EntityModel]

    @validator("required_entities", "provided_entities")
    def _deduplicate_entities(cls, val: List[EntityModel]) -> List[EntityModel]:
        entities_dict: dict[ObjectId, EntityModel] = {}
        for entity in val:
            entities_dict[entity.id] = entity
        return list(entities_dict.values())

    def are_all_required_entities_provided(self) -> bool:
        """
        Returns whether all the required entities are provided in the request

        Returns
        -------
        bool
        """
        return self.required_entity_ids <= self.provided_entity_ids

    @property
    def required_entity_ids(self) -> set[ObjectId]:
        """
        Set of the required entity ids

        Returns
        -------
        set[ObjectId]
        """
        return {entity.id for entity in self.required_entities}

    @property
    def provided_entity_ids(self) -> set[ObjectId]:
        """
        Set of provided entity ids

        Returns
        -------
        set[ObjectId]
        """
        return {entity.id for entity in self.provided_entities}

    @property
    def missing_entities(self) -> List[EntityModel]:
        """
        List of entities that are required but not provided

        Returns
        -------
        List[EntityModel]
        """
        provided_ids = self.provided_entity_ids
        return [entity for entity in self.required_entities if entity.id not in provided_ids]


class EntityValidationService(BaseService):
    """
    EntityValidationService class is responsible for validating that required entities are
    provided in feature requests during preview, historical features, and online serving.
    """

    def __init__(self, user: Any, persistent: Persistent):
        super().__init__(user, persistent)
        self.entity_service = EntityService(user, persistent)

    async def get_entity_info_from_request(
        self,
        graph: QueryGraphModel,
        nodes: list[Node],
        request_column_names: set[str],
        serving_names_mapping: dict[str, str] | None = None,
    ) -> EntityInfo:

        # serving_names_mapping: original serving names to overridden serving name
        # inv_serving_names_mapping: overridden serving name to original serving name
        if serving_names_mapping is not None:
            inv_serving_names_mapping = {v: k for (k, v) in serving_names_mapping.items()}
        else:
            inv_serving_names_mapping = None

        def _get_original_serving_name(serving_name: str) -> str:
            if inv_serving_names_mapping is not None and serving_name in inv_serving_names_mapping:
                return inv_serving_names_mapping[serving_name]
            return serving_name

        planner = FeatureExecutionPlanner(
            graph=graph,
            is_online_serving=False,
            serving_names_mapping=serving_names_mapping,
        )
        plan = planner.generate_plan(nodes)
        candidate_serving_names = {_get_original_serving_name(col) for col in request_column_names}
        provided_entities = await self.entity_service.get_entities_with_serving_names(
            candidate_serving_names
        )
        required_entities = await self.entity_service.get_entities(plan.required_entity_ids)

        return EntityInfo(
            provided_entities=provided_entities,
            required_entities=required_entities,
        )

    async def validate_provided_entities(
        self,
        graph: QueryGraphModel,
        nodes: list[Node],
        request_column_names: set[str],
        serving_names_mapping: dict[str, str] | None = None,
    ) -> None:

        entity_info = await self.get_entity_info_from_request(
            graph=graph,
            nodes=nodes,
            request_column_names=request_column_names,
            serving_names_mapping=serving_names_mapping,
        )

        if not entity_info.are_all_required_entities_provided():
            missing_entities = sorted(entity_info.missing_entities, key=lambda x: x.name)  # type: ignore
            formatted_pairs = []
            for entity in missing_entities:
                formatted_pairs.append(f'{entity.name} (serving name: "{entity.serving_names[0]}")')
            formatted_pairs_str = ", ".join(formatted_pairs)
            raise RequiredEntityNotProvidedError(
                f"Required entities are not provided in the request: {formatted_pairs_str}"
            )


class ParentEntityLookupService(BaseService):
    def __init__(self, user: Any, persistent: Persistent):
        super().__init__(user, persistent)
        self.entity_service = EntityService(user, persistent)
        self.data_service = DataService(user, persistent)

    async def get_required_join_steps(self, entity_info: EntityInfo) -> list[JoinStep]:

        if entity_info.are_all_required_entities_provided():
            return []

        all_join_steps = []
        for entity in entity_info.missing_entities:
            join_path = await self.get_entity_join_path(entity.id, entity_info.provided_entity_ids)
            join_steps = await self.get_join_steps_from_join_path(join_path)
            for join_step in join_steps:
                if join_step not in all_join_steps:
                    all_join_steps.append(join_step)

        return all_join_steps

    async def get_join_steps_from_join_path(self, join_path: list[ObjectId]) -> list[JoinStep]:

        join_steps = []

        for child_entity_id, parent_entity_id in zip(join_path, join_path[1:]):

            parents = (await self.entity_service.get_document(child_entity_id)).parents
            relationship = next(parent for parent in parents if parent.id == parent_entity_id)

            data = await self.data_service.get_document(relationship.data_id)
            child_key, parent_key = None, None
            for column_info in data.columns_info:
                name = column_info.name
                if column_info.entity_id == child_entity_id:
                    child_key = name
                if column_info.entity_id == parent_entity_id:
                    parent_key = name

            assert child_key is not None
            assert parent_key is not None

            join_step = JoinStep(
                data=data,
                parent_key=parent_key,
                child_key=child_key,
            )
            join_steps.append(join_step)

        return join_steps

    async def get_entity_join_path(
        self,
        required_entity: ObjectId,
        provided_entities: set[ObjectId],
    ) -> list[ObjectId]:

        pending: List[Tuple[ObjectId, List[ObjectId]]] = [(required_entity, [])]

        join_path = None
        visited = defaultdict(bool)

        while pending:

            (current_entity, current_path), pending = pending[0], pending[1:]
            updated_path = [current_entity] + current_path

            if current_entity in provided_entities:
                join_path = updated_path
                break

            visited[current_entity] = True
            for child_entity in await self.entity_service.get_children_entities(current_entity):
                if not visited[child_entity.id]:
                    pending.append((child_entity.id, updated_path))

        if join_path is None:
            raise ValueError("Cannot find a join path for entity")

        return join_path
