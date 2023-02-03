"""
Module to support serving using parent-child relationship
"""
from __future__ import annotations

from typing import Any, List

from bson import ObjectId
from pydantic import validator

from featurebyte.exception import RequiredEntityNotProvidedError
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.entity import EntityModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner
from featurebyte.service.base_service import BaseService
from featurebyte.service.entity import EntityService


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
    @classmethod
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
        """
        Create an EntityInfo instance given graph and request

        Parameters
        ----------
        graph : QueryGraphModel
            Query graph
        nodes : list[Node]
            List of query graph node
        request_column_names: set[str]
            Column names provided in the request
        serving_names_mapping : dict[str, str] | None
            Optional serving names mapping if the entities are provided under different serving
            names in the request

        Returns
        -------
        EntityInfo
        """

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
        """
        Validate that entities are provided correctly in feature requests

        Parameters
        ----------
        graph : QueryGraphModel
            Query graph
        nodes : list[Node]
            List of query graph node
        request_column_names: set[str]
            Column names provided in the request
        serving_names_mapping : dict[str, str] | None
            Optional serving names mapping if the entities are provided under different serving
            names in the request
        """

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
