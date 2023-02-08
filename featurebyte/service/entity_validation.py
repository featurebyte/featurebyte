"""
Module to support serving using parent-child relationship
"""
from __future__ import annotations

from typing import Any, List, Optional

from featurebyte.exception import EntityJoinPathNotFoundError, RequiredEntityNotProvidedError
from featurebyte.models.entity_validation import EntityInfo
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.parent_serving import JoinStep, ParentServingPreparation
from featurebyte.persistent import Persistent
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import FeatureStoreDetails
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner
from featurebyte.service.base_service import BaseService
from featurebyte.service.entity import EntityService
from featurebyte.service.parent_serving import ParentEntityLookupService


class EntityValidationService(BaseService):
    """
    EntityValidationService class is responsible for validating that required entities are
    provided in feature requests during preview, historical features, and online serving.
    """

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        entity_service: EntityService,
        parent_entity_lookup_service: ParentEntityLookupService,
    ):
        super().__init__(user, persistent)
        self.entity_service = entity_service
        self.parent_entity_lookup_service = parent_entity_lookup_service

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

    async def validate_entities_or_prepare_for_parent_serving(
        self,
        graph: QueryGraphModel,
        nodes: list[Node],
        request_column_names: set[str],
        feature_store: FeatureStoreModel,
        serving_names_mapping: dict[str, str] | None = None,
    ) -> Optional[ParentServingPreparation]:

        join_steps = await self.validate_provided_entities(
            graph=graph,
            nodes=nodes,
            request_column_names=request_column_names,
            serving_names_mapping=serving_names_mapping,
        )

        if join_steps is None:
            parent_serving_preparation = None
        else:
            feature_store_details = FeatureStoreDetails(**feature_store.dict())
            parent_serving_preparation = ParentServingPreparation(
                join_steps=join_steps, feature_store_details=feature_store_details
            )

        return parent_serving_preparation

    async def validate_provided_entities(
        self,
        graph: QueryGraphModel,
        nodes: list[Node],
        request_column_names: set[str],
        serving_names_mapping: dict[str, str] | None = None,
    ) -> Optional[List[JoinStep]]:
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

        Raises
        ------
        RequiredEntityNotProvidedError
            When any of the required entities is not provided in the request
        """

        entity_info = await self.get_entity_info_from_request(
            graph=graph,
            nodes=nodes,
            request_column_names=request_column_names,
            serving_names_mapping=serving_names_mapping,
        )

        if entity_info.are_all_required_entities_provided():
            return None

        try:
            join_steps = await self.parent_entity_lookup_service.get_required_join_steps(
                entity_info
            )
        except EntityJoinPathNotFoundError:
            join_steps = None

        if join_steps is None:
            missing_entities = sorted(entity_info.missing_entities, key=lambda x: x.name)  # type: ignore
            formatted_pairs = []
            for entity in missing_entities:
                formatted_pairs.append(f'{entity.name} (serving name: "{entity.serving_names[0]}")')
            formatted_pairs_str = ", ".join(formatted_pairs)
            raise RequiredEntityNotProvidedError(
                f"Required entities are not provided in the request: {formatted_pairs_str}"
            )

        return join_steps
