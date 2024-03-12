"""
Module to support serving using parent-child relationship
"""
from __future__ import annotations

from typing import Optional, Tuple

from featurebyte.exception import RequiredEntityNotProvidedError, UnexpectedServingNamesMappingError
from featurebyte.models.entity_validation import EntityInfo
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.parent_serving import EntityRelationshipsContext, ParentServingPreparation
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import FeatureStoreDetails
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner
from featurebyte.service.entity import EntityService
from featurebyte.service.parent_serving import ParentEntityLookupService


class EntityValidationService:
    """
    EntityValidationService class is responsible for validating that required entities are
    provided in feature requests during preview, historical features, and online serving.
    """

    def __init__(
        self,
        entity_service: EntityService,
        parent_entity_lookup_service: ParentEntityLookupService,
    ):
        self.entity_service = entity_service
        self.parent_entity_lookup_service = parent_entity_lookup_service

    async def get_entity_info_from_request(
        self,
        graph_nodes: Optional[Tuple[QueryGraphModel, list[Node]]],
        feature_list_model: Optional[FeatureListModel],
        request_column_names: set[str],
        serving_names_mapping: dict[str, str] | None = None,
    ) -> EntityInfo:
        """
        Create an EntityInfo instance given graph and request

        Parameters
        ----------
        graph_nodes : Optional[Tuple[QueryGraphModel, list[Node]]
            Query graph and nodes
        feature_list_model: Optional[FeatureListModel]
            Feature list model
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

        # Infer provided entities from provided request columns
        candidate_serving_names = {_get_original_serving_name(col) for col in request_column_names}
        provided_entities = await self.entity_service.get_entities_with_serving_names(
            candidate_serving_names
        )

        # Extract required entities from feature list (faster) or graph
        if feature_list_model is not None:
            if self._to_use_frozen_relationships(feature_list_model):
                required_entity_ids = feature_list_model.primary_entity_ids
            else:
                required_entity_ids = feature_list_model.entity_ids
            required_entities = await self.entity_service.get_entities(set(required_entity_ids))
        else:
            assert graph_nodes is not None
            graph, nodes = graph_nodes
            planner = FeatureExecutionPlanner(
                graph=graph,
                is_online_serving=False,
                serving_names_mapping=serving_names_mapping,
            )
            plan = planner.generate_plan(nodes)
            required_entities = await self.entity_service.get_entities(plan.required_entity_ids)

        return EntityInfo(
            provided_entities=provided_entities,
            required_entities=required_entities,
            serving_names_mapping=serving_names_mapping,
        )

    async def validate_entities_or_prepare_for_parent_serving(
        self,
        request_column_names: set[str],
        feature_store: FeatureStoreModel,
        graph_nodes: Optional[Tuple[QueryGraphModel, list[Node]]] = None,
        feature_list_model: Optional[FeatureListModel] = None,
        serving_names_mapping: dict[str, str] | None = None,
    ) -> ParentServingPreparation:
        """
        Validate that entities are provided correctly in feature requests

        Parameters
        ----------
        graph_nodes : Optional[Tuple[QueryGraphModel, list[Node]]]
            Query graph and nodes
        feature_list_model: Optional[FeatureListModel]
            Feature list model
        request_column_names: set[str]
            Column names provided in the request
        feature_store: FeatureStoreModel
            FeatureStoreModel object to be used to extract FeatureStoreDetails
        serving_names_mapping : dict[str, str] | None
            Optional serving names mapping if the entities are provided under different serving
            names in the request

        Returns
        -------
        ParentServingPreparation

        Raises
        ------
        RequiredEntityNotProvidedError
            When any of the required entities is not provided in the request and it is not possible
            to retrieve the parent entities using existing relationships
        UnexpectedServingNamesMappingError
            When unexpected keys are provided in serving_names_mapping
        """
        assert graph_nodes is not None or feature_list_model is not None

        entity_info = await self.get_entity_info_from_request(
            graph_nodes=graph_nodes,
            feature_list_model=feature_list_model,
            request_column_names=request_column_names,
            serving_names_mapping=serving_names_mapping,
        )

        if serving_names_mapping is not None:
            provided_serving_names = {
                entity.serving_names[0] for entity in entity_info.provided_entities
            }
            unexpected_keys = {
                k for k in serving_names_mapping.keys() if k not in provided_serving_names
            }
            if unexpected_keys:
                unexpected_keys_str = ", ".join(sorted(unexpected_keys))
                raise UnexpectedServingNamesMappingError(
                    f"Unexpected serving names provided in serving_names_mapping: {unexpected_keys_str}"
                )

        if entity_info.are_all_required_entities_provided():
            join_steps = []
        else:
            # Try to see if missing entities can be obtained using the provided entities as children
            try:
                join_steps = await self.parent_entity_lookup_service.get_required_join_steps(
                    entity_info
                )
            except RequiredEntityNotProvidedError:
                raise RequiredEntityNotProvidedError(  # pylint: disable=raise-missing-from
                    entity_info.format_missing_entities_error(
                        [entity.id for entity in entity_info.missing_entities]
                    )
                )

        feature_store_details = FeatureStoreDetails(**feature_store.dict())
        entity_relationships_context = await self._get_entity_relationships_context(
            entity_info,
            feature_list_model,
        )
        return ParentServingPreparation(
            join_steps=join_steps,
            feature_store_details=feature_store_details,
            entity_relationships_context=entity_relationships_context,
        )

    @staticmethod
    def _to_use_frozen_relationships(feature_list_model: FeatureListModel) -> bool:
        return (
            feature_list_model.feature_clusters is not None
            and feature_list_model.feature_clusters[0].feature_node_relationships_infos is not None
        )

    async def _get_entity_relationships_context(
        self,
        entity_info: EntityInfo,
        feature_list_model: Optional[FeatureListModel],
    ) -> Optional[EntityRelationshipsContext]:
        if feature_list_model is None or feature_list_model.feature_clusters is None:
            return None

        feature_cluster = feature_list_model.feature_clusters[0]
        if feature_cluster.feature_node_relationships_infos is None:
            return None

        all_relationships = set(feature_list_model.relationships_info or [])
        all_relationships.update(feature_list_model.feature_clusters[0].combined_relationships_info)
        entity_lookup_step_creator = (
            await self.parent_entity_lookup_service.get_entity_lookup_step_creator(
                list(all_relationships)
            )
        )
        return EntityRelationshipsContext(
            feature_list_primary_entity_ids=feature_list_model.primary_entity_ids,
            feature_list_serving_names=[
                entity_info.get_effective_serving_name(entity_info.get_entity(entity_id))
                for entity_id in feature_list_model.primary_entity_ids
            ],
            feature_list_relationships_info=feature_list_model.relationships_info,
            feature_node_relationships_infos=feature_cluster.feature_node_relationships_infos,
            entity_lookup_step_creator=entity_lookup_step_creator,
        )
