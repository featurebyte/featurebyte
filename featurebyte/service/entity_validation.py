"""
Module to support serving using parent-child relationship
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

from bson import ObjectId

from featurebyte.enum import DBVarType
from featurebyte.exception import RequiredEntityNotProvidedError, UnexpectedServingNamesMappingError
from featurebyte.models.entity_validation import EntityInfo
from featurebyte.models.feature_list import FeatureCluster, FeatureListModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.models.parent_serving import EntityRelationshipsContext, ParentServingPreparation
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import FeatureStoreDetails
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner
from featurebyte.service.entity import EntityService
from featurebyte.service.parent_serving import ParentEntityLookupService


def to_use_frozen_relationships(
    feature_cluster_like: Optional[List[FeatureCluster]] | Optional[FeatureCluster],
) -> bool:
    """
    Whether the feature cluster has the field feature_node_relationships_infos populated. If so, sql
    generation should use it to compute features based on frozen relationships.

    Parameters
    ----------
    feature_cluster_like: Optional[List[FeatureCluster]] | Optional[FeatureCluster]
        The feature cluster like object from FeatureListModel or OfflineStoreFeatureTableModel

    Returns
    -------
    bool
    """
    if feature_cluster_like is None:
        return False
    if isinstance(feature_cluster_like, list):
        feature_cluster = feature_cluster_like[0]
    else:
        feature_cluster = feature_cluster_like
    return feature_cluster.feature_node_relationships_infos is not None


@dataclass
class EntityRelationshipsContextParameters:
    """
    Parameters required to construct a EntityRelationshipsContext object

    Can be created from FeatureListModel or OfflineFeatureStoreTableModel objects.
    """

    # Graph and nodes representing a list of features
    feature_cluster: FeatureCluster

    # Primary entity ids of the features represented by the feature cluster
    primary_entity_ids: Sequence[ObjectId]

    # Relationships available to lookup primary entity from serving entity
    relationships_info: Optional[List[EntityRelationshipInfo]]

    @classmethod
    def from_feature_list(
        cls, feature_list_model: FeatureListModel
    ) -> Optional[EntityRelationshipsContextParameters]:
        """
        Create EntityRelationshipsContextParameters from a FeatureListModel

        Parameters
        ----------
        feature_list_model: FeatureListModel
            Feature list model

        Returns
        -------
        Optional[EntityRelationshipsContextParameters]
            None for legacy documents without the required information
        """
        if feature_list_model.feature_clusters is not None:
            feature_cluster = feature_list_model.feature_clusters[0]
            if feature_cluster.feature_node_relationships_infos is not None:
                return EntityRelationshipsContextParameters(
                    feature_cluster=feature_cluster,
                    primary_entity_ids=feature_list_model.primary_entity_ids,
                    relationships_info=feature_list_model.relationships_info,
                )
        return None

    @classmethod
    def from_offline_store_feature_table(
        cls, offline_store_feature_table_model: OfflineStoreFeatureTableModel
    ) -> Optional[EntityRelationshipsContextParameters]:
        """
        Create EntityRelationshipsContextParameters from a OfflineStoreFeatureTableModel

        Parameters
        ----------
        offline_store_feature_table_model: OfflineStoreFeatureTableModel
            Offline store feature table model

        Returns
        -------
        Optional[EntityRelationshipsContextParameters]
            None for legacy documents without the required information
        """
        if offline_store_feature_table_model.feature_cluster is not None:
            feature_cluster = offline_store_feature_table_model.feature_cluster
            if feature_cluster.feature_node_relationships_infos is not None:
                # Set relationships_info to None because when computing features for offline store
                # feature tables, the serving entity is always the primary entity of the feature
                # table, so no lookup is required there.
                return EntityRelationshipsContextParameters(
                    feature_cluster=feature_cluster,
                    primary_entity_ids=offline_store_feature_table_model.primary_entity_ids,
                    relationships_info=None,
                )
        return None


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
        offline_store_feature_table_model: Optional[OfflineStoreFeatureTableModel] = None,
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
        offline_store_feature_table_model: Optional[OfflineStoreFeatureTableModel]
            Offline store feature table model when the request is initiated by feature materialize
            service

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

        # Extract required entities from feature cluster (faster) or graph
        required_entity_ids: Optional[Sequence[ObjectId]] = None
        if feature_list_model is not None:
            if to_use_frozen_relationships(feature_list_model.feature_clusters):
                required_entity_ids = feature_list_model.primary_entity_ids
            else:
                required_entity_ids = feature_list_model.entity_ids
        elif offline_store_feature_table_model is not None:
            if to_use_frozen_relationships(offline_store_feature_table_model.feature_cluster):
                required_entity_ids = offline_store_feature_table_model.primary_entity_ids

        if required_entity_ids is None:
            assert graph_nodes is not None
            graph, nodes = graph_nodes
            planner = FeatureExecutionPlanner(
                graph=graph,
                is_online_serving=False,
                serving_names_mapping=serving_names_mapping,
            )
            required_entity_ids = list(planner.generate_plan(nodes).required_entity_ids)

        required_entities = await self.entity_service.get_entities(set(required_entity_ids))

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
        offline_store_feature_table_model: Optional[OfflineStoreFeatureTableModel] = None,
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
        offline_store_feature_table_model: Optional[OfflineStoreFeatureTableModel]
            Offline store feature table model when the request is initiated by feature materialize
            service

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
            offline_store_feature_table_model=offline_store_feature_table_model,
        )
        entity_relationships_context = await self._get_entity_relationships_context(
            entity_info,
            feature_list_model,
            offline_store_feature_table_model=offline_store_feature_table_model,
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
                    entity_info,
                    (
                        entity_relationships_context.feature_list_relationships_info
                        if entity_relationships_context is not None
                        else None
                    ),
                )
            except RequiredEntityNotProvidedError:
                raise RequiredEntityNotProvidedError(
                    entity_info.format_missing_entities_error([
                        entity.id for entity in entity_info.missing_entities
                    ])
                )

        feature_store_details = FeatureStoreDetails(**feature_store.model_dump())
        return ParentServingPreparation(
            join_steps=join_steps,
            feature_store_details=feature_store_details,
            entity_relationships_context=entity_relationships_context,
        )

    async def _get_entity_relationships_context(
        self,
        entity_info: EntityInfo,
        feature_list_model: Optional[FeatureListModel],
        offline_store_feature_table_model: Optional[OfflineStoreFeatureTableModel],
    ) -> Optional[EntityRelationshipsContext]:
        if feature_list_model is not None:
            parameters = EntityRelationshipsContextParameters.from_feature_list(feature_list_model)
        elif offline_store_feature_table_model is not None:
            parameters = EntityRelationshipsContextParameters.from_offline_store_feature_table(
                offline_store_feature_table_model
            )
        else:
            parameters = None
        if parameters is None:
            return None
        return await self._get_entity_relationships_context_from_parameters(
            entity_info=entity_info,
            parameters=parameters,
        )

    async def _get_entity_relationships_context_from_parameters(
        self,
        entity_info: EntityInfo,
        parameters: EntityRelationshipsContextParameters,
    ) -> EntityRelationshipsContext:
        all_relationships = set(parameters.relationships_info or [])
        all_relationships.update(parameters.feature_cluster.combined_relationships_info)
        entity_lookup_step_creator = (
            await self.parent_entity_lookup_service.get_entity_lookup_step_creator(
                list(all_relationships)
            )
        )
        return EntityRelationshipsContext(
            feature_list_primary_entity_ids=parameters.primary_entity_ids,
            feature_list_serving_names=[
                entity_info.get_effective_serving_name(entity_info.get_entity(entity_id))
                for entity_id in parameters.primary_entity_ids
            ],
            feature_list_relationships_info=parameters.relationships_info or [],
            feature_node_relationships_infos=parameters.feature_cluster.feature_node_relationships_infos,
            entity_lookup_step_creator=entity_lookup_step_creator,
        )

    async def validate_request_columns(
        self, columns_and_dtypes: dict[str, DBVarType], serving_entity_ids: List[ObjectId]
    ) -> None:
        """
        Validate that the serving entity IDs are present in the provided columns info.

        Parameters
        ----------
        columns_and_dtypes: dict[str, DBVarType]
            List of input columns and their data types
        serving_entity_ids: List[ObjectId]
            List of serving entity IDs that are expected to be present

        Raises
        ------
        RequiredEntityNotProvidedError
            If any of the serving entity IDs are not found in the provided columns info.
        """
        required_entities = await self.entity_service.get_entities(set(serving_entity_ids))
        missing_entities = []
        for entity in required_entities:
            serving_name = entity.serving_names[0]
            if serving_name not in columns_and_dtypes:
                missing_entities.append(entity)

        if missing_entities:
            entity_info = EntityInfo(
                provided_entities=[],
                required_entities=missing_entities,
                serving_names_mapping=None,
            )
            raise RequiredEntityNotProvidedError(
                entity_info.format_missing_entities_error(
                    sorted(entity.id for entity in missing_entities)
                )
            )
