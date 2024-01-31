"""
OfflineStoreFeatureTableConstructionService class
"""
from __future__ import annotations

from typing import Dict, List, Optional, Sequence

from bson import ObjectId

from featurebyte import FeatureJobSetting, SourceType
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.entity import EntityModel
from featurebyte.models.entity_universe import (
    EntityUniverseModel,
    EntityUniverseParams,
    construct_window_aggregates_universe,
    get_combined_universe,
)
from featurebyte.models.entity_validation import EntityInfo
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureCluster
from featurebyte.models.offline_store_feature_table import (
    OfflineStoreFeatureTableModel,
    get_combined_ingest_graph,
)
from featurebyte.models.offline_store_ingest_query import OfflineStoreIngestQueryGraph
from featurebyte.models.sqlglot_expression import SqlglotExpressionModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import LookupNode
from featurebyte.query_graph.node.mixin import BaseGroupbyParameters
from featurebyte.service.entity import EntityService
from featurebyte.service.entity_serving_names import EntityServingNamesService
from featurebyte.service.parent_serving import ParentEntityLookupService


class OfflineStoreFeatureTableConstructionService:
    """
    OfflineStoreFeatureTableConstructionService class
    """

    def __init__(
        self,
        entity_service: EntityService,
        parent_entity_lookup_service: ParentEntityLookupService,
        entity_serving_names_service: EntityServingNamesService,
    ):
        self.entity_service = entity_service
        self.parent_entity_lookup_service = parent_entity_lookup_service
        self.entity_serving_names_service = entity_serving_names_service

    async def get_dummy_offline_store_feature_table_model(
        self,
        primary_entity_ids: Sequence[ObjectId],
        feature_job_setting: Optional[FeatureJobSetting],
        has_ttl: bool,
        feature_store_id: ObjectId,
        catalog_id: ObjectId,
        table_name_prefix: str,
        entity_id_to_serving_name: Optional[Dict[ObjectId, str]] = None,
    ) -> OfflineStoreFeatureTableModel:
        """
        Returns a dummy OfflineStoreFeatureTableModel for a feature table

        Parameters
        ----------
        primary_entity_ids: Sequence[ObjectId]
            List of primary entity ids
        feature_job_setting: Optional[FeatureJobSetting]
            Feature job setting of the feature table
        has_ttl: bool
            Whether the feature table has TTL
        feature_store_id: ObjectId
            Feature store id
        catalog_id: ObjectId
            Catalog id
        table_name_prefix: str
            Registry project name
        entity_id_to_serving_name: Optional[Dict[ObjectId, str]]
            Entity id to serving name mapping

        Returns
        -------
        OfflineStoreFeatureTableModel
        """
        if entity_id_to_serving_name is None:
            entity_id_to_serving_name = await self.entity_serving_names_service.get_entity_id_to_serving_name_for_offline_store(
                entity_ids=primary_entity_ids
            )

        feature_cluster = FeatureCluster(
            feature_store_id=feature_store_id,
            graph=QueryGraph(),
            node_names=[],
        )
        return OfflineStoreFeatureTableModel(
            name="",  # to be filled in later
            name_prefix=table_name_prefix,
            feature_ids=[],
            primary_entity_ids=primary_entity_ids,
            serving_names=[
                entity_id_to_serving_name[entity_id] for entity_id in primary_entity_ids
            ],
            feature_cluster=feature_cluster,
            output_column_names=[],
            output_dtypes=[],
            entity_universe=None,
            has_ttl=has_ttl,
            feature_job_setting=feature_job_setting.normalize() if feature_job_setting else None,
            feature_store_id=feature_store_id,
            catalog_id=catalog_id,
        )

    async def get_offline_store_feature_table_model(
        self,
        feature_table_name: str,
        features: List[FeatureModel],
        aggregate_result_table_names: List[str],
        primary_entities: List[EntityModel],
        has_ttl: bool,
        feature_job_setting: Optional[FeatureJobSetting],
        source_type: SourceType,
    ) -> OfflineStoreFeatureTableModel:
        """
        Returns a OfflineStoreFeatureTableModel for a feature table

        Parameters
        ----------
        feature_table_name : str
            Feature table name
        features : List[FeatureModel]
            List of features
        aggregate_result_table_names : List[str]
            List of aggregate result table names
        primary_entities : List[EntityModel]
            List of primary entities
        has_ttl : bool
            Whether the feature table has TTL
        feature_job_setting : Optional[FeatureJobSetting]
            Feature job setting of the feature table
        source_type : SourceType
            Source type information

        Returns
        -------
        OfflineStoreFeatureTableModel
        """
        ingest_graph_metadata = get_combined_ingest_graph(
            features=features,
            primary_entities=primary_entities,
            has_ttl=has_ttl,
            feature_job_setting=feature_job_setting,
        )

        entity_universe = await self.get_entity_universe_model(
            serving_names=[entity.serving_names[0] for entity in primary_entities],
            aggregate_result_table_names=aggregate_result_table_names,
            offline_ingest_graphs=ingest_graph_metadata.offline_ingest_graphs,
            source_type=source_type,
        )

        return OfflineStoreFeatureTableModel(
            name=feature_table_name,
            feature_ids=[feature.id for feature in features],
            primary_entity_ids=[entity.id for entity in primary_entities],
            # FIXME: consolidate all the offline store serving names (get_entity_id_to_serving_name_for_offline_store)
            serving_names=[entity.serving_names[0] for entity in primary_entities],
            feature_cluster=ingest_graph_metadata.feature_cluster,
            output_column_names=ingest_graph_metadata.output_column_names,
            output_dtypes=ingest_graph_metadata.output_dtypes,
            entity_universe=entity_universe,
            has_ttl=has_ttl,
            feature_job_setting=feature_job_setting.normalize() if feature_job_setting else None,
        )

    async def get_entity_universe_model(
        self,
        serving_names: List[str],
        aggregate_result_table_names: List[str],
        offline_ingest_graphs: List[OfflineStoreIngestQueryGraph],
        source_type: SourceType,
    ) -> EntityUniverseModel:
        """
        Create a new EntityUniverseModel object

        Parameters
        ----------
        serving_names: List[str]
            The serving names of the entities
        aggregate_result_table_names: List[str]
            The names of the aggregate result tables for window aggregates
        offline_ingest_graphs: List[OfflineStoreIngestQueryGraph]
            The offline ingest graphs that the entity universe is to be constructed from
        source_type: SourceType
            Source type information

        Returns
        -------
        EntityUniverse
        """
        if aggregate_result_table_names:
            universe_expr = construct_window_aggregates_universe(
                serving_names=serving_names,
                aggregate_result_table_names=aggregate_result_table_names,
            )
        else:
            params = []
            for offline_ingest_graph in offline_ingest_graphs:
                primary_entities = [
                    (await self.entity_service.get_document(entity_id))
                    for entity_id in offline_ingest_graph.primary_entity_ids
                ]
                for info in offline_ingest_graph.aggregation_nodes_info:
                    node = offline_ingest_graph.graph.get_node_by_name(info.node_name)
                    # The entity universe has to be described in terms of the primary entity. If the
                    # aggregation is based on a parent entity, we need to map it back to the primary
                    # entity (a child).
                    non_primary_entity_ids = self._get_non_primary_entity_ids(
                        node,
                        offline_ingest_graph.primary_entity_ids,
                    )
                    if non_primary_entity_ids:
                        entity_info = EntityInfo(
                            required_entities=[
                                (await self.entity_service.get_document(entity_id))
                                for entity_id in non_primary_entity_ids
                            ],
                            provided_entities=primary_entities,
                        )
                        join_steps = (
                            await self.parent_entity_lookup_service.get_required_join_steps(
                                entity_info
                            )
                        )
                    else:
                        join_steps = None
                    params.append(
                        EntityUniverseParams(
                            graph=offline_ingest_graph.graph,
                            node=node,
                            join_steps=join_steps,
                        )
                    )
            universe_expr = get_combined_universe(params, source_type)

        return EntityUniverseModel(query_template=SqlglotExpressionModel.create(universe_expr))

    @staticmethod
    def _get_non_primary_entity_ids(
        node: Node, offline_ingest_graph_primary_entity_ids: List[PydanticObjectId]
    ) -> List[PydanticObjectId]:
        node_entity_ids = None
        if isinstance(node.parameters, BaseGroupbyParameters):
            node_entity_ids = node.parameters.entity_ids
        elif isinstance(node, LookupNode):
            node_entity_ids = [node.parameters.entity_id]
        assert node_entity_ids is not None
        return [
            entity_id
            for entity_id in node_entity_ids
            if entity_id not in offline_ingest_graph_primary_entity_ids
        ]
