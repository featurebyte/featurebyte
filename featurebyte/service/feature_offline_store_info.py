"""
Feature Offline Store Info Initialization Service
"""
from typing import Dict, Optional, Sequence, Tuple

from bson import ObjectId

from featurebyte.models.feature import FeatureModel
from featurebyte.models.offline_store_ingest_query import (
    OfflineStoreInfo,
    OfflineStoreInfoMetadata,
    ServingNameInfo,
)
from featurebyte.query_graph.enum import GraphNodeType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node.base import BaseNode
from featurebyte.query_graph.node.nested import OfflineStoreIngestQueryGraphNodeParameters
from featurebyte.query_graph.transform.decompose_point import FeatureJobSettingExtractor
from featurebyte.query_graph.transform.offline_store_ingest import (
    OfflineStoreIngestQueryGraphTransformer,
)
from featurebyte.service.catalog import CatalogService
from featurebyte.service.entity_serving_names import EntityServingNamesService
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService
from featurebyte.service.offline_store_feature_table_construction import (
    OfflineStoreFeatureTableConstructionService,
)


class OfflineStoreInfoInitializationService:
    """
    OfflineStoreInfoInitializationService class
    """

    def __init__(
        self,
        catalog_service: CatalogService,
        offline_store_feature_table_service: OfflineStoreFeatureTableService,
        offline_store_feature_table_construction_service: OfflineStoreFeatureTableConstructionService,
        entity_serving_names_service: EntityServingNamesService,
    ) -> None:
        self.catalog_service = catalog_service
        self.offline_store_feature_table_service = offline_store_feature_table_service
        self.offline_store_feature_table_construction_service = (
            offline_store_feature_table_construction_service
        )
        self.entity_serving_names_service = entity_serving_names_service

    async def offline_store_feature_table_creator(
        self,
        primary_entity_ids: Sequence[ObjectId],
        feature_job_setting: Optional[FeatureJobSetting],
        has_ttl: bool,
        catalog_id: ObjectId,
        entity_id_to_serving_name: Optional[Dict[ObjectId, str]] = None,
    ) -> str:
        """
        Create offline store feature table name

        Parameters
        ----------
        primary_entity_ids: Sequence[ObjectId]
            Primary entity ids
        feature_job_setting: Optional[FeatureJobSetting]
            Feature job setting
        has_ttl: bool
            Whether the offline store feature table has ttl
        catalog_id: ObjectId
            Catalog id
        entity_id_to_serving_name: Optional[Dict[ObjectId, str]]
            Entity id to serving name mapping

        Returns
        -------
        str
        """
        construct_service = self.offline_store_feature_table_construction_service
        catalog = await self.catalog_service.get_document(catalog_id)
        table = await construct_service.get_dummy_offline_store_feature_table_model(
            primary_entity_ids=primary_entity_ids,
            feature_job_setting=feature_job_setting,
            has_ttl=has_ttl,
            feature_store_id=catalog.default_feature_store_ids[0],
            catalog_id=catalog_id,
            entity_id_to_serving_name=entity_id_to_serving_name,
        )
        persist_table = await self.offline_store_feature_table_service.get_or_create_document(
            data=table
        )
        return persist_table.name

    async def reconstruct_decomposed_graph(
        self,
        graph: QueryGraphModel,
        node_name: str,
        catalog_id: ObjectId,
        entity_id_to_serving_name: Optional[Dict[ObjectId, str]] = None,
    ) -> Tuple[QueryGraphModel, str]:
        """
        Reconstruct decomposed graph

        Parameters
        ----------
        graph: QueryGraphModel
            Query graph model
        node_name: str
            Node name
        catalog_id: ObjectId
            Catalog id
        entity_id_to_serving_name: Optional[Dict[ObjectId, str]]
            Entity id to serving name mapping

        Returns
        -------
        Tuple[QueryGraphModel, str]
        """
        query_graph = QueryGraph(**graph.dict(by_alias=True))

        node_name_to_repl_node: Dict[str, BaseNode] = {}
        for node in query_graph.iterate_sorted_graph_nodes(
            graph_node_types={GraphNodeType.OFFLINE_STORE_INGEST_QUERY}
        ):
            assert isinstance(node.parameters, OfflineStoreIngestQueryGraphNodeParameters)
            node.parameters.offline_store_table_name = (
                await self.offline_store_feature_table_creator(
                    primary_entity_ids=node.parameters.primary_entity_ids,
                    feature_job_setting=node.parameters.feature_job_setting,
                    has_ttl=node.parameters.has_ttl,
                    catalog_id=catalog_id,
                    entity_id_to_serving_name=entity_id_to_serving_name,
                )
            )
            node_name_to_repl_node[node.name] = node

        new_graph, node_name_map = query_graph.reconstruct(
            node_name_to_replacement_node=node_name_to_repl_node,
            regenerate_groupby_hash=False,
        )
        return new_graph, node_name_map[node_name]

    async def initialize_offline_store_info(
        self, feature: FeatureModel, entity_id_to_serving_name: Optional[Dict[ObjectId, str]] = None
    ) -> OfflineStoreInfo:
        """
        Initialize feature offline store info

        Parameters
        ----------
        feature: FeatureModel
            Feature
        entity_id_to_serving_name: Optional[Dict[ObjectId, str]]
            Entity id to serving name mapping

        Returns
        -------
        OfflineStoreInfo
        """
        if entity_id_to_serving_name is None:
            entity_id_to_serving_name = await self.entity_serving_names_service.get_entity_id_to_serving_name_for_offline_store(
                entity_ids=feature.entity_ids
            )

        transformer = OfflineStoreIngestQueryGraphTransformer(graph=feature.graph)
        assert feature.name is not None
        result = transformer.transform(
            target_node=feature.node,
            relationships_info=feature.relationships_info or [],
            feature_name=feature.name,
            feature_version=feature.version.to_str(),
        )

        if result.is_decomposed:
            decomposed_graph, output_node_name = await self.reconstruct_decomposed_graph(
                graph=result.graph,
                node_name=result.node_name_map[feature.node.name],
                catalog_id=feature.catalog_id,
                entity_id_to_serving_name=entity_id_to_serving_name,
            )
            metadata = None
        else:
            decomposed_graph = feature.graph
            output_node_name = feature.node.name
            feature_job_setting = FeatureJobSettingExtractor(
                graph=feature.graph
            ).extract_from_target_node(node=feature.node)

            has_ttl = bool(
                next(
                    feature.graph.iterate_nodes(
                        target_node=feature.node, node_type=NodeType.GROUPBY
                    ),
                    None,
                )
            )
            table_name = await self.offline_store_feature_table_creator(
                primary_entity_ids=feature.primary_entity_ids,
                feature_job_setting=feature_job_setting,
                has_ttl=has_ttl,
                catalog_id=feature.catalog_id,
                entity_id_to_serving_name=entity_id_to_serving_name,
            )
            entity_id_to_dtype = dict(zip(feature.entity_ids, feature.entity_dtypes))
            metadata = OfflineStoreInfoMetadata(
                aggregation_nodes_info=feature.extract_aggregation_nodes_info(),
                feature_job_setting=feature_job_setting,
                has_ttl=has_ttl,
                offline_store_table_name=table_name,
                output_column_name=feature.versioned_name,
                output_dtype=feature.dtype,
                primary_entity_ids=feature.primary_entity_ids,
                primary_entity_dtypes=[
                    entity_id_to_dtype[entity_id] for entity_id in feature.primary_entity_ids
                ],
            )

        # populate offline store info
        offline_store_info = OfflineStoreInfo(
            graph=decomposed_graph,
            node_name=output_node_name,
            node_name_map=result.node_name_map,
            is_decomposed=result.is_decomposed,
            metadata=metadata,
            serving_names_info=[
                ServingNameInfo(serving_name=serving_name, entity_id=entity_id)
                for entity_id, serving_name in entity_id_to_serving_name.items()
            ],
        )
        offline_store_info.initialize(
            feature_versioned_name=feature.versioned_name,
            feature_dtype=feature.dtype,
            feature_job_settings=[
                setting.feature_job_setting for setting in feature.table_id_feature_job_settings
            ],
            feature_id=feature.id,
        )
        return offline_store_info
