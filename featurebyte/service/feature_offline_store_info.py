"""
Feature Offline Store Info Initialization Service
"""

from typing import Dict, List, Optional, Sequence, Tuple

from bson import ObjectId

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature import FeatureModel
from featurebyte.models.offline_store_ingest_query import (
    OfflineStoreInfo,
    OfflineStoreInfoMetadata,
    ServingNameInfo,
)
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
from featurebyte.query_graph.model.entity_lookup_plan import EntityLookupPlanner
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.query_graph.model.feature_job_setting import (
    FeatureJobSettingUnion,
)
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node.base import BaseNode
from featurebyte.query_graph.node.nested import OfflineStoreIngestQueryGraphNodeParameters
from featurebyte.query_graph.transform.decompose_point import FeatureJobSettingExtractor
from featurebyte.query_graph.transform.null_filling_value import NullFillingValueExtractor
from featurebyte.query_graph.transform.offline_store_ingest import (
    OfflineStoreIngestQueryGraphTransformer,
)
from featurebyte.service.catalog import CatalogService
from featurebyte.service.entity_serving_names import EntityServingNamesService
from featurebyte.service.feature_store import FeatureStoreService
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
        feature_store_service: FeatureStoreService,
    ) -> None:
        self.catalog_service = catalog_service
        self.offline_store_feature_table_service = offline_store_feature_table_service
        self.offline_store_feature_table_construction_service = (
            offline_store_feature_table_construction_service
        )
        self.entity_serving_names_service = entity_serving_names_service
        self.feature_store_service = feature_store_service

    async def offline_store_feature_table_creator(
        self,
        primary_entity_ids: Sequence[ObjectId],
        feature_job_setting: Optional[FeatureJobSettingUnion],
        has_ttl: bool,
        catalog_id: ObjectId,
        table_name_prefix: str,
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
        table_name_prefix: str
            Registry project name
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
            table_name_prefix=table_name_prefix,
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
        table_name_prefix: str,
        entity_id_to_serving_name: Optional[Dict[ObjectId, str]] = None,
        dry_run: bool = False,
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
        table_name_prefix: str
            Registry project name
        entity_id_to_serving_name: Optional[Dict[ObjectId, str]]
            Entity id to serving name mapping
        dry_run: bool
            If True, don't create the offline feature tables but return OfflineStoreInfo consisting
            of dummy feature table names.

        Returns
        -------
        Tuple[QueryGraphModel, str]
        """
        query_graph = QueryGraph(**graph.model_dump(by_alias=True))

        node_name_to_repl_node: Dict[str, BaseNode] = {}
        for node in query_graph.iterate_sorted_graph_nodes(
            graph_node_types={GraphNodeType.OFFLINE_STORE_INGEST_QUERY}
        ):
            assert isinstance(node.parameters, OfflineStoreIngestQueryGraphNodeParameters)
            if not dry_run:
                node.parameters.offline_store_table_name = (
                    await self.offline_store_feature_table_creator(
                        primary_entity_ids=node.parameters.primary_entity_ids,
                        feature_job_setting=node.parameters.feature_job_setting,
                        has_ttl=node.parameters.has_ttl,
                        catalog_id=catalog_id,
                        table_name_prefix=table_name_prefix,
                        entity_id_to_serving_name=entity_id_to_serving_name,
                    )
                )
                node_params = node.parameters
                extractor = NullFillingValueExtractor(graph=node_params.graph)
                state = extractor.extract(
                    node=node_params.graph.get_node_by_name(node_params.output_node_name)
                )
                node.parameters.null_filling_value = state.fill_value

            node_name_to_repl_node[node.name] = node

        new_graph, node_name_map = query_graph.reconstruct(
            node_name_to_replacement_node=node_name_to_repl_node,
            regenerate_groupby_hash=False,
        )
        return new_graph, node_name_map[node_name]

    async def get_offline_store_feature_tables_entity_ids(
        self, feature: FeatureModel, entity_id_to_serving_name: Dict[ObjectId, str]
    ) -> List[List[PydanticObjectId]]:
        """
        Get the primary entity of the offline feature tables for the feature. This doesn't require
        offline_store_info to be initialized beforehand.

        Parameters
        ----------
        feature: FeatureModel
            Feature
        entity_id_to_serving_name: Optional[Dict[ObjectId, str]]
            Entity id to serving name mapping

        Returns
        -------
        List[List[PydanticObjectId]]
        """
        if feature.internal_offline_store_info is not None:
            offline_store_info = feature.offline_store_info
        else:
            offline_store_info = await self.initialize_offline_store_info(
                feature=feature,
                table_name_prefix="",  # this value is not used in dry_run mode
                entity_id_to_serving_name=entity_id_to_serving_name,
                dry_run=True,
            )
        feature_table_entity_ids = []
        for ingest_graph in offline_store_info.extract_offline_store_ingest_query_graphs():
            feature_table_entity_ids.append(ingest_graph.primary_entity_ids)
        return feature_table_entity_ids

    async def initialize_offline_store_info(
        self,
        feature: FeatureModel,
        table_name_prefix: str,
        entity_id_to_serving_name: Optional[Dict[ObjectId, str]] = None,
        dry_run: bool = False,
    ) -> OfflineStoreInfo:
        """
        Initialize feature offline store info

        Parameters
        ----------
        feature: FeatureModel
            Feature
        table_name_prefix: str
            Registry project name
        entity_id_to_serving_name: Optional[Dict[ObjectId, str]]
            Entity id to serving name mapping
        dry_run: bool
            If True, don't create the offline feature tables but return OfflineStoreInfo consisting
            of dummy feature table names.

        Returns
        -------
        OfflineStoreInfo
        """
        if entity_id_to_serving_name is None:
            serv_name_service = self.entity_serving_names_service
            entity_id_to_serving_name = (
                await serv_name_service.get_entity_id_to_serving_name_for_offline_store(
                    entity_ids=feature.entity_ids
                )
            )

        transformer = OfflineStoreIngestQueryGraphTransformer(graph=feature.graph)
        assert feature.name is not None
        result = transformer.transform(
            target_node=feature.node,
            relationships_info=feature.relationships_info or [],
            feature_name=feature.name,
            feature_version=feature.version.to_str(),
        )

        null_filling_value = None
        source_type = None
        if not dry_run:
            # retrieve the source type and extract feature job settings to initialize offline store info
            catalog = await self.catalog_service.get_document(feature.catalog_id)
            feature_store = await self.feature_store_service.get_document(
                catalog.default_feature_store_ids[0]
            )
            source_type = feature_store.get_source_info().source_type
            null_filling_value = (
                NullFillingValueExtractor(graph=feature.graph)
                .extract(node=feature.node, source_type=source_type)
                .fill_value
            )

        has_ttl = feature.has_bounded_window_aggregated_node
        if result.is_decomposed:
            decomposed_graph, output_node_name = await self.reconstruct_decomposed_graph(
                graph=result.graph,
                node_name=result.node_name_map[feature.node.name],
                catalog_id=feature.catalog_id,
                table_name_prefix=table_name_prefix,
                entity_id_to_serving_name=entity_id_to_serving_name,
                dry_run=dry_run,
            )
            metadata = None
        else:
            decomposed_graph = feature.graph
            output_node_name = feature.node.name
            feature_job_setting = FeatureJobSettingExtractor(
                graph=feature.graph
            ).extract_from_target_node(node=feature.node)

            table_name = (
                await self.offline_store_feature_table_creator(
                    primary_entity_ids=feature.primary_entity_ids,
                    feature_job_setting=feature_job_setting,
                    has_ttl=has_ttl,
                    table_name_prefix=table_name_prefix,
                    catalog_id=feature.catalog_id,
                    entity_id_to_serving_name=entity_id_to_serving_name,
                )
                if not dry_run
                else ""
            )
            entity_id_to_dtype = dict(zip(feature.entity_ids, feature.entity_dtypes))
            metadata = OfflineStoreInfoMetadata(
                aggregation_nodes_info=feature.extract_aggregation_nodes_info(),
                feature_job_setting=feature_job_setting,
                has_ttl=has_ttl,
                offline_store_table_name=table_name,
                output_column_name=feature.versioned_name,
                output_dtype_info=DBVarTypeInfo(dtype=feature.dtype),
                primary_entity_ids=feature.primary_entity_ids,
                primary_entity_dtypes=[
                    entity_id_to_dtype[entity_id] for entity_id in feature.primary_entity_ids
                ],
                null_filling_value=null_filling_value,
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
        if not dry_run:
            assert source_type is not None, "Source type should be set in previous statement"
            feature_job_settings = [
                setting.feature_job_setting for setting in feature.table_id_feature_job_settings
            ]
            offline_store_info.initialize(
                feature_versioned_name=feature.versioned_name,
                feature_dtype=feature.dtype,
                feature_job_settings=feature_job_settings,
                feature_id=feature.id,
                has_ttl=has_ttl,
                source_type=source_type,
                null_filling_value=null_filling_value,
            )

        return offline_store_info

    async def get_entity_join_steps_for_feature_table(
        self, feature: FeatureModel, entity_id_to_serving_name: Dict[ObjectId, str]
    ) -> List[EntityRelationshipInfo]:
        """
        Get entity join steps for feature table

        Parameters
        ----------
        feature: FeatureModel
            Feature
        entity_id_to_serving_name: Dict[ObjectId, str]
            Entity id to serving name mapping

        Returns
        -------
        List[EntityRelationshipInfo]
        """
        if feature.entity_join_steps is not None:
            return feature.entity_join_steps

        # derive entity join steps for old features which don't have entity_join_steps
        feature_tables_entity_ids = await self.get_offline_store_feature_tables_entity_ids(
            feature, entity_id_to_serving_name
        )

        entity_join_steps = []
        for entity_ids in feature_tables_entity_ids:
            if feature.relationships_info is None:
                continue
            internal_steps = EntityLookupPlanner.generate_lookup_steps(
                available_entity_ids=feature.primary_entity_ids,
                required_entity_ids=entity_ids,
                relationships_info=feature.relationships_info,
            )
            for step in internal_steps:
                if step not in entity_join_steps:
                    entity_join_steps.append(step)

        return entity_join_steps
