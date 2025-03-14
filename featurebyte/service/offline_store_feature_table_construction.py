"""
OfflineStoreFeatureTableConstructionService class
"""

from __future__ import annotations

import os
import pickle
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import aiofiles
from bson import ObjectId

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.entity import EntityModel
from featurebyte.models.entity_universe import (
    EntityUniverseModel,
    EntityUniverseParams,
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
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.query_graph.model.feature_job_setting import (
    FeatureJobSetting,
    FeatureJobSettingUnion,
)
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import LookupNode
from featurebyte.query_graph.node.mixin import BaseGroupbyParameters
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.service.entity import EntityService
from featurebyte.service.entity_serving_names import EntityServingNamesService
from featurebyte.service.exception import OfflineStoreFeatureTableBadStateError
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService
from featurebyte.service.parent_serving import ParentEntityLookupService
from featurebyte.storage import Storage


class OfflineStoreFeatureTableConstructionService:
    """
    OfflineStoreFeatureTableConstructionService class
    """

    def __init__(
        self,
        entity_service: EntityService,
        parent_entity_lookup_service: ParentEntityLookupService,
        entity_serving_names_service: EntityServingNamesService,
        offline_store_feature_table_service: OfflineStoreFeatureTableService,
        storage: Storage,
    ):
        self.entity_service = entity_service
        self.parent_entity_lookup_service = parent_entity_lookup_service
        self.entity_serving_names_service = entity_serving_names_service
        self.offline_store_feature_table_service = offline_store_feature_table_service
        self.storage = storage

    async def get_dummy_offline_store_feature_table_model(
        self,
        primary_entity_ids: Sequence[ObjectId],
        feature_job_setting: Optional[FeatureJobSettingUnion],
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
        feature_job_setting: Optional[FeatureJobSettingUnion]
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

        if isinstance(feature_job_setting, FeatureJobSetting):
            feature_job_setting = feature_job_setting.normalize()

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
            feature_job_setting=feature_job_setting,
            feature_store_id=feature_store_id,
            catalog_id=catalog_id,
        )

    async def get_offline_store_feature_table_model(
        self,
        feature_table_name: str,
        features: List[FeatureModel],
        primary_entities: List[EntityModel],
        has_ttl: bool,
        feature_job_setting: Optional[FeatureJobSettingUnion],
        source_info: SourceInfo,
    ) -> OfflineStoreFeatureTableModel:
        """
        Returns a OfflineStoreFeatureTableModel for a feature table

        Parameters
        ----------
        feature_table_name : str
            Feature table name
        features : List[FeatureModel]
            List of features
        primary_entities : List[EntityModel]
            List of primary entities
        has_ttl : bool
            Whether the feature table has TTL
        feature_job_setting : Optional[FeatureJobSettingUnion]
            Feature job setting of the feature table
        source_info: SourceInfo
            Source information

        Returns
        -------
        OfflineStoreFeatureTableModel

        Raises
        ------
        OfflineStoreFeatureTableBadStateError
            If the entity universe cannot be determined
        """
        ingest_graph_metadata = get_combined_ingest_graph(
            features=features,
            primary_entities=primary_entities,
            has_ttl=has_ttl,
            feature_job_setting=feature_job_setting,
        )

        try:
            entity_universe = await self.get_entity_universe_model(
                offline_ingest_graphs=ingest_graph_metadata.offline_ingest_graphs,
                source_info=source_info,
                feature_table_name=feature_table_name,
            )
        except OfflineStoreFeatureTableBadStateError:
            # Temporarily save the offline ingest graphs and other information to a file for
            # troubleshooting
            debug_info = {
                "feature_ids": [feature.id for feature in features],
                "primary_entities": primary_entities,
                "has_ttl": has_ttl,
                "feature_job_setting": feature_job_setting,
                "feature_table_name": feature_table_name,
                "offline_ingest_graphs": ingest_graph_metadata.offline_ingest_graphs,
            }
            path = self.offline_store_feature_table_service.get_full_remote_file_path(
                f"offline_store_feature_table/{feature_table_name}/entity_universe_debug_info.pickle"
            )
            try:
                await self.storage.delete(Path(path))
            except FileNotFoundError:
                pass
            async with aiofiles.tempfile.TemporaryDirectory() as tempdir_path:
                file_path = os.path.join(tempdir_path, "data.pickle")
                with open(file_path, "wb") as file_obj:
                    pickle.dump(debug_info, file_obj, protocol=pickle.HIGHEST_PROTOCOL)
                await self.storage.put(Path(file_path), Path(path))
            raise

        if isinstance(feature_job_setting, FeatureJobSetting):
            feature_job_setting = feature_job_setting.normalize()

        return OfflineStoreFeatureTableModel(
            name=feature_table_name,
            feature_ids=[feature.id for feature in features],
            primary_entity_ids=[entity.id for entity in primary_entities],
            # FIXME: consolidate all the offline store serving names (get_entity_id_to_serving_name_for_offline_store)
            serving_names=[entity.serving_names[0] for entity in primary_entities],
            feature_cluster=ingest_graph_metadata.feature_cluster,
            output_column_names=ingest_graph_metadata.output_column_names,
            output_dtypes=ingest_graph_metadata.output_dtypes,
            entity_universe=entity_universe.model_dump(by_alias=True),
            has_ttl=has_ttl,
            feature_job_setting=feature_job_setting,
            aggregation_ids=ingest_graph_metadata.aggregation_ids,
        )

    async def get_entity_universe_model(
        self,
        offline_ingest_graphs: List[
            Tuple[OfflineStoreIngestQueryGraph, List[EntityRelationshipInfo]]
        ],
        source_info: SourceInfo,
        feature_table_name: str,
    ) -> EntityUniverseModel:
        """
        Create a new EntityUniverseModel object

        Parameters
        ----------
        offline_ingest_graphs: List[OfflineStoreIngestQueryGraph]
            The offline ingest graphs that the entity universe is to be constructed from
        source_info: SourceInfo
            Source information
        feature_table_name: str
            Name of the offline store feature table which the entity universe is for

        Returns
        -------
        EntityUniverse

        Raises
        ------
        OfflineStoreFeatureTableBadStateError
            If the entity universe cannot be determined
        """
        params = []
        for offline_ingest_graph, relationships_info in offline_ingest_graphs:
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
                    join_steps = await self.parent_entity_lookup_service.get_required_join_steps(
                        entity_info, relationships_info
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
        universe_expr = get_combined_universe(params, source_info)

        if universe_expr is None:
            raise OfflineStoreFeatureTableBadStateError(
                f"Failed to create entity universe for offline store feature table {feature_table_name}"
            )

        return EntityUniverseModel(
            query_template=SqlglotExpressionModel.create(universe_expr, source_info.source_type)
        )

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
