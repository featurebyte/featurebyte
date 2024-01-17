"""
OfflineStoreFeatureTableModel class
"""
from __future__ import annotations

from typing import List, Optional, Union

from dataclasses import dataclass
from datetime import datetime

import pymongo
from pydantic import Field

from featurebyte.enum import DBVarType
from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueValuesConstraint,
)
from featurebyte.models.entity import EntityModel
from featurebyte.models.entity_universe import EntityUniverseModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureCluster
from featurebyte.models.offline_store_ingest_query import OfflineStoreIngestQueryGraph
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema


class OfflineStoreFeatureTableModel(FeatureByteCatalogBaseDocumentModel):
    """
    OfflineStoreFeatureTable class
    """

    name: str
    feature_ids: List[PydanticObjectId]
    primary_entity_ids: List[PydanticObjectId]
    serving_names: List[str]
    feature_job_setting: Optional[FeatureJobSetting]
    has_ttl: bool
    last_materialized_at: Optional[datetime]

    feature_cluster: FeatureCluster
    output_column_names: List[str]
    output_dtypes: List[DBVarType]
    entity_universe: EntityUniverseModel
    entity_lookup_info: Optional[EntityRelationshipInfo]

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "offline_store_feature_table"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
            ),
        ]

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("feature_ids"),
            pymongo.operations.IndexModel("primary_entity_ids"),
            pymongo.operations.IndexModel("serving_names"),
            pymongo.operations.IndexModel("feature_job_setting"),
            pymongo.operations.IndexModel("has_ttl"),
            pymongo.operations.IndexModel("entity_lookup_info"),
        ]


class FeaturesUpdate(BaseDocumentServiceUpdateSchema):
    """
    FeaturesUpdate class to be used when updating features related fields
    """

    feature_ids: List[PydanticObjectId]
    feature_cluster: FeatureCluster
    output_column_names: List[str]
    output_dtypes: List[DBVarType]
    entity_universe: EntityUniverseModel


class LastMaterializedAtUpdate(BaseDocumentServiceUpdateSchema):
    """
    LastMaterializedAtUpdate class to be used when updating last_materialized_at field
    """

    last_materialized_at: Optional[datetime]


OfflineStoreFeatureTableUpdate = Union[FeaturesUpdate, LastMaterializedAtUpdate]


@dataclass
class OfflineIngestGraphMetadata:
    """
    Information about offline ingest graph combined for all features
    """

    feature_cluster: FeatureCluster
    output_column_names: List[str]
    output_dtypes: List[DBVarType]
    offline_ingest_graphs: List[OfflineStoreIngestQueryGraph]


def get_combined_ingest_graph(
    features: List[FeatureModel],
    primary_entities: List[EntityModel],
    has_ttl: bool,
    feature_job_setting: Optional[FeatureJobSetting],
) -> OfflineIngestGraphMetadata:
    """
    Returns a combined ingest graph and related information for all features belonging to a
    feature table

    Parameters
    ----------
    features : List[FeatureModel]
        List of features
    primary_entities : List[EntityModel]
        List of primary entity models
    has_ttl : bool
        Whether the feature table has TTL
    feature_job_setting : Optional[FeatureJobSetting]
        Feature job setting

    Returns
    -------
    OfflineIngestGraphMetadata
    """
    local_query_graph = QueryGraph()
    output_nodes = []
    output_column_names = []
    output_dtypes = []
    all_offline_ingest_graphs = []

    primary_entity_ids = sorted([entity.id for entity in primary_entities])
    for feature in features:
        offline_ingest_graphs = (
            feature.offline_store_info.extract_offline_store_ingest_query_graphs()
        )
        for offline_ingest_graph in offline_ingest_graphs:
            if (
                offline_ingest_graph.primary_entity_ids != primary_entity_ids
                or offline_ingest_graph.has_ttl != has_ttl
                or offline_ingest_graph.feature_job_setting != feature_job_setting
            ):
                # Feature of a primary entity can be decomposed into ingest graphs that should be
                # published to different feature tables. Skip if it is not the same as the current
                # feature table.
                continue

            graph, node = offline_ingest_graph.ingest_graph_and_node()
            local_query_graph, local_name_map = local_query_graph.load(graph)
            output_nodes.append(local_query_graph.get_node_by_name(local_name_map[node.name]))
            output_column_names.append(offline_ingest_graph.output_column_name)
            output_dtypes.append(offline_ingest_graph.output_dtype)
            all_offline_ingest_graphs.append(offline_ingest_graph)

    feature_cluster = FeatureCluster(
        feature_store_id=features[0].tabular_source.feature_store_id,
        graph=local_query_graph,
        node_names=[node.name for node in output_nodes],
    )

    return OfflineIngestGraphMetadata(
        feature_cluster=feature_cluster,
        output_column_names=output_column_names,
        output_dtypes=output_dtypes,
        offline_ingest_graphs=all_offline_ingest_graphs,
    )
