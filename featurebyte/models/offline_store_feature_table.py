"""
OfflineStoreFeatureTableModel class
"""
from typing import List, Literal, Optional, Union

from dataclasses import dataclass
from datetime import datetime

import pymongo
from pydantic import Field
from sqlglot import expressions
from sqlglot.expressions import Select, select

from featurebyte.enum import DBVarType, StrEnum
from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueValuesConstraint,
)
from featurebyte.models.entity import EntityModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureCluster
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema


class EntityUniverseType(StrEnum):
    """
    EntityUniverseType enum
    """

    WINDOW_AGGREGATE = "window_aggregate"


class WindowAggregateEntityUniverse(FeatureByteBaseModel):
    """
    WindowAggregateEntityUniverse class
    """

    serving_names: List[str]
    aggregate_result_table_names: List[str] = Field(min_length=1)
    type: Literal[EntityUniverseType.WINDOW_AGGREGATE] = Field(
        EntityUniverseType.WINDOW_AGGREGATE, const=True
    )

    def get_entity_universe_expr(self) -> Select:
        """
        Get the SQL expression for the entity universe

        Returns
        -------
        Select
        """
        if not self.serving_names:
            return expressions.select(
                expressions.alias_(make_literal_value(1), "dummy_entity", quoted=True)
            )

        assert len(self.aggregate_result_table_names) > 0

        # select distinct serving names across all aggregation result tables
        distinct_serving_names_from_tables = [
            select(*[quoted_identifier(serving_name) for serving_name in self.serving_names])
            .distinct()
            .from_(table_name)
            for table_name in self.aggregate_result_table_names
        ]

        union_expr = distinct_serving_names_from_tables[0]
        for expr in distinct_serving_names_from_tables[1:]:
            union_expr = expressions.Union(this=expr, distinct=True, expression=union_expr)  # type: ignore

        return union_expr


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
    entity_universe: WindowAggregateEntityUniverse

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
        ]


class FeaturesUpdate(BaseDocumentServiceUpdateSchema):
    """
    FeaturesUpdate class to be used when updating features related fields
    """

    feature_ids: List[PydanticObjectId]
    feature_cluster: FeatureCluster
    output_column_names: List[str]
    output_dtypes: List[DBVarType]
    entity_universe: WindowAggregateEntityUniverse


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

    feature_cluster = FeatureCluster(
        feature_store_id=features[0].tabular_source.feature_store_id,
        graph=local_query_graph,
        node_names=[node.name for node in output_nodes],
    )

    return OfflineIngestGraphMetadata(
        feature_cluster=feature_cluster,
        output_column_names=output_column_names,
        output_dtypes=output_dtypes,
    )


def get_offline_store_feature_table_model(
    feature_table_name: str,
    features: List[FeatureModel],
    aggregate_result_table_names: List[str],
    primary_entities: List[EntityModel],
    has_ttl: bool,
    feature_job_setting: Optional[FeatureJobSetting],
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

    # TODO: handle feature types that are currently not being precomputed, e.g. lookup
    #  features, asat aggregates, etc.
    assert len(aggregate_result_table_names) > 0
    entity_universe = WindowAggregateEntityUniverse(
        serving_names=[entity.serving_names[0] for entity in primary_entities],
        aggregate_result_table_names=aggregate_result_table_names,
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
        feature_job_setting=feature_job_setting,
    )
