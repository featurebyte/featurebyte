"""
OfflineStoreFeatureTableModel class
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pymongo
from bson import ObjectId
from pydantic import Field, model_validator

from featurebyte.common.string import sanitize_identifier
from featurebyte.enum import DBVarType
from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueValuesConstraint,
)
from featurebyte.models.entity import EntityModel
from featurebyte.models.entity_universe import EntityUniverseModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureCluster
from featurebyte.models.offline_store_ingest_query import OfflineStoreIngestQueryGraph
from featurebyte.models.parent_serving import FeatureNodeRelationshipsInfo
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.query_graph.model.feature_job_setting import (
    FeatureJobSettingUnion,
)
from featurebyte.query_graph.node.generic import GroupByNodeParameters
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema


class OnlineStoreLastMaterializedAt(FeatureByteBaseModel):
    """
    Last materialized timestamp for an online store
    """

    online_store_id: PydanticObjectId
    value: datetime


class PrecomputedLookupMapping(FeatureByteBaseModel):
    """
    Track entities before and after lookup
    """

    lookup_feature_table_serving_name: str
    source_feature_table_serving_name: str


class PrecomputedLookupFeatureTableInfo(FeatureByteBaseModel):
    """
    Metadata for a feature table that is derived from a source feature table in order to support
    precomputed lookup using a related entity
    """

    lookup_steps: List[EntityRelationshipInfo]
    source_feature_table_id: Optional[PydanticObjectId] = Field(default=None)
    lookup_mapping: Optional[List[PrecomputedLookupMapping]] = Field(default=None)

    def get_lookup_feature_table_serving_name(
        self, source_feature_table_serving_name: str
    ) -> Optional[str]:
        """
        Returns the corresponding serving name in the precomputed lookup feature table given a
        serving name in the source feature table, if available.

        Parameters
        ----------
        source_feature_table_serving_name: str
            Source feature table serving name

        Returns
        -------
        Optional[str]
        """
        mapping = {
            entry.source_feature_table_serving_name: entry.lookup_feature_table_serving_name
            for entry in (self.lookup_mapping or {})
        }
        return mapping.get(source_feature_table_serving_name)


class OfflineStoreFeatureTableModel(FeatureByteCatalogBaseDocumentModel):
    """
    OfflineStoreFeatureTable class
    """

    name: str
    base_name: Optional[str] = Field(default=None)
    name_prefix: Optional[str] = Field(default=None)
    name_suffix: Optional[str] = Field(default=None)
    feature_ids: List[PydanticObjectId]
    primary_entity_ids: List[PydanticObjectId]
    serving_names: List[str]
    feature_job_setting: Optional[FeatureJobSettingUnion] = Field(default=None)
    has_ttl: bool
    last_materialized_at: Optional[datetime] = Field(default=None)
    online_stores_last_materialized_at: List[OnlineStoreLastMaterializedAt] = Field(
        default_factory=list
    )

    feature_cluster_path: Optional[str] = Field(default=None)
    feature_cluster: Optional[FeatureCluster] = Field(default=None)

    output_column_names: List[str]
    output_dtypes: List[DBVarType]
    internal_entity_universe: Optional[Dict[str, Any]] = Field(
        alias="entity_universe", default=None
    )
    entity_lookup_info: Optional[EntityRelationshipInfo] = Field(default=None)  # Note: deprecated
    precomputed_lookup_feature_table_info: Optional[PrecomputedLookupFeatureTableInfo] = Field(
        default=None
    )
    feature_store_id: Optional[PydanticObjectId] = Field(default=None)
    deployment_ids: List[PydanticObjectId] = Field(default_factory=list)
    aggregation_ids: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _set_feature_store_id(self) -> "OfflineStoreFeatureTableModel":
        """
        Set feature_store_id

        Returns
        -------
        OfflineStoreFeatureTableModel
        """
        if not self.feature_store_id and self.feature_cluster:
            self.__dict__["feature_store_id"] = self.feature_cluster.feature_store_id
        return self

    @classmethod
    def _get_remote_attribute_paths(cls, document_dict: Dict[str, Any]) -> List[Path]:
        paths = []
        feature_cluster_path = document_dict.get("feature_cluster_path")
        if feature_cluster_path:
            paths.append(Path(feature_cluster_path))
        return paths

    @property
    def table_signature(self) -> Dict[str, Any]:
        """
        Get table signature

        Returns
        -------
        Dict[str, Any]
        """
        entity_lookup_info = None
        if self.entity_lookup_info:
            entity_lookup_info = self.entity_lookup_info.model_dump(by_alias=True)
        precomputed_lookup_feature_table_info = None
        if self.precomputed_lookup_feature_table_info:
            precomputed_lookup_feature_table_info = (
                self.precomputed_lookup_feature_table_info.model_dump(by_alias=True)
            )
        return {
            "catalog_id": self.catalog_id,
            "primary_entity_ids": self.primary_entity_ids,
            "serving_names": self.serving_names,
            "feature_job_setting": (
                self.feature_job_setting.model_dump() if self.feature_job_setting else None
            ),
            "has_ttl": self.has_ttl,
            "entity_lookup_info": entity_lookup_info,
            "precomputed_lookup_feature_table_info": precomputed_lookup_feature_table_info,
        }

    @property
    def entity_universe(self) -> EntityUniverseModel:
        """
        Get entity universe

        Returns
        -------
        EntityUniverseModel

        Raises
        ------
        ValueError
            If entity_universe is not set
        """
        if self.internal_entity_universe is None:
            raise ValueError("entity_universe is not set")
        return EntityUniverseModel(**self.internal_entity_universe)

    @property
    def warehouse_tables(self) -> list[TableDetails]:
        return [TableDetails(table_name=self.name)]

    @staticmethod
    def get_serving_names_for_table_name(
        serving_names: List[str],
        max_serv_name_len: int = 19,
        max_serv_num: int = 2,
    ) -> str:
        """
        Get the serving names formatted to be used as part of the feature table name

        Parameters
        ----------
        serving_names: List[str]
            List of serving names
        max_serv_name_len: int
            Maximum length of each serving name when formatting
        max_serv_num: int
            Maximum number of serving names to include when formatting

        Returns
        -------
        str
        """
        # take first 3 serving names and join them with underscore
        # if serving name is longer than 16 characters, truncate it
        # max length of the name = 15 * 3 + 2 = 47
        # strip leading and trailing underscores for all serving names & sanitized name
        name = sanitize_identifier(
            "_".join(
                serving_name[:max_serv_name_len].strip("_")
                for serving_name in serving_names[:max_serv_num]
            )
        ).strip("_")
        return name

    def get_basename(self) -> str:
        """
        Get the base name of the feature table (exclude prefix and suffix)

        Returns
        -------
        str
        """
        # max length of feature table name is 64
        # reserving 8 characters for prefix (catalog name, `<project_name>_`, which is 7 hex digits)
        # need to the same prefix for project name (same as catalog prefix)
        # 3 characters for suffix (count, `_<num>`, num is 1-99)
        max_len = 45  # 64 - 2 * 8 - 3
        max_serv_name_len = 19
        max_serv_num = 2
        max_freq_len = (  # 45 - 39 - 1 = 5
            max_len
            - max_serv_name_len * max_serv_num  # serving names
            - (max_serv_num - 1)  # underscores
            - 1  # frequency separator
        )
        name = "_no_entity"
        if self.serving_names:
            name = self.get_serving_names_for_table_name(
                self.serving_names,
                max_serv_name_len=max_serv_name_len,
                max_serv_num=max_serv_num,
            )

        if self.feature_job_setting:
            # take the frequency part of the feature job setting
            freq_part = self.feature_job_setting.extract_offline_store_feature_table_name_postfix(
                max_length=max_freq_len
            )
            keep = max_len - len(freq_part) - 1
            name = f"{name[:keep]}_{freq_part}"
        return name

    def get_name(self) -> str:
        """
        Get full name of the feature table

        Returns
        -------
        str
        """
        full_name = ""
        if self.name_prefix:
            full_name += self.name_prefix + "_"

        full_name += self.get_basename()

        if self.name_suffix:
            full_name += "_" + self.name_suffix
        return full_name

    def get_online_store_last_materialized_at(
        self, online_store_id: ObjectId
    ) -> Optional[datetime]:
        """
        Get the last materialized at timestamp for an online store. Returns None if this offline
        store table has never been materialized to the online store.

        Parameters
        ----------
        online_store_id: ObjectId
            Online store id

        Returns
        -------
        Optional[datetime]
        """
        if self.online_stores_last_materialized_at is not None:
            for entry in self.online_stores_last_materialized_at:
                if entry.online_store_id == online_store_id:
                    return entry.value
        return None

    def get_output_dtypes_for_columns(self, column_names: List[str]) -> List[DBVarType]:
        """
        Get a list DBVarType corresponding to the provided column names

        Parameters
        ----------
        column_names: List[str]
            Column names

        Returns
        -------
        List[DBVarType]
        """
        mapping = dict(zip(self.output_column_names, self.output_dtypes))
        return [mapping[col] for col in column_names]

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
            pymongo.operations.IndexModel(
                "precomputed_lookup_feature_table_info.source_feature_table_id"
            ),
            pymongo.operations.IndexModel("deployment_ids"),
            pymongo.operations.IndexModel("aggregation_ids"),
        ]
        auditable = False


class FeaturesUpdate(BaseDocumentServiceUpdateSchema):
    """
    FeaturesUpdate class to be used when updating features related fields
    """

    feature_ids: List[PydanticObjectId]
    feature_cluster: Optional[FeatureCluster] = Field(default=None)
    feature_cluster_path: Optional[str] = Field(default=None)
    output_column_names: List[str]
    output_dtypes: List[DBVarType]
    entity_universe: EntityUniverseModel
    aggregation_ids: List[str]


class OfflineLastMaterializedAtUpdate(BaseDocumentServiceUpdateSchema):
    """
    Schema to be used when updating last_materialized_at field
    """

    last_materialized_at: datetime


class OnlineStoresLastMaterializedAtUpdate(BaseDocumentServiceUpdateSchema):
    """
    Schema to be used when updating online_stores_last_materialized_at field
    """

    online_stores_last_materialized_at: List[OnlineStoreLastMaterializedAt]


OfflineStoreFeatureTableUpdate = Union[
    FeaturesUpdate,
    OfflineLastMaterializedAtUpdate,
    OnlineStoresLastMaterializedAtUpdate,
]


@dataclass
class OfflineIngestGraphMetadata:
    """
    Information about offline ingest graph combined for all features
    """

    feature_cluster: FeatureCluster
    output_column_names: List[str]
    output_dtypes: List[DBVarType]
    aggregation_ids: List[str]
    offline_ingest_graphs: List[Tuple[OfflineStoreIngestQueryGraph, List[EntityRelationshipInfo]]]


def get_combined_ingest_graph(
    features: List[FeatureModel],
    primary_entities: List[EntityModel],
    has_ttl: bool,
    feature_job_setting: Optional[FeatureJobSettingUnion],
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
    feature_job_setting : Optional[FeatureJobSettingUnion]
        Feature job setting

    Returns
    -------
    OfflineIngestGraphMetadata
    """
    local_query_graph = QueryGraph()
    output_nodes = []
    output_column_names = []
    output_dtypes = []
    aggregation_ids = set()
    all_offline_ingest_graphs = []

    primary_entity_ids = sorted([entity.id for entity in primary_entities])
    feature_node_relationships_info = []
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
            mapped_node = local_query_graph.get_node_by_name(local_name_map[node.name])
            output_nodes.append(mapped_node)
            feature_node_relationships_info.append(
                FeatureNodeRelationshipsInfo(
                    node_name=mapped_node.name,
                    relationships_info=feature.relationships_info or [],
                    primary_entity_ids=offline_ingest_graph.primary_entity_ids,
                )
            )
            output_column_names.append(offline_ingest_graph.output_column_name)
            output_dtypes.append(offline_ingest_graph.output_dtype)
            aggregation_ids.update(_extract_aggregation_ids(offline_ingest_graph))
            all_offline_ingest_graphs.append((
                offline_ingest_graph,
                feature.relationships_info or [],
            ))

    feature_cluster = FeatureCluster(
        feature_store_id=features[0].tabular_source.feature_store_id,
        graph=local_query_graph,
        node_names=[node.name for node in output_nodes],
        feature_node_relationships_infos=feature_node_relationships_info,
    )

    return OfflineIngestGraphMetadata(
        feature_cluster=feature_cluster,
        output_column_names=output_column_names,
        output_dtypes=output_dtypes,
        aggregation_ids=list(aggregation_ids),
        offline_ingest_graphs=all_offline_ingest_graphs,
    )


def _extract_aggregation_ids(offline_ingest_graph: OfflineStoreIngestQueryGraph) -> list[str]:
    """
    Extract all aggregation_id from groupby nodes in OfflineStoreIngestQueryGraph

    Parameters
    ----------
    offline_ingest_graph: OfflineStoreIngestQueryGraph
        Offline store ingest query graph

    Returns
    -------
    list[str]
    """
    aggregation_ids = []
    for info in offline_ingest_graph.aggregation_nodes_info:
        node = offline_ingest_graph.graph.get_node_by_name(info.node_name)
        if (
            isinstance(node.parameters, GroupByNodeParameters)
            and node.parameters.aggregation_id is not None
        ):
            aggregation_ids.append(node.parameters.aggregation_id)
    return aggregation_ids
