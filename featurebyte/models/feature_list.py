"""
This module contains Feature list related models
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

import functools
from collections import defaultdict
from pathlib import Path

import pymongo
from bson.objectid import ObjectId
from pydantic import Field, PrivateAttr, StrictStr, parse_obj_as, root_validator, validator
from typeguard import typechecked

from featurebyte.common.validator import construct_sort_validator, version_validator
from featurebyte.enum import DBVarType, SourceType
from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
    VersionIdentifier,
)
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list_store_info import (
    DataBricksStoreInfo,
    DataBricksUnityStoreInfo,
    SnowflakeStoreInfo,
    SparkStoreInfo,
    StoreInfo,
)
from featurebyte.models.feature_namespace import FeatureReadiness
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.parent_serving import FeatureNodeRelationshipsInfo
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.entity_relationship_info import (
    EntityRelationshipInfo,
    FeatureEntityLookupInfo,
)
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.query_graph.pruning_util import get_combined_graph_and_nodes


class FeatureTypeFeatureCount(FeatureByteBaseModel):
    """
    Feature count corresponding to the feature type within a feature list

    dtype: DBVarType
        Feature table type
    count: int
        Number of features with the specified table type
    """

    dtype: DBVarType
    count: int


class FeatureReadinessCount(FeatureByteBaseModel):
    """
    Feature count corresponding to the feature readiness within a feature list

    readiness: FeatureReadiness
        Feature readiness level
    count: int
        Number of features with the given readiness within a feature list
    """

    readiness: FeatureReadiness
    count: int


class FeatureReadinessTransition(FeatureByteBaseModel):
    """
    Feature readiness transition
    """

    from_readiness: FeatureReadiness
    to_readiness: FeatureReadiness


@functools.total_ordering
class FeatureReadinessDistribution(FeatureByteBaseModel):
    """
    Feature readiness distribution
    """

    __root__: List[FeatureReadinessCount]

    @property
    def total_count(self) -> int:
        """
        Total count of the distribution

        Returns
        -------
        int
        """
        return sum(readiness_count.count for readiness_count in self.__root__)

    @staticmethod
    def _to_count_per_readiness_map(
        feature_readiness_dist: FeatureReadinessDistribution,
    ) -> dict[FeatureReadiness, int]:
        output = {}
        for feature_readiness in FeatureReadiness:
            output[feature_readiness] = 0

        for feature_readiness_count in feature_readiness_dist.__root__:
            output[feature_readiness_count.readiness] += feature_readiness_count.count
        return output

    @classmethod
    def _transform_and_check(
        cls, this_dist: FeatureReadinessDistribution, other_dist: FeatureReadinessDistribution
    ) -> tuple[dict[FeatureReadiness, int], dict[FeatureReadiness, int]]:
        this_dist_map = cls._to_count_per_readiness_map(this_dist)
        other_dist_map = cls._to_count_per_readiness_map(other_dist)
        if sum(this_dist_map.values()) != sum(other_dist_map.values()):
            raise ValueError(
                "Invalid comparison between two feature readiness distributions with different sums."
            )
        return this_dist_map, other_dist_map

    @typechecked
    def __eq__(self, other: FeatureReadinessDistribution) -> bool:  # type: ignore[override]
        this_dist_map, other_dist_map = self._transform_and_check(self, other)
        for feature_readiness in FeatureReadiness:
            if this_dist_map[feature_readiness] != other_dist_map[feature_readiness]:
                return False
        return True

    @typechecked
    def __lt__(self, other: FeatureReadinessDistribution) -> bool:
        # check whether the two readiness distributions comparison is valid
        this_dist_map, other_dist_map = self._transform_and_check(self, other)

        # first check the production readiness fraction first
        this_prod_ready_frac = self.derive_production_ready_fraction()
        other_prod_ready_frac = other.derive_production_ready_fraction()
        if this_prod_ready_frac != other_prod_ready_frac:
            return this_prod_ready_frac < other_prod_ready_frac

        # feature readiness sorted from the worst readiness (deprecated) to the best readiness (production ready)
        # the one with the lower number of readiness should be preferred
        # this mean: dist_with_lower_bad_readiness > dist_with_higher_bad_readiness
        for feature_readiness in FeatureReadiness:
            compare_readiness = (
                this_dist_map[feature_readiness] == other_dist_map[feature_readiness]
            )
            if compare_readiness:
                continue
            return this_dist_map[feature_readiness] > other_dist_map[feature_readiness]
        return False

    def derive_production_ready_fraction(self) -> float:
        """
        Derive fraction of features whose readiness level is at production ready

        Returns
        -------
        Fraction of production ready features
        """
        production_ready_cnt = 0
        for readiness_count in self.__root__:
            if readiness_count.readiness == FeatureReadiness.PRODUCTION_READY:
                production_ready_cnt += readiness_count.count
        return production_ready_cnt / max(self.total_count, 1)

    def update_readiness(
        self, transition: FeatureReadinessTransition
    ) -> FeatureReadinessDistribution:
        """
        Construct a new readiness distribution based on current distribution & readiness transition

        Parameters
        ----------
        transition: FeatureReadinessTransition
            Feature readiness transition

        Returns
        -------
        FeatureReadinessDistribution

        Raises
        ------
        ValueError
            When the readiness transition is invalid
        """
        this_dist_map = self._to_count_per_readiness_map(self)
        if this_dist_map[transition.from_readiness] < 1:
            raise ValueError("Invalid feature readiness transition.")
        this_dist_map[transition.from_readiness] -= 1
        this_dist_map[transition.to_readiness] += 1
        readiness_dist = []
        for feature_readiness in FeatureReadiness:
            count = this_dist_map[feature_readiness]
            if count:
                readiness_dist.append(
                    FeatureReadinessCount(readiness=feature_readiness, count=count)
                )
        return FeatureReadinessDistribution(__root__=readiness_dist)

    def worst_case(self) -> FeatureReadinessDistribution:
        """
        Return the worst possible case for feature readiness distribution

        Returns
        -------
        FeatureReadinessDistribution
        """
        return FeatureReadinessDistribution(
            __root__=[
                FeatureReadinessCount(readiness=min(FeatureReadiness), count=self.total_count)
            ]
        )


class FeatureNodeDefinitionHash(FeatureByteBaseModel):
    """
    Feature definition hash for each node in the FeatureCluster
    """

    node_name: str
    definition_hash: Optional[str]


class FeatureCluster(FeatureByteBaseModel):
    """
    Schema for a group of features from the same feature store
    """

    feature_store_id: PydanticObjectId
    graph: QueryGraph
    node_names: List[StrictStr]
    feature_node_relationships_infos: Optional[List[FeatureNodeRelationshipsInfo]]
    feature_node_definition_hashes: Optional[List[FeatureNodeDefinitionHash]]
    combined_relationships_info: List[EntityRelationshipInfo] = Field(allow_mutation=False)

    @root_validator(pre=True)
    @classmethod
    def _derive_combined_relationships_info(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "combined_relationships_info" in values:
            return values
        combined_relationships_info: Set[EntityRelationshipInfo] = set()
        for info in values.get("feature_node_relationships_infos", []):
            combined_relationships_info.update(info.relationships_info or [])
        values["combined_relationships_info"] = list(combined_relationships_info)
        return values

    @property
    def nodes(self) -> List[Node]:
        """
        Get feature nodes

        Returns
        -------
        List[Node]
        """
        return [self.graph.get_node_by_name(name) for name in self.node_names]

    def get_nodes_for_feature_names(self, feature_names: List[str]) -> List[Node]:
        """
        Get feature nodes for a list of feature names

        Parameters
        ----------
        feature_names: List[str]
            List of feature names

        Returns
        -------
        List[Node]
        """
        selected_nodes = []
        for node in self.nodes:
            feature_name = self.graph.get_node_output_column_name(node.name)
            if feature_name in feature_names:
                selected_nodes.append(node)
        return selected_nodes


ServingEntity = List[PydanticObjectId]


class FeatureListModel(FeatureByteCatalogBaseDocumentModel):
    """
    Model for feature list entity

    id: PydanticObjectId
        FeatureList id of the object
    name: str
        Name of the feature list
    feature_ids: List[PydanticObjectId]
        List of feature IDs
    online_enabled_feature_ids: List[PydanticObjectId]
        List of online enabled feature version id
    readiness_distribution: List[Dict[str, Any]]
        Feature readiness distribution of this feature list
    version: VersionIdentifier
        Feature list version
    relationships_info: Optional[List[EntityRelationshipInfo]]
        List of entity relationship info for the feature list
    supported_serving_entity_ids: List[ServingEntity]
        List of supported serving entity ids, serving entity id is a list of entity ids for serving
    deployed: bool
        Whether to deploy this feature list version
    feature_list_namespace_id: PydanticObjectId
        Feature list namespace id of the object
    created_at: Optional[datetime]
        Datetime when the FeatureList was first saved or published
    internal_feature_clusters: Optional[List[Any]]
        List of combined graphs for features from the same feature store
    internal_store_info: Optional[dict[str, Any]]
        Store specific info for the feature list
    """

    version: VersionIdentifier = Field(allow_mutation=False, description="Feature list version")
    relationships_info: Optional[List[EntityRelationshipInfo]] = Field(
        allow_mutation=False, default=None  # DEV-556
    )
    features_entity_lookup_info: Optional[List[FeatureEntityLookupInfo]] = Field(
        allow_mutation=False, default=None
    )
    supported_serving_entity_ids: List[ServingEntity] = Field(
        allow_mutation=False, default_factory=list
    )
    readiness_distribution: FeatureReadinessDistribution = Field(
        allow_mutation=False, default_factory=list
    )
    dtype_distribution: List[FeatureTypeFeatureCount] = Field(
        allow_mutation=False, default_factory=list
    )
    deployed: bool = Field(allow_mutation=False, default=False)

    # special handling for those attributes that are expensive to deserialize
    # internal_* is used to store the raw data from persistence, _* is used as a cache
    feature_clusters_path: Optional[str] = Field(default=None)
    internal_feature_clusters: Optional[List[Any]] = Field(alias="feature_clusters")
    _feature_clusters: Optional[List[FeatureCluster]] = PrivateAttr(default=None)

    # list of IDs attached to this feature list
    feature_ids: List[PydanticObjectId]
    primary_entity_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)
    entity_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)
    features_primary_entity_ids: List[List[PydanticObjectId]] = Field(
        allow_mutation=False, default_factory=list
    )
    table_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)
    feature_list_namespace_id: PydanticObjectId = Field(
        allow_mutation=False, default_factory=ObjectId
    )
    online_enabled_feature_ids: List[PydanticObjectId] = Field(
        allow_mutation=False, default_factory=list
    )

    # store info contains the warehouse specific info for the feature list
    internal_store_info: Optional[Dict[str, Any]] = Field(alias="store_info", default=None)

    # pydantic validators
    _sort_ids_validator = validator(
        "online_enabled_feature_ids",
        "features_primary_entity_ids",
        "primary_entity_ids",
        "entity_ids",
        "table_ids",
        allow_reuse=True,
    )(construct_sort_validator())
    _version_validator = validator("version", pre=True, allow_reuse=True)(version_validator)

    @validator("supported_serving_entity_ids")
    @classmethod
    def _validate_supported_serving_entity_ids(
        cls, value: List[ServingEntity]
    ) -> List[ServingEntity]:
        return [
            sorted(set(serving_entity))
            for serving_entity in sorted(value, key=lambda e: (len(e), e))
        ]

    @root_validator(pre=True)
    @classmethod
    def _derive_feature_related_attributes(cls, values: dict[str, Any]) -> dict[str, Any]:
        # "features" is not an attribute to the FeatureList model, when it appears in the input to
        # constructor, it is intended to be used to derive other feature-related attributes
        if "features" in values:
            features = values["features"]
            values["readiness_distribution"] = cls.derive_readiness_distribution(features)
            values["dtype_distribution"] = cls.derive_dtype_distribution(features)
            try:
                values["feature_clusters"] = cls.derive_feature_clusters(features)
            except StopIteration:
                # add a try except block here for the old features that may trigger StopIteration,
                # in this case, we will not add feature_clusters
                pass

            # add other entity related attributes
            entity_ids = set()
            table_ids = set()
            features_primary_entity_ids = set()
            for feature in features:
                entity_ids.update(feature.entity_ids)
                features_primary_entity_ids.add(tuple(feature.primary_entity_ids))
                table_ids.update(feature.table_ids)

            values["entity_ids"] = sorted(entity_ids)
            values["features_primary_entity_ids"] = sorted(features_primary_entity_ids)
            values["table_ids"] = sorted(table_ids)

            # some sanity check
            total_count = sum(
                read_count.count for read_count in values["readiness_distribution"].__root__
            )
            if total_count != len(values["feature_ids"]):
                raise ValueError(
                    "readiness_distribution total count is different from total feature ids."
                )
        return values

    @staticmethod
    def derive_readiness_distribution(features: List[FeatureModel]) -> FeatureReadinessDistribution:
        """
        Derive feature readiness distribution from features

        Parameters
        ----------
        features: List[FeatureModel]
            List of features

        Returns
        -------
        FeatureReadinessDistribution
        """
        readiness_count_map: dict[FeatureReadiness, int] = defaultdict(int)
        for feature in features:
            readiness_count_map[feature.readiness] += 1
        return FeatureReadinessDistribution(
            __root__=[
                FeatureReadinessCount(readiness=readiness, count=count)
                for readiness, count in readiness_count_map.items()
            ]
        )

    @staticmethod
    def derive_dtype_distribution(features: List[FeatureModel]) -> List[FeatureTypeFeatureCount]:
        """
        Derive feature table type distribution from features

        Parameters
        ----------
        features: List[FeatureModel]
            List of features

        Returns
        -------
        List[FeatureTypeFeatureCount]
        """
        dtype_count_map: dict[DBVarType, int] = defaultdict(int)
        for feature in features:
            dtype_count_map[feature.dtype] += 1
        return [
            FeatureTypeFeatureCount(dtype=dtype, count=count)
            for dtype, count in dtype_count_map.items()
        ]

    @staticmethod
    def derive_feature_clusters(features: List[FeatureModel]) -> List[FeatureCluster]:
        """
        Derive feature_clusters attribute from features

        Parameters
        ----------
        features: List[FeatureModel]
            List of features

        Returns
        -------
        List[FeatureCluster]
        """
        # split features into groups that share the same feature store
        groups = defaultdict(list)
        for feature in features:
            feature_store_id = feature.tabular_source.feature_store_id
            groups[feature_store_id].append(feature)

        # create a FeatureCluster for each group
        feature_clusters = []
        for feature_store_id, group_features in groups.items():
            pruned_graph, mapped_nodes = get_combined_graph_and_nodes(
                feature_objects=group_features
            )
            feature_node_relationships_info = []
            feature_node_definition_hashes = []
            for feature, mapped_node in zip(group_features, mapped_nodes):
                feature_node_relationships_info.append(
                    FeatureNodeRelationshipsInfo(
                        node_name=mapped_node.name,
                        relationships_info=feature.relationships_info or [],
                        primary_entity_ids=feature.primary_entity_ids,
                    )
                )
                feature_node_definition_hashes.append(
                    FeatureNodeDefinitionHash(
                        node_name=mapped_node.name,
                        definition_hash=feature.definition_hash,
                    )
                )
            feature_clusters.append(
                FeatureCluster(
                    feature_store_id=feature_store_id,
                    graph=pruned_graph,
                    node_names=[node.name for node in mapped_nodes],
                    feature_node_relationships_infos=feature_node_relationships_info,
                    feature_node_definition_hashes=feature_node_definition_hashes,
                )
            )
        return feature_clusters

    @property
    def versioned_name(self) -> str:
        """
        Retrieve feature name with version info

        Returns
        -------
        str
        """
        return f"{self.name}_{self.version.to_str()}"

    @property
    def feature_clusters(self) -> Optional[List[FeatureCluster]]:
        """
        List of combined graphs for features from the same feature store

        Returns
        -------
        Optional[List[FeatureCluster]]
        """
        # TODO: make this a cached_property for pydantic v2
        if self.internal_feature_clusters is None:
            return None
        if self._feature_clusters is None:
            self._feature_clusters = [
                FeatureCluster(**cluster) for cluster in self.internal_feature_clusters
            ]
        return self._feature_clusters

    @classmethod
    def _get_remote_attribute_paths(cls, document_dict: Dict[str, Any]) -> List[Path]:
        paths = []
        feature_clusters_path = document_dict.get("feature_clusters_path")
        if feature_clusters_path:
            paths.append(Path(feature_clusters_path))
        return paths

    @property
    def store_info(self) -> StoreInfo:
        """
        Store info for a feature list

        Returns
        -------
        StoreInfo
        """
        return parse_obj_as(StoreInfo, self.internal_store_info or {"type": "uninitialized"})  # type: ignore

    def initialize_store_info(
        self,
        features: List[FeatureModel],
        feature_store: FeatureStoreModel,
        feature_table_map: Dict[str, Any],
        serving_entity_specs: Optional[List[ColumnSpec]],
    ) -> None:
        """
        Initialize store info for a feature list

        Parameters
        ----------
        features: List[FeatureModel]
            List of features
        feature_store: FeatureStoreModel
            Feature store model
        feature_table_map: Dict[str, Any]
            List of offline feature table map from source table name to actual feature table
            (could be a source table or a precomputed lookup table)
        serving_entity_specs: Optional[List[ColumnSpec]]
            List of serving entity specs
        """
        store_type_to_store_info_class = {
            SourceType.SNOWFLAKE: SnowflakeStoreInfo,
            SourceType.DATABRICKS: DataBricksStoreInfo,
            SourceType.DATABRICKS_UNITY: DataBricksUnityStoreInfo,
            SourceType.SPARK: SparkStoreInfo,
        }
        if feature_store.type in store_type_to_store_info_class:
            store_info_class = store_type_to_store_info_class[feature_store.type]
            store_info = store_info_class.create(
                features=features,
                feature_store=feature_store,
                feature_table_map=feature_table_map,
                serving_entity_specs=serving_entity_specs,
            )
            self.internal_store_info = store_info.dict(by_alias=True)

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "feature_list"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("name", "version"),
                conflict_fields_signature={"name": ["name"], "version": ["version"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
        ]
        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("feature_ids"),
            pymongo.operations.IndexModel("feature_list_namespace_id"),
            pymongo.operations.IndexModel("version"),
            pymongo.operations.IndexModel("online_enabled_feature_ids"),
            pymongo.operations.IndexModel("features_primary_entity_ids"),
            pymongo.operations.IndexModel("primary_entity_ids"),
            pymongo.operations.IndexModel("entity_ids"),
            pymongo.operations.IndexModel("table_ids"),
            pymongo.operations.IndexModel("deployed"),
            pymongo.operations.IndexModel("relationships_info"),
            pymongo.operations.IndexModel("supported_serving_entity_ids"),
            [
                ("name", pymongo.TEXT),
                ("version", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]
