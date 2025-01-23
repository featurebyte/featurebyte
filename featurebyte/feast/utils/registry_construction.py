"""
This module contains classes for constructing feast registry
"""

from __future__ import annotations

import tempfile
from collections import defaultdict
from datetime import timedelta
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, cast
from unittest.mock import patch

from feast import Entity as FeastEntity
from feast import FeatureService as FeastFeatureService
from feast import FeatureStore as FeastFeatureStore
from feast import FeatureView as FeastFeatureView
from feast import Field as FeastField
from feast import OnDemandFeatureView as FeastOnDemandFeatureView
from feast import RequestSource as FeastRequestSource
from feast.data_source import DataSource as FeastDataSource
from feast.feature_view import DUMMY_ENTITY
from feast.infra.registry.registry import Registry
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import FeastConfigBaseModel, RegistryConfig, RepoConfig
from feast.repo_contents import RepoContents
from feast.repo_operations import apply_total_with_repo_instance
from pydantic import Field as PydanticField

from featurebyte.enum import DBVarType, InternalName, SpecialColumnName
from featurebyte.feast.enum import to_feast_primitive_type
from featurebyte.feast.model.feature_store import (
    FeastDatabaseDetails,
    FeatureStoreDetailsWithFeastConfiguration,
)
from featurebyte.feast.model.online_store import get_feast_online_store_details
from featurebyte.feast.utils.on_demand_view import OnDemandFeatureViewConstructor
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.entity import EntityModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.offline_store_ingest_query import (
    OfflineStoreEntityInfo,
    OfflineStoreIngestQueryGraph,
)
from featurebyte.models.online_store import OnlineStoreModel
from featurebyte.models.parent_serving import EntityLookupStep
from featurebyte.models.precomputed_lookup_feature_table import (
    _get_feature_lists_to_relationships_info,
    get_precomputed_lookup_feature_table,
)
from featurebyte.query_graph.model.entity_relationship_info import EntityAncestorDescendantMapper
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSettingUnion
from featurebyte.query_graph.sql.entity import get_combined_serving_names

DEFAULT_REGISTRY_PROJECT_NAME = "featurebyte_project"


class EntityFeatureChecker:
    """
    Class for checking the consistency and completeness of entities and features.
    """

    @staticmethod
    def check_missing_entities(entities: List[EntityModel], features: List[FeatureModel]) -> None:
        """
        Checks if all entities have been provided

        Parameters
        ----------
        entities: List[EntityModel]
            List of featurebyte entity models
        features: List[FeatureModel]
            List of featurebyte feature models

        Raises
        ------
        ValueError
            If missing entities
        """
        primary_entity_ids = set()
        for feature in features:
            primary_entity_ids.update(feature.primary_entity_ids)

        provided_entity_ids = set(entity.id for entity in entities)
        if not primary_entity_ids.issubset(provided_entity_ids):
            raise ValueError(f"Missing entities: {primary_entity_ids - provided_entity_ids}")

    @staticmethod
    def check_missing_features(
        features: List[FeatureModel], feature_lists: List[FeatureListModel]
    ) -> None:
        """
        Checks if all features have been provided

        Parameters
        ----------
        features: List[FeatureModel]
            List of featurebyte feature models
        feature_lists: List[FeatureListModel]
            List of featurebyte feature list models

        Raises
        ------
        ValueError
            If missing features
        """
        feature_ids = set()
        for feature_list in feature_lists:
            feature_ids.update(feature_list.feature_ids)

        provided_feature_ids = set(feature.id for feature in features)
        if not feature_ids.issubset(provided_feature_ids):
            raise ValueError(f"Missing features: {feature_ids - provided_feature_ids}")


class OfflineStoreTable(FeatureByteBaseModel):
    """
    Represents an offline store table in feast, each feature of the table shares the same
    - primary entity ids
    - feature job setting
    - time-to-live (TTL) component (derived from feature job setting)
    """

    table_name: str
    feature_job_setting: Optional[FeatureJobSettingUnion] = PydanticField(default=None)
    has_ttl: bool
    output_column_names: List[str]
    output_dtypes: List[DBVarType]
    primary_entity_info: List[OfflineStoreEntityInfo]
    source_feature_table_name: Optional[str] = PydanticField(default=None)

    @property
    def primary_entity_ids(self) -> Tuple[PydanticObjectId, ...]:
        """
        Get primary entity ids

        Returns
        -------
        Tuple[PydanticObjectId, ...]
            Primary entity ids
        """
        return tuple(entity_info.id for entity_info in self.primary_entity_info)

    @classmethod
    def create(
        cls,
        table_name: str,
        ingest_query_graphs: List[OfflineStoreIngestQueryGraph],
        entity_id_to_serving_name: Dict[PydanticObjectId, str],
    ) -> OfflineStoreTable:
        """
        Create offline store table

        Parameters
        ----------
        table_name: str
            Table name
        ingest_query_graphs: List[OfflineStoreIngestQueryGraph]
            List of offline store ingest query graphs
        entity_id_to_serving_name: Dict[PydanticObjectId, str]
            Mapping from entity id to serving name

        Returns
        -------
        OfflineStoreTable
            Offline store table
        """
        assert len(ingest_query_graphs) > 0
        first_ingest_query_graph = ingest_query_graphs[0]
        return cls(
            table_name=table_name,
            feature_job_setting=first_ingest_query_graph.feature_job_setting,
            has_ttl=first_ingest_query_graph.has_ttl,
            output_column_names=[
                ingest_query_graph.output_column_name for ingest_query_graph in ingest_query_graphs
            ],
            output_dtypes=[
                ingest_query_graph.output_dtype for ingest_query_graph in ingest_query_graphs
            ],
            primary_entity_info=first_ingest_query_graph.get_primary_entity_info(
                entity_id_to_serving_name=entity_id_to_serving_name
            ),
        )

    def create_feast_entity(self) -> FeastEntity:
        """
        Create feast entity based on the offline store ingest query graph

        Returns
        -------
        FeastEntity
            Feast entity
        """
        # FIXME: We likely need to set the value type based on the dtype of the primary entity
        value_type = to_feast_primitive_type(DBVarType.VARCHAR).to_value_type()
        assert len(self.primary_entity_info) > 0
        serving_names = [entity_info.name for entity_info in self.primary_entity_info]
        entity_name = get_combined_serving_names(serving_names)
        entity = FeastEntity(
            name=entity_name,
            join_keys=[entity_name],
            value_type=value_type,
        )
        return entity

    def create_feast_data_source(
        self,
        database_details: FeastDatabaseDetails,
        name: str,
    ) -> FeastDataSource:
        """
        Create feast data source based on the offline store ingest query graph

        Parameters
        ----------
        database_details: FeastDatabaseDetails
            Database details
        name: str
            Feast data source name

        Returns
        -------
        FeastDataSource
            Feast data source
        """
        return database_details.create_feast_data_source(
            name=name,
            table_name=self.table_name,
            timestamp_field=InternalName.FEATURE_TIMESTAMP_COLUMN,
        )

    def create_feast_feature_view(
        self,
        name: str,
        entity: FeastEntity,
        data_source: FeastDataSource,
    ) -> FeastFeatureView:
        """
        Create feast feature view based on the offline store ingest query graph

        Parameters
        ----------
        name: str
            Feast feature view name
        entity: FeastEntity
            Feast entity
        data_source: FeastDataSource
            Feast data source

        Returns
        -------
        FeastFeatureView
        """
        time_to_live = None
        schema = []
        if self.has_ttl:
            assert self.feature_job_setting is not None
            time_to_live = timedelta(seconds=self.feature_job_setting.extract_ttl_seconds())
            schema.append(
                FeastField(
                    name=InternalName.FEATURE_TIMESTAMP_COLUMN,
                    dtype=to_feast_primitive_type(DBVarType.TIMESTAMP),
                )
            )

        for output_column_name, output_dtype in zip(self.output_column_names, self.output_dtypes):
            schema.append(
                FeastField(
                    name=output_column_name,
                    dtype=to_feast_primitive_type(DBVarType(output_dtype)),
                )
            )

        feature_view = FeastFeatureView(
            name=name,
            entities=[entity],
            ttl=time_to_live,
            schema=schema,
            online=True,
            source=data_source,
        )
        return feature_view


class OfflineStoreTableBuilder:
    """
    Class for building Offline Store Tables.
    """

    @staticmethod
    def create_offline_store_tables(
        features: List[FeatureModel],
        feature_lists: List[FeatureListModel],
        entity_id_to_serving_name: Dict[PydanticObjectId, str],
        feature_store: FeatureStoreModel,
        entity_lookup_steps_mapping: Dict[PydanticObjectId, EntityLookupStep],
        serving_entity_ids: Optional[List[PydanticObjectId]],
    ) -> List[OfflineStoreTable]:
        """
        Group each offline store ingest query graphs of features into list of offline store tables

        Parameters
        ----------
        features: List[FeatureModel]
            List of featurebyte feature models
        feature_lists: List[FeatureListModel]
            List of feature lists
        entity_id_to_serving_name: Dict[PydanticObjectId, str]
            Mapping from entity id to serving name
        feature_store: FeatureStoreModel
            Feature store model
        entity_lookup_steps_mapping: Dict[PydanticObjectId, EntityLookupStep]
            Entity lookup steps mapping derived from feature lists
        serving_entity_ids: Optional[List[PydanticObjectId]]
            Serving entity ids based on the deployment

        Returns
        -------
        List[OfflineStoreTable]
            List of offline store tables
        """
        offline_table_key_to_ingest_query_graphs = defaultdict(list)
        offline_table_key_to_feature_ids = defaultdict(set)
        for feature in features:
            offline_ingest_query_graphs = (
                feature.offline_store_info.extract_offline_store_ingest_query_graphs()
            )
            for ingest_query_graph in offline_ingest_query_graphs:
                table_name = ingest_query_graph.offline_store_table_name
                offline_table_key_to_ingest_query_graphs[table_name].append(ingest_query_graph)
                offline_table_key_to_feature_ids[table_name].add(feature.id)

        offline_store_tables = []
        for table_name, ingest_query_graphs in offline_table_key_to_ingest_query_graphs.items():
            offline_store_table = OfflineStoreTable.create(
                table_name=table_name,
                ingest_query_graphs=ingest_query_graphs,
                entity_id_to_serving_name=entity_id_to_serving_name,
            )
            offline_store_tables.append(offline_store_table)
            if serving_entity_ids is not None:
                assert len(feature_lists) == 1
                relationships_info = _get_feature_lists_to_relationships_info(feature_lists)[
                    feature_lists[0].id
                ]
                relationships_mapper = EntityAncestorDescendantMapper.create(relationships_info)
                related_serving_entity_ids = relationships_mapper.keep_related_entity_ids(
                    entity_ids_to_filter=serving_entity_ids,
                    filter_by=offline_store_table.primary_entity_ids,
                )
                if sorted(offline_store_table.primary_entity_ids) != related_serving_entity_ids:
                    precomputed_lookup_feature_table = (
                        OfflineStoreTableBuilder._get_precomputed_lookup_feature_table(
                            offline_store_table=offline_store_table,
                            full_serving_entity_ids=serving_entity_ids,
                            feature_list=feature_lists[0],
                            entity_id_to_serving_name=entity_id_to_serving_name,
                            entity_lookup_steps_mapping=entity_lookup_steps_mapping,
                            feature_store=feature_store,
                            offline_table_key_to_feature_ids=offline_table_key_to_feature_ids,
                        )
                    )
                    assert precomputed_lookup_feature_table is not None
                    offline_store_tables.append(precomputed_lookup_feature_table)

        return offline_store_tables

    @staticmethod
    def _get_precomputed_lookup_feature_table(
        offline_store_table: OfflineStoreTable,
        full_serving_entity_ids: List[PydanticObjectId],
        feature_list: FeatureListModel,
        entity_id_to_serving_name: Dict[PydanticObjectId, str],
        entity_lookup_steps_mapping: Dict[PydanticObjectId, EntityLookupStep],
        feature_store: FeatureStoreModel,
        offline_table_key_to_feature_ids: Dict[str, Set[PydanticObjectId]],
    ) -> Optional[OfflineStoreTable]:
        """
        Get a precomputed lookup feature table corresponding to offline_store_table that can be
        readily served using serving_entity_ids

        # noqa: DAR101

        Returns
        -------
        Optional[OfflineStoreTable]
        """
        precomputed_lookup_feature_table = get_precomputed_lookup_feature_table(
            primary_entity_ids=list(offline_store_table.primary_entity_ids),
            feature_ids=list(offline_table_key_to_feature_ids[offline_store_table.table_name]),
            feature_list=feature_list,
            full_serving_entity_ids=full_serving_entity_ids,
            feature_table_name=offline_store_table.table_name,
            feature_table_has_ttl=offline_store_table.has_ttl,
            entity_id_to_serving_name=entity_id_to_serving_name,
            entity_lookup_steps_mapping=entity_lookup_steps_mapping,
            feature_store_model=feature_store,
        )
        if precomputed_lookup_feature_table is not None:
            return OfflineStoreTable(
                table_name=precomputed_lookup_feature_table.name,
                feature_job_setting=offline_store_table.feature_job_setting,
                has_ttl=offline_store_table.has_ttl,
                output_column_names=offline_store_table.output_column_names,
                output_dtypes=offline_store_table.output_dtypes,
                primary_entity_info=[
                    OfflineStoreEntityInfo(
                        id=entity_id,
                        name=serving_name,
                        dtype=DBVarType.VARCHAR,
                    )
                    for (entity_id, serving_name) in zip(
                        precomputed_lookup_feature_table.primary_entity_ids,
                        precomputed_lookup_feature_table.serving_names,
                    )
                ],
                source_feature_table_name=offline_store_table.table_name,
            )
        return None


class FeastAssetCreator:
    """
    Class for creating various Feast assets like Data Source, Feature View, etc.
    """

    @staticmethod
    def create_feast_name_to_request_source(
        features: List[FeatureModel],
    ) -> Dict[str, FeastRequestSource]:
        """
        Create feast request source based on the features

        Parameters
        ----------
        features: List[FeatureModel]
            List of featurebyte feature models

        Returns
        -------
        Dict[str, FeastRequestSource]
            Mapping from feast request source name to feast request source
        """
        name_to_feast_request_source: Dict[str, FeastRequestSource] = {}
        for feature in features:
            for req_col_node in feature.extract_request_column_nodes():
                req_col_name = req_col_node.parameters.column_name
                if req_col_name not in name_to_feast_request_source:
                    name_to_feast_request_source[req_col_name] = FeastRequestSource(
                        name=req_col_name,
                        schema=[
                            FeastField(
                                name=req_col_name,
                                dtype=to_feast_primitive_type(
                                    DBVarType(req_col_node.parameters.dtype)
                                ),
                            )
                        ],
                    )

        # always add point in time as a request source for on-demand feature views
        if SpecialColumnName.POINT_IN_TIME.value not in name_to_feast_request_source:
            name_to_feast_request_source[SpecialColumnName.POINT_IN_TIME.value] = (
                FeastRequestSource(
                    name=SpecialColumnName.POINT_IN_TIME.value,
                    schema=[
                        FeastField(
                            name=SpecialColumnName.POINT_IN_TIME.value,
                            dtype=to_feast_primitive_type(DBVarType.TIMESTAMP),
                        )
                    ],
                )
            )

        return name_to_feast_request_source

    @staticmethod
    def create_feast_on_demand_feature_views(
        features: List[FeatureModel],
        name_to_feast_feature_view: Dict[str, FeastFeatureView],
        name_to_feast_request_source: Dict[str, FeastRequestSource],
    ) -> List[FeastOnDemandFeatureView]:
        """
        Create feast on demand feature views based on the features

        Parameters
        ----------
        features: List[FeatureModel]
            List of featurebyte feature models
        name_to_feast_feature_view: Dict[str, FeastFeatureView]
            Mapping from feast feature view name to feast feature view
        name_to_feast_request_source: Dict[str, FeastRequestSource]
            Mapping from feast request source name to feast request source

        Returns
        -------
        List[FeastOnDemandFeatureView]
            List of feast on demand feature views
        """
        on_demand_feature_views: List[FeastOnDemandFeatureView] = []
        for feature in features:
            if not feature.offline_store_info.is_decomposed:
                assert feature.offline_store_info.metadata is not None
                if not feature.offline_store_info.metadata.has_ttl:
                    continue

            on_demand_feature_view = OnDemandFeatureViewConstructor.create(
                feature_model=feature,
                name_to_feast_feature_view=name_to_feast_feature_view,
                name_to_feast_request_source=name_to_feast_request_source,
            )
            on_demand_feature_views.append(on_demand_feature_view)
        return on_demand_feature_views

    @staticmethod
    def create_feast_feature_services(
        feature_lists: List[FeatureListModel],
        features: List[FeatureModel],
        feast_feature_views: List[FeastFeatureView],
        feast_on_demand_feature_views: List[FeastOnDemandFeatureView],
    ) -> List[FeastFeatureService]:
        """
        Create feast feature services based on the feature lists

        Parameters
        ----------
        feature_lists: List[FeatureListModel]
            List of featurebyte feature list models
        features: List[FeatureModel]
            List of featurebyte feature models
        feast_feature_views: List[FeastFeatureView]
            List of feast feature views
        feast_on_demand_feature_views: List[FeastOnDemandFeatureView]
            List of feast on demand feature views

        Returns
        -------
        List[FeastFeatureService]
            List of feast feature services
        """
        feature_id_to_name_version = {feature.id: feature.versioned_name for feature in features}
        feature_services = []
        for feature_list in feature_lists:
            feature_name_versions = set(
                feature_id_to_name_version[feature_id] for feature_id in feature_list.feature_ids
            )

            # construct input for feature service
            input_feature_views = []
            found_feature_name_versions = set()
            for feature_view in feast_on_demand_feature_views + feast_feature_views:
                feast_feat_names = [
                    feat.name
                    for feat in feature_view.features
                    if feat.name in feature_name_versions
                    and feat.name not in found_feature_name_versions
                ]
                if feast_feat_names:
                    input_feature_views.append(feature_view[feast_feat_names])
                    found_feature_name_versions.update(feast_feat_names)

            # construct feature service
            feature_service = FeastFeatureService(
                name=feature_list.versioned_name,
                features=input_feature_views,
            )
            feature_services.append(feature_service)
        return feature_services


class FeastRegistryBuilder:
    """
    Class for constructing the Feast Registry.
    """

    @staticmethod
    def get_offline_store_config(
        feature_store_model: FeatureStoreModel, offline_store_credentials: Any
    ) -> Any:
        """
        Get the offline store configuration

        Parameters
        ----------
        feature_store_model: FeatureStoreModel
            Feature store model
        offline_store_credentials: Any
            Offline store credentials

        Returns
        -------
        Any
            Offline store configuration
        """
        feature_store_details = FeatureStoreDetailsWithFeastConfiguration(
            **feature_store_model.get_feature_store_details().model_dump()
        )
        database_credential = None
        storage_credential = None
        if offline_store_credentials:
            database_credential = offline_store_credentials.database_credential
            storage_credential = offline_store_credentials.storage_credential
        offline_store_config = feature_store_details.details.get_offline_store_config(
            database_credential=database_credential,
            storage_credential=storage_credential,
        )
        return offline_store_config

    @staticmethod
    def get_online_store_config(
        online_store: Optional[OnlineStoreModel],
    ) -> Optional[FeastConfigBaseModel]:
        """
        Get the online store configuration

        Parameters
        ----------
        online_store: Optional[OnlineStoreModel]
            Online store model

        Returns
        -------
        Optional[FaestConfigBaseModel]
            Online store configuration
        """
        if online_store is None:
            return None

        return get_feast_online_store_details(
            online_store_details=online_store.details
        ).to_feast_online_store_config()

    @staticmethod
    def create_repo_config(
        project_name: str,
        registry_file_path: str,
        offline_store_config: Optional[Any] = None,
        online_store_config: Optional[Any] = None,
        registry_store_type: Optional[str] = None,
    ) -> RepoConfig:
        """
        Create a RepoConfig object for the feast registry construction

        Parameters
        ----------
        project_name: str
            Project name
        registry_file_path: str
            Registry file path
        offline_store_config: Optional[Any]
            Feast offline store configuration
        online_store_config: Optional[Any]
            Online store configuration
        registry_store_type: Optional[str]
            Registry store type used for the registry

        Returns
        -------
        RepoConfig
        """
        repo_config_kwargs = {
            "online_store": online_store_config,
            "entity_key_serialization_version": 2,
        }
        if offline_store_config:
            repo_config_kwargs["offline_store"] = offline_store_config

        registry_config_kwargs = {}
        if registry_store_type:
            registry_config_kwargs["registry_store_type"] = registry_store_type

        return RepoConfig(
            project=project_name,
            provider="local",
            registry=RegistryConfig(
                registry_type="file",
                path=registry_file_path,
                cache_ttl_seconds=0,
                **registry_config_kwargs,
            ),
            **repo_config_kwargs,
        )

    @classmethod
    def create_feast_registry_proto_from_repo_content(
        cls,
        project_name: str,
        offline_store_config: Optional[Any],
        online_store: Optional[OnlineStoreModel],
        repo_content: RepoContents,
    ) -> RegistryProto:
        """
        Create a feast RegistryProto from a RepoContents object

        Parameters
        ----------
        project_name: str
            Project name
        offline_store_config: Optional[Any]
            Offline store configuration
        online_store: Optional[OnlineStoreModel]
            Online store model
        repo_content: RepoContents
            Repo contents containing the feast assets

        Returns
        -------
        RegistryProto
        """
        with tempfile.NamedTemporaryFile() as temp_file:
            online_store_config = cls.get_online_store_config(online_store)
            repo_config = cls.create_repo_config(
                project_name=project_name,
                offline_store_config=offline_store_config,
                online_store_config=online_store_config,
                registry_file_path=temp_file.name,
            )
            feature_store = FeastFeatureStore(config=repo_config)
            registry = feature_store.registry

            with patch("feast.on_demand_feature_view.OnDemandFeatureView.infer_features"):
                # FIXME: (DEV-2946) patch to avoid calling infer_features() which may cause error
                #  when the input to the on-demand feature view contains types requiring json decoding
                #  (COUNT_DICT or ARRAY types) this simulates feast apply command
                apply_total_with_repo_instance(
                    store=feature_store,
                    project=project_name,
                    registry=cast(Registry, registry),
                    repo=repo_content,
                    skip_source_validation=True,
                )
                return registry.proto()

    @classmethod
    def _create_feast_registry_proto(
        cls,
        project_name: Optional[str],
        online_store: Optional[OnlineStoreModel],
        feast_data_sources: List[FeastDataSource],
        primary_entity_ids_to_feast_entity: Dict[Tuple[PydanticObjectId, ...], FeastEntity],
        feast_request_sources: List[FeastRequestSource],
        feast_feature_views: List[FeastFeatureView],
        feast_on_demand_feature_views: List[FeastOnDemandFeatureView],
        feast_feature_services: List[FeastFeatureService],
    ) -> RegistryProto:
        project_name = project_name or DEFAULT_REGISTRY_PROJECT_NAME

        # prepare repo content by adding all feast assets
        repo_content = RepoContents(
            data_sources=[],
            entities=[],
            feature_views=[],
            feature_services=[],
            on_demand_feature_views=[],
            stream_feature_views=[],
        )
        for data_source in feast_data_sources + feast_request_sources:
            repo_content.data_sources.append(data_source)
        for entity in primary_entity_ids_to_feast_entity.values():
            repo_content.entities.append(entity)
        for feature_view in feast_feature_views:
            repo_content.feature_views.append(feature_view)
        for on_demand_feature_view in feast_on_demand_feature_views:
            repo_content.on_demand_feature_views.append(on_demand_feature_view)
        for feature_service in feast_feature_services:
            repo_content.feature_services.append(feature_service)

        registry_proto = cls.create_feast_registry_proto_from_repo_content(
            project_name=project_name,
            offline_store_config=None,
            online_store=online_store,
            repo_content=repo_content,
        )
        return registry_proto

    @classmethod
    def create(
        cls,
        feature_store: FeatureStoreModel,
        online_store: Optional[OnlineStoreModel],
        entities: List[EntityModel],
        features: List[FeatureModel],
        feature_lists: List[FeatureListModel],
        entity_lookup_steps_mapping: Dict[PydanticObjectId, EntityLookupStep],
        serving_entity_ids: Optional[List[PydanticObjectId]],
        project_name: Optional[str] = None,
    ) -> RegistryProto:
        """
        Create a feast RegistryProto from featurebyte asset models

        Parameters
        ----------
        feature_store: FeatureStoreModel
            Feature store model
        online_store: Optional[OnlineStoreModel]
            Online store model
        entities: List[EntityModel]
            List of featurebyte entity models
        features: List[FeatureModel]
            List of featurebyte feature models
        feature_lists: List[FeatureListModel]
            List of featurebyte feature list models
        entity_lookup_steps_mapping: Dict[PydanticObjectId, EntityLookupStep]
            Mapping from relationships info id to EntityLookupStep objects
        serving_entity_ids: Optional[List[PydanticObjectId]]
            Serving entity ids of the deployment that the registry will be associated with
        project_name: Optional[str]
            Project name

        Returns
        -------
        RegistryProto
        """
        EntityFeatureChecker.check_missing_entities(entities, features)
        EntityFeatureChecker.check_missing_features(features, feature_lists)
        offline_store_tables = OfflineStoreTableBuilder.create_offline_store_tables(
            features=features,
            feature_lists=feature_lists,
            entity_id_to_serving_name={entity.id: entity.serving_names[0] for entity in entities},
            feature_store=feature_store,
            entity_lookup_steps_mapping=entity_lookup_steps_mapping,
            serving_entity_ids=serving_entity_ids,
        )
        primary_entity_ids_to_feast_entity: Dict[Tuple[PydanticObjectId, ...], FeastEntity] = {}
        feast_data_sources = []
        name_to_feast_feature_view: Dict[str, FeastFeatureView] = {}
        all_feast_feature_views = []
        feature_store_details = FeatureStoreDetailsWithFeastConfiguration(
            **feature_store.get_feature_store_details().model_dump()
        )
        for offline_store_table in offline_store_tables:
            entity_key = offline_store_table.primary_entity_ids
            if len(entity_key) > 0:
                feast_entity = primary_entity_ids_to_feast_entity.get(
                    entity_key,
                    offline_store_table.create_feast_entity(),
                )
            else:
                feast_entity = DUMMY_ENTITY
            if entity_key not in primary_entity_ids_to_feast_entity:
                primary_entity_ids_to_feast_entity[entity_key] = feast_entity

            feast_data_source = offline_store_table.create_feast_data_source(
                database_details=feature_store_details.details,
                name=offline_store_table.table_name,
            )
            feast_data_sources.append(feast_data_source)

            feast_feature_view = offline_store_table.create_feast_feature_view(
                name=offline_store_table.table_name,
                entity=feast_entity,
                data_source=feast_data_source,
            )
            if offline_store_table.source_feature_table_name is not None:
                table_name = offline_store_table.source_feature_table_name
            else:
                table_name = offline_store_table.table_name
            name_to_feast_feature_view[table_name] = feast_feature_view
            all_feast_feature_views.append(feast_feature_view)

        name_to_feast_request_source = FeastAssetCreator.create_feast_name_to_request_source(
            features
        )
        on_demand_feature_views = FeastAssetCreator.create_feast_on_demand_feature_views(
            features=features,
            name_to_feast_feature_view=name_to_feast_feature_view,
            name_to_feast_request_source=name_to_feast_request_source,
        )
        feast_feature_services = FeastAssetCreator.create_feast_feature_services(
            feature_lists=feature_lists,
            features=features,
            feast_feature_views=list(name_to_feast_feature_view.values()),
            feast_on_demand_feature_views=on_demand_feature_views,
        )

        return cls._create_feast_registry_proto(
            project_name=project_name,
            online_store=online_store,
            feast_data_sources=feast_data_sources,
            primary_entity_ids_to_feast_entity=primary_entity_ids_to_feast_entity,
            feast_request_sources=list(name_to_feast_request_source.values()),
            feast_feature_views=all_feast_feature_views,
            feast_on_demand_feature_views=on_demand_feature_views,
            feast_feature_services=feast_feature_services,
        )

    @classmethod
    def create_feast_registry_proto_for_feature_materialization(
        cls,
        project_name: Optional[str],
        offline_store_config: Any,
        online_store: Optional[OnlineStoreModel],
        feature_table_name: str,
        feast_stores: Sequence[FeastFeatureStore],
    ) -> RegistryProto:
        """
        Create a feast RegistryProto for feature materialization task

        Parameters
        ----------
        project_name: Optional[str]
            Project name
        offline_store_config: Any
            Offline store configuration
        online_store: Optional[OnlineStoreModel]
            Online store model
        feature_table_name: str
            Feature table name
        feast_stores: Sequence[FeastFeatureStore]
            Sequence of feast feature stores to get the feature view from

        Returns
        -------
        RegistryProto
        """
        repo_content = RepoContents(
            data_sources=[],
            entities=[],
            feature_views=[],
            feature_services=[],
            on_demand_feature_views=[],
            stream_feature_views=[],
        )

        first_store = feast_stores[0]
        repo_content.data_sources.extend(first_store.list_data_sources())
        repo_content.entities.extend(first_store.list_entities())

        first_fv = first_store.get_feature_view(feature_table_name)
        if not first_fv.entities:
            existing_entities = {entity.name for entity in repo_content.entities}
            if DUMMY_ENTITY.name not in existing_entities:
                repo_content.entities.append(DUMMY_ENTITY)

            fv_entities = [DUMMY_ENTITY]
        else:
            fv_entities = [
                FeastEntity(
                    name=entity.name,
                    join_keys=[entity.name],
                    value_type=entity.dtype.to_value_type(),
                )
                for entity in first_fv.entity_columns
            ]

        feature_view_params: Dict[str, Any] = {
            "name": feature_table_name,
            "entities": fv_entities,
            "ttl": first_fv.ttl,
            "online": True,
            "source": first_fv.batch_source,
        }
        name_to_field_map = {}
        for feast_store in feast_stores:
            for feature in feast_store.get_feature_view(feature_table_name).features:
                if feature.name not in name_to_field_map:
                    name_to_field_map[feature.name] = feature

        feature_view_params["schema"] = list(name_to_field_map.values())
        repo_content.feature_views.append(FeastFeatureView(**feature_view_params))

        registry_proto = FeastRegistryBuilder.create_feast_registry_proto_from_repo_content(
            project_name=project_name or DEFAULT_REGISTRY_PROJECT_NAME,
            offline_store_config=offline_store_config,
            online_store=online_store,
            repo_content=repo_content,
        )
        return registry_proto
