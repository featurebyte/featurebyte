"""
This module contains classes for constructing feast registry
"""
# pylint: disable=no-name-in-module
from typing import Dict, List, Optional, Tuple, cast

import tempfile
from collections import defaultdict
from datetime import timedelta

from feast import Entity as FeastEntity
from feast import FeatureService as FeastFeatureService
from feast import FeatureStore as FeastFeatureStore
from feast import FeatureView as FeastFeatureView
from feast import Field as FeastField
from feast import OnDemandFeatureView as FeastOnDemandFeatureView
from feast import RequestSource as FeastRequestSource
from feast.data_source import DataSource as FeastDataSource
from feast.feature_view import DUMMY_ENTITY
from feast.inference import update_feature_views_with_inferred_features_and_entities
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import RegistryConfig, RepoConfig

from featurebyte.common.env_util import add_sys_path
from featurebyte.enum import DBVarType, InternalName
from featurebyte.feast.enum import to_feast_primitive_type
from featurebyte.feast.model.feature_store import (
    FeastDatabaseDetails,
    FeatureStoreDetailsWithFeastConfiguration,
)
from featurebyte.feast.utils.on_demand_view import OnDemandFeatureViewConstructor
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.entity import EntityModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.offline_store_ingest_query import OfflineStoreIngestQueryGraph
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting


class OfflineStoreTable(FeatureByteBaseModel):
    """
    Represents an offline store table in feast, each feature of the table shares the same
    - primary entity ids
    - feature job setting
    - time-to-live (TTL) component (derived from feature job setting)
    """

    table_name: str
    primary_entity_ids: List[PydanticObjectId]
    feature_job_setting: Optional[FeatureJobSetting]
    has_ttl: bool
    ingest_query_graphs: List[OfflineStoreIngestQueryGraph]
    primary_entity_serving_names: List[str]

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
        assert len(self.primary_entity_serving_names) > 0
        entity = FeastEntity(
            name=" x ".join(self.primary_entity_serving_names),
            join_keys=self.primary_entity_serving_names,
            value_type=value_type,
        )
        return entity  # type: ignore[no-any-return]

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
        if self.has_ttl:
            assert self.feature_job_setting is not None
            time_to_live = timedelta(seconds=2 * self.feature_job_setting.frequency_seconds)

        schema = []
        for ingest_query_graph in self.ingest_query_graphs:
            schema.append(
                FeastField(
                    name=ingest_query_graph.output_column_name,
                    dtype=to_feast_primitive_type(DBVarType(ingest_query_graph.output_dtype)),
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
        return feature_view  # type: ignore[no-any-return]


class FeastRegistryConstructor:
    """Generates a feast RegistryProto from featurebyte asset models"""

    @staticmethod
    def _check_missing_entities(entities: List[EntityModel], features: List[FeatureModel]) -> None:
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
    def _check_missing_features(
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

    @staticmethod
    def create_offline_store_tables(
        features: List[FeatureModel], entity_id_to_serving_name: Dict[PydanticObjectId, str]
    ) -> List[OfflineStoreTable]:
        """
        Group each offline store ingest query graphs of features into list of offline store tables

        Parameters
        ----------
        features: List[FeatureModel]
            List of featurebyte feature models
        entity_id_to_serving_name: Dict[PydanticObjectId, str]
            Mapping from entity id to serving name

        Returns
        -------
        List[OfflineStoreTable]
            List of offline store tables
        """
        offline_table_key_to_ingest_query_graphs = defaultdict(list)
        for feature in features:
            offline_ingest_query_graphs = (
                feature.offline_store_info.extract_offline_store_ingest_query_graphs()
            )
            for ingest_query_graph in offline_ingest_query_graphs:
                table_name = ingest_query_graph.offline_store_table_name
                offline_table_key_to_ingest_query_graphs[table_name].append(ingest_query_graph)

        offline_store_tables = []
        for table_name, ingest_query_graphs in offline_table_key_to_ingest_query_graphs.items():
            assert len(ingest_query_graphs) > 0
            primary_entity_ids = ingest_query_graphs[0].primary_entity_ids
            feature_job_setting = ingest_query_graphs[0].feature_job_setting
            has_ttl = ingest_query_graphs[0].has_ttl
            offline_store_table = OfflineStoreTable(
                table_name=table_name,
                primary_entity_ids=primary_entity_ids,
                feature_job_setting=feature_job_setting,
                ingest_query_graphs=ingest_query_graphs,
                has_ttl=has_ttl,
                primary_entity_serving_names=[
                    entity_id_to_serving_name[entity_id] for entity_id in primary_entity_ids
                ],
            )
            offline_store_tables.append(offline_store_table)
        return offline_store_tables

    @classmethod
    def create_feast_name_to_request_source(
        cls, features: List[FeatureModel]
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
        return name_to_feast_request_source

    @classmethod
    def create_feast_on_demand_feature_views(
        cls,
        features: List[FeatureModel],
        name_to_feast_feature_view: Dict[str, FeastFeatureView],
        name_to_feast_request_source: Dict[str, FeastRequestSource],
        on_demand_feature_view_dir: str,
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
        on_demand_feature_view_dir: str
            Directory to store the on demand feature view

        Returns
        -------
        List[FeastOnDemandFeatureView]
            List of feast on demand feature views
        """
        on_demand_feature_views: List[FeastOnDemandFeatureView] = []
        for feature in features:
            if feature.offline_store_info is None or not feature.offline_store_info.is_decomposed:
                continue

            on_demand_feature_view = OnDemandFeatureViewConstructor.create(
                feature_model=feature,
                name_to_feast_feature_view=name_to_feast_feature_view,
                name_to_feast_request_source=name_to_feast_request_source,
                on_demand_feature_view_dir=on_demand_feature_view_dir,
            )
            on_demand_feature_views.append(on_demand_feature_view)
        return on_demand_feature_views

    @classmethod
    def create_feast_feature_services(
        cls,
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

        Raises
        ------
        ValueError
            If missing features
        """
        feature_id_to_name = {feature.id: feature.name for feature in features}
        feature_services = []
        for feature_list in feature_lists:
            feature_names = set(
                feature_id_to_name[feature_id] for feature_id in feature_list.feature_ids
            )

            # construct input for feature service
            input_feature_views = []
            found_feature_names = set()
            for feature_view in feast_feature_views + feast_on_demand_feature_views:
                feast_feat_names = [
                    feat.name for feat in feature_view.features if feat.name in feature_names
                ]
                if feast_feat_names:
                    input_feature_views.append(feature_view[feast_feat_names])
                    found_feature_names.update(feast_feat_names)

            # check if all feature names are found
            if found_feature_names != feature_names:
                # If the feature list contains features that require post-processing or request column,
                # the feature name will not be found in the feature view. This will be fixed once we support
                # ondemand feature view.
                raise ValueError(f"Missing features: {feature_names - found_feature_names}")

            # construct feature service
            feature_service = FeastFeatureService(
                name=feature_list.name,
                features=input_feature_views,
            )
            feature_services.append(feature_service)
        return feature_services

    @classmethod
    def _create_feast_registry_proto(
        cls,
        project_name: Optional[str],
        feast_data_sources: List[FeastDataSource],
        primary_entity_ids_to_feast_entity: Dict[Tuple[PydanticObjectId, ...], FeastEntity],
        feast_request_sources: List[FeastRequestSource],
        feast_feature_views: List[FeastFeatureView],
        feast_on_demand_feature_views: List[FeastOnDemandFeatureView],
        feast_feature_services: List[FeastFeatureService],
    ) -> RegistryProto:
        project_name = project_name or "featurebyte_project"
        with tempfile.NamedTemporaryFile() as temp_file:
            repo_config = RepoConfig(
                project=project_name,
                provider="local",
                registry=RegistryConfig(
                    registry_type="file",
                    path=temp_file.name,
                    cache_ttl_seconds=0,
                ),
            )

            # FIXME: Temporarily calling this inference function here to populate the entity_columns
            #  field in feature views which is needed by feast materialize. This can be removed once
            #  we call feast apply code path directly.
            update_feature_views_with_inferred_features_and_entities(
                feast_feature_views, list(primary_entity_ids_to_feast_entity.values()), repo_config
            )
            feature_store = FeastFeatureStore(config=repo_config)
            registry = feature_store.registry
            for data_source in feast_data_sources + feast_request_sources:
                registry.apply_data_source(data_source=data_source, project=project_name)
            for entity in primary_entity_ids_to_feast_entity.values():
                registry.apply_entity(entity=entity, project=project_name)
            for feature_view in feast_feature_views + feast_on_demand_feature_views:
                registry.apply_feature_view(feature_view=feature_view, project=project_name)
            for feature_service in feast_feature_services:
                registry.apply_feature_service(
                    feature_service=feature_service, project=project_name
                )
            registry_proto = registry.proto()
            return cast(RegistryProto, registry_proto)

    @classmethod
    def create(
        cls,
        feature_store: FeatureStoreModel,
        entities: List[EntityModel],
        features: List[FeatureModel],
        feature_lists: List[FeatureListModel],
        project_name: Optional[str] = None,
    ) -> RegistryProto:
        """
        Create a feast RegistryProto from featurebyte asset models

        Parameters
        ----------
        feature_store: FeatureStoreModel
            Feature store model
        entities: List[EntityModel]
            List of featurebyte entity models
        features: List[FeatureModel]
            List of featurebyte feature models
        feature_lists: List[FeatureListModel]
            List of featurebyte feature list models
        project_name: Optional[str]
            Project name

        Returns
        -------
        RegistryProto
        """
        cls._check_missing_entities(entities, features)
        cls._check_missing_features(features, feature_lists)

        offline_store_tables = cls.create_offline_store_tables(
            features=features,
            entity_id_to_serving_name={entity.id: entity.serving_names[0] for entity in entities},
        )
        primary_entity_ids_to_feast_entity: Dict[Tuple[PydanticObjectId, ...], FeastEntity] = {}
        feast_data_sources = []
        name_to_feast_feature_view: Dict[str, FeastFeatureView] = {}
        feature_store_details = FeatureStoreDetailsWithFeastConfiguration(
            **feature_store.get_feature_store_details().dict()
        )
        for offline_store_table in offline_store_tables:
            entity_key = tuple(offline_store_table.primary_entity_ids)
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
            name_to_feast_feature_view[offline_store_table.table_name] = feast_feature_view

        name_to_feast_request_source = cls.create_feast_name_to_request_source(features)
        with tempfile.TemporaryDirectory() as temp_dir:
            with add_sys_path(temp_dir):
                on_demand_feature_views = cls.create_feast_on_demand_feature_views(
                    features=features,
                    name_to_feast_feature_view=name_to_feast_feature_view,
                    name_to_feast_request_source=name_to_feast_request_source,
                    on_demand_feature_view_dir=temp_dir,
                )
                feast_feature_services = cls.create_feast_feature_services(
                    feature_lists=feature_lists,
                    features=features,
                    feast_feature_views=list(name_to_feast_feature_view.values()),
                    feast_on_demand_feature_views=on_demand_feature_views,
                )

                # construct feast registry by constructing a feast feature store and extracting the registry
                return cls._create_feast_registry_proto(
                    project_name=project_name,
                    feast_data_sources=feast_data_sources,
                    primary_entity_ids_to_feast_entity=primary_entity_ids_to_feast_entity,
                    feast_request_sources=list(name_to_feast_request_source.values()),
                    feast_feature_views=list(name_to_feast_feature_view.values()),
                    feast_on_demand_feature_views=on_demand_feature_views,
                    feast_feature_services=feast_feature_services,
                )
