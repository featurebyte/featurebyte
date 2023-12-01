"""
This module contains the model for feast registry
"""
# pylint: disable=no-name-in-module
from typing import Dict, List, Optional, Tuple

from collections import defaultdict
from datetime import timedelta

from feast import Entity as FeastEntity
from feast import FeatureService as FeastFeatureService
from feast import FeatureView as FeastFeatureView
from feast import Field as FeastField
from feast.data_source import DataSource as FeastDataSource
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.entity import EntityModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.offline_store_ingest_query import OfflineStoreIngestQueryGraph
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.node.schema import DatabaseDetails


class OfflineStoreTable(FeatureByteBaseModel):
    """
    Represents an offline store table in feast, each feature of the table shares the same
    - primary entity ids
    - feature job setting
    - time-to-live (TTL) component (derived from feature job setting)
    """

    table_name: str = Field(default="")
    primary_entity_ids: List[PydanticObjectId]
    feature_job_setting: Optional[FeatureJobSetting]
    has_ttl: bool
    ingest_query_graphs: List[OfflineStoreIngestQueryGraph]

    def set_table_name(self, entity_id_to_serving_name: Dict[PydanticObjectId, str]) -> None:
        """
        Offline store table name

        Parameters
        ----------
        entity_id_to_serving_name: Dict[PydanticObjectId, str]
            Entity id to serving name mapping
        """
        entity_part = "_".join(
            entity_id_to_serving_name[entity_id] for entity_id in self.primary_entity_ids
        )
        table_name = f"fb_entity_{entity_part}"
        if self.feature_job_setting:
            fjs = self.feature_job_setting.to_seconds()
            frequency = fjs["frequency"]
            time_modulo_frequency = fjs["time_modulo_frequency"]
            blind_spot = fjs["blind_spot"]
            table_name = f"{table_name}_fjs_{frequency}_{time_modulo_frequency}_{blind_spot}"
        if self.has_ttl:
            table_name = f"{table_name}_ttl"
        self.table_name = table_name

    def create_feast_entity(
        self, entity_id_to_serving_name: Dict[PydanticObjectId, str]
    ) -> FeastEntity:
        """
        Create feast entity based on the offline store ingest query graph

        Parameters
        ----------
        entity_id_to_serving_name: Dict[PydanticObjectId, str]
            Entity id to serving name mapping

        Returns
        -------
        FeastEntity
            Feast entity
        """
        entity_id_part = "_".join(str(entity_id)[-8:] for entity_id in self.primary_entity_ids)
        entity = FeastEntity(
            name=f"entity_{entity_id_part}",
            join_keys=[
                entity_id_to_serving_name[entity_id] for entity_id in self.primary_entity_ids
            ],
        )
        return entity  # type: ignore[no-any-return]

    def create_feast_data_source(
        self,
        database_details: DatabaseDetails,
        name: str,
    ) -> FeastDataSource:
        """
        Create feast data source based on the offline store ingest query graph

        Parameters
        ----------
        database_details: DatabaseDetails
            Database details
        name: str
            Feast data source name

        Returns
        -------
        FeastDataSource
            Feast data source
        """
        return database_details.create_feast_data_source(name=name, table_name=self.table_name)

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
                    dtype=ingest_query_graph.output_dtype.to_feast_primitive_type(),
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
            offline_ingest_query_graphs = feature.extract_offline_store_ingest_query_graphs()
            for ingest_query_graph in offline_ingest_query_graphs:
                group_key = ingest_query_graph.feast_feature_view_grouping_key
                offline_table_key_to_ingest_query_graphs[group_key].append(ingest_query_graph)

        offline_store_tables = []
        for ingest_query_graphs in offline_table_key_to_ingest_query_graphs.values():
            assert len(ingest_query_graphs) > 0
            primary_entity_ids = ingest_query_graphs[0].primary_entity_ids
            feature_job_setting = ingest_query_graphs[0].feature_job_setting
            has_ttl = ingest_query_graphs[0].has_ttl
            offline_store_table = OfflineStoreTable(
                primary_entity_ids=primary_entity_ids,
                feature_job_setting=feature_job_setting,
                ingest_query_graphs=ingest_query_graphs,
                has_ttl=has_ttl,
            )
            offline_store_table.set_table_name(entity_id_to_serving_name=entity_id_to_serving_name)
            offline_store_tables.append(offline_store_table)
        return offline_store_tables

    @classmethod
    def create_feast_feature_services(
        cls,
        feature_lists: List[FeatureListModel],
        features: List[FeatureModel],
        feast_feature_views: List[FeastFeatureView],
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
            for feature_view in feast_feature_views:
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
    def create(
        cls,
        feature_store: FeatureStoreModel,
        entities: List[EntityModel],
        features: List[FeatureModel],
        feature_lists: List[FeatureListModel],
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

        Returns
        -------
        RegistryProto
        """
        cls._check_missing_entities(entities, features)
        cls._check_missing_features(features, feature_lists)

        entity_id_to_serving_name = {entity.id: entity.serving_names[0] for entity in entities}
        offline_store_tables = cls.create_offline_store_tables(
            features=features, entity_id_to_serving_name=entity_id_to_serving_name
        )
        primary_entity_ids_to_feast_entity: Dict[Tuple[PydanticObjectId, ...], FeastEntity] = {}
        feast_data_sources = []
        feast_feature_views = []
        feature_store_details = feature_store.get_feature_store_details()
        for offline_store_table in offline_store_tables:
            entity_key = tuple(offline_store_table.primary_entity_ids)
            feast_entity = primary_entity_ids_to_feast_entity.get(
                entity_key,
                offline_store_table.create_feast_entity(
                    entity_id_to_serving_name=entity_id_to_serving_name
                ),
            )
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
            feast_feature_views.append(feast_feature_view)

        feast_feature_services = cls.create_feast_feature_services(
            feature_lists=feature_lists,
            features=features,
            feast_feature_views=feast_feature_views,
        )

        # construct feast registry
        registry_proto = RegistryProto()
        registry_proto.data_sources.extend(
            [data_source.to_proto() for data_source in feast_data_sources]
        )
        registry_proto.entities.extend(
            [entity.to_proto() for entity in primary_entity_ids_to_feast_entity.values()]
        )
        registry_proto.feature_views.extend(
            [feature_view.to_proto() for feature_view in feast_feature_views]
        )
        registry_proto.feature_services.extend(
            [feature_service.to_proto() for feature_service in feast_feature_services]
        )
        return registry_proto
