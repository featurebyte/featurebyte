"""
OfflineStoreFeatureTableUpdateService class
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple, cast

from collections import defaultdict
from dataclasses import dataclass

from bson import ObjectId

from featurebyte import FeatureJobSetting
from featurebyte.feast.schema.registry import FeastRegistryCreate, FeastRegistryUpdate
from featurebyte.feast.service.registry import FeastRegistryService
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.offline_store_feature_table import (
    FeaturesUpdate,
    OfflineStoreFeatureTableModel,
    get_offline_store_feature_table_model,
)
from featurebyte.models.offline_store_ingest_query import OfflineStoreIngestQueryGraph
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_materialize import FeatureMaterializeService
from featurebyte.service.feature_materialize_scheduler import FeatureMaterializeSchedulerService
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService
from featurebyte.service.online_store_compute_query_service import OnlineStoreComputeQueryService


@dataclass
class OfflineIngestGraphContainer:
    """
    OfflineIngestGraphContainer class

    This helper class breaks FeatureModel objects into groups by offline store feature table name.
    """

    offline_store_table_name_to_features: Dict[str, List[FeatureModel]]
    offline_store_table_name_to_graphs: Dict[str, List[OfflineStoreIngestQueryGraph]]

    @classmethod
    async def build(
        cls,
        features: List[FeatureModel],
    ) -> OfflineIngestGraphContainer:
        """
        Build OfflineIngestGraphContainer

        Parameters
        ----------
        features : List[FeatureModel]
            List of features

        Returns
        -------
        OfflineIngestGraphContainer
        """
        # Build mapping from entity id to serving names needed by offline store info extraction
        all_feature_entity_ids = set()
        for feature in features:
            all_feature_entity_ids.update(feature.entity_ids)

        # Group features by offline store feature table name
        offline_store_table_name_to_features = defaultdict(list)
        offline_store_table_name_to_graphs = defaultdict(list)
        for feature in features:
            offline_ingest_graphs = (
                feature.offline_store_info.extract_offline_store_ingest_query_graphs()
            )

            for offline_ingest_graph in offline_ingest_graphs:
                offline_store_table_name_to_features[
                    offline_ingest_graph.offline_store_table_name
                ].append(feature)
                offline_store_table_name_to_graphs[
                    offline_ingest_graph.offline_store_table_name
                ].append(offline_ingest_graph)

        return cls(
            offline_store_table_name_to_features=offline_store_table_name_to_features,
            offline_store_table_name_to_graphs=offline_store_table_name_to_graphs,
        )

    def get_offline_ingest_graphs(
        self, feature_table_name: str
    ) -> List[OfflineStoreIngestQueryGraph]:
        """
        Get offline ingest graphs by offline store feature table name

        Parameters
        ----------
        feature_table_name: str
            Offline store feature table name

        Returns
        -------
        List[OfflineStoreIngestQueryGraph]
        """
        return self.offline_store_table_name_to_graphs[feature_table_name]

    def iterate_features_by_table_name(self) -> Iterable[Tuple[str, List[FeatureModel]]]:
        """
        Iterate features by offline store feature table name

        Yields
        ------
        Tuple[str, List[FeatureModel]]
            Tuple of offline store feature table name and list of features
        """
        for table_name, features in self.offline_store_table_name_to_features.items():
            yield table_name, features


class OfflineStoreFeatureTableManagerService:  # pylint: disable=too-many-instance-attributes
    """
    OfflineStoreFeatureTableManagerService class
    """

    def __init__(
        self,
        catalog_id: ObjectId,
        offline_store_feature_table_service: OfflineStoreFeatureTableService,
        feature_service: FeatureService,
        online_store_compute_query_service: OnlineStoreComputeQueryService,
        entity_service: EntityService,
        feature_materialize_service: FeatureMaterializeService,
        feature_materialize_scheduler_service: FeatureMaterializeSchedulerService,
        feast_registry_service: FeastRegistryService,
        feature_list_service: FeatureListService,
    ):
        self.catalog_id = catalog_id
        self.offline_store_feature_table_service = offline_store_feature_table_service
        self.feature_service = feature_service
        self.online_store_compute_query_service = online_store_compute_query_service
        self.entity_service = entity_service
        self.feature_materialize_service = feature_materialize_service
        self.feature_materialize_scheduler_service = feature_materialize_scheduler_service
        self.feast_registry_service = feast_registry_service
        self.feature_list_service = feature_list_service

    async def handle_online_enabled_features(self, features: List[FeatureModel]) -> None:
        """
        Handles the case where features are enabled for online serving by updating all affected
        feature tables.

        Parameters
        ----------
        features: List[FeatureModel]
            Features to be enabled for online serving
        """
        ingest_graph_container = await OfflineIngestGraphContainer.build(features)

        for (
            offline_store_table_name,
            offline_store_table_features,
        ) in ingest_graph_container.iterate_features_by_table_name():
            feature_table_dict = await self._get_compatible_existing_feature_table(
                table_name=offline_store_table_name,
            )

            if feature_table_dict is not None:
                # update existing table
                feature_ids = feature_table_dict["feature_ids"][:]
                feature_ids_set = set(feature_ids)
                for feature in offline_store_table_features:
                    if feature.id not in feature_ids_set:
                        feature_ids.append(feature.id)
                if feature_ids != feature_table_dict["feature_ids"]:
                    feature_table_model = await self._update_offline_store_feature_table(
                        feature_table_dict, feature_ids
                    )
                else:
                    feature_table_model = None
            else:
                # create new table
                offline_ingest_graph = ingest_graph_container.get_offline_ingest_graphs(
                    feature_table_name=offline_store_table_name
                )[0]
                feature_table_model = await self._construct_offline_store_feature_table_model(
                    feature_table_name=offline_store_table_name,
                    feature_ids=[feature.id for feature in offline_store_table_features],
                    primary_entity_ids=offline_ingest_graph.primary_entity_ids,
                    has_ttl=offline_ingest_graph.has_ttl,
                    feature_job_setting=offline_ingest_graph.feature_job_setting,
                )
                await self.offline_store_feature_table_service.create_document(feature_table_model)

            if feature_table_model is not None:
                await self._create_or_update_feast_registry()
                await self.feature_materialize_service.initialize_new_columns(feature_table_model)
                await self.feature_materialize_scheduler_service.start_job_if_not_exist(
                    feature_table_model
                )

    async def handle_online_disabled_features(self, features: List[FeatureModel]) -> None:
        """
        Handles the case where a feature is disabled for online serving by updating all affected
        feature tables. In normal case there should only be one.

        Parameters
        ----------
        features: FeatureModel
            Model of the feature to be disabled for online serving
        """
        feature_ids_to_remove = {feature.id for feature in features}
        feature_table_data = await self.offline_store_feature_table_service.list_documents_as_dict(
            query_filter={"feature_ids": {"$in": list(feature_ids_to_remove)}},
        )
        for feature_table_dict in feature_table_data["data"]:
            updated_feature_ids = [
                feature_id
                for feature_id in feature_table_dict["feature_ids"]
                if feature_id not in feature_ids_to_remove
            ]
            if updated_feature_ids:
                updated_feature_table = await self._update_offline_store_feature_table(
                    feature_table_dict, updated_feature_ids
                )
                await self.feature_materialize_service.drop_columns(
                    updated_feature_table, self._get_offline_store_feature_table_columns(features)
                )
            else:
                await self.feature_materialize_service.drop_table(
                    await self.offline_store_feature_table_service.get_document(
                        feature_table_dict["_id"],
                    )
                )
                await self.feature_materialize_scheduler_service.stop_job(
                    feature_table_dict["_id"],
                )
                await self.offline_store_feature_table_service.delete_document(
                    document_id=feature_table_dict["_id"],
                )
            await self._create_or_update_feast_registry()

    @staticmethod
    def _get_offline_store_feature_table_columns(features: List[FeatureModel]) -> List[str]:
        for feature_model in features:
            info = feature_model.offline_store_info
            assert info is not None
            output_column_names = []
            for ingest_query_graph in info.extract_offline_store_ingest_query_graphs():
                output_column_names.append(ingest_query_graph.output_column_name)
            return output_column_names
        return []

    async def _get_compatible_existing_feature_table(
        self, table_name: str
    ) -> Optional[Dict[str, Any]]:
        feature_table_data = await self.offline_store_feature_table_service.list_documents_as_dict(
            query_filter={"name": table_name},
        )
        if feature_table_data["total"] > 0:
            return cast(Dict[str, Any], feature_table_data["data"][0])
        return None

    async def _update_offline_store_feature_table(
        self, feature_table_dict: Dict[str, Any], updated_feature_ids: List[ObjectId]
    ) -> OfflineStoreFeatureTableModel:
        feature_table_model = await self._construct_offline_store_feature_table_model(
            feature_table_name=feature_table_dict["name"],
            feature_ids=updated_feature_ids,
            primary_entity_ids=feature_table_dict["primary_entity_ids"],
            has_ttl=feature_table_dict["has_ttl"],
            feature_job_setting=FeatureJobSetting(**feature_table_dict["feature_job_setting"]),
        )
        update_schema = FeaturesUpdate(**feature_table_model.dict())
        return cast(
            OfflineStoreFeatureTableModel,
            await self.offline_store_feature_table_service.update_document(
                document_id=feature_table_dict["_id"], data=update_schema
            ),
        )

    async def _construct_offline_store_feature_table_model(
        self,
        feature_table_name: str,
        feature_ids: List[ObjectId],
        primary_entity_ids: List[PydanticObjectId],
        has_ttl: bool,
        feature_job_setting: Optional[FeatureJobSetting],
    ) -> OfflineStoreFeatureTableModel:
        feature_ids_to_model: Dict[ObjectId, FeatureModel] = {}
        async for feature_model in self.feature_service.list_documents_iterator(
            query_filter={"_id": {"$in": feature_ids}}
        ):
            feature_ids_to_model[feature_model.id] = feature_model

        primary_entities = []
        async for entity_model in self.entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": primary_entity_ids}}
        ):
            primary_entities.append(entity_model)

        required_aggregate_result_tables = await self._get_required_aggregate_result_tables(
            feature_id_to_models=feature_ids_to_model,
            primary_entity_serving_names=[entity.serving_names[0] for entity in primary_entities],
        )

        return get_offline_store_feature_table_model(
            feature_table_name=feature_table_name,
            features=[feature_ids_to_model[feature_id] for feature_id in feature_ids],
            aggregate_result_table_names=required_aggregate_result_tables,
            primary_entities=primary_entities,
            has_ttl=has_ttl,
            feature_job_setting=feature_job_setting,
        )

    async def _get_required_aggregate_result_tables(
        self,
        feature_id_to_models: Dict[ObjectId, FeatureModel],
        primary_entity_serving_names: List[str],
    ) -> List[str]:
        aggregate_result_table_names = set()
        for feature_model in feature_id_to_models.values():
            aggregate_result_table_names.update(feature_model.online_store_table_names)
        aggregate_result_table_names = list(aggregate_result_table_names)  # type: ignore[assignment]

        # Get aggregate result tables
        filtered_table_names = []
        if primary_entity_serving_names:
            query_filter = {
                "table_name": {"$in": aggregate_result_table_names},
                "serving_names": {
                    "$all": primary_entity_serving_names,
                    "$size": len(primary_entity_serving_names),
                },
            }
        else:
            query_filter = {
                "table_name": {"$in": aggregate_result_table_names},
            }

        async for online_store_compute_query_model in self.online_store_compute_query_service.list_documents_iterator(
            query_filter=query_filter
        ):
            filtered_table_names.append(online_store_compute_query_model.table_name)

        return sorted(set(filtered_table_names))

    async def _create_or_update_feast_registry(self) -> None:
        feature_lists = []
        async for feature_list_dict in self.feature_list_service.iterate_online_enabled_feature_lists_as_dict():
            feature_lists.append(FeatureListModel(**feature_list_dict))

        feast_registry = await self.feast_registry_service.get_feast_registry_for_catalog()
        if feast_registry is None:
            await self.feast_registry_service.create_document(
                FeastRegistryCreate(project_name=str(self.catalog_id), feature_lists=feature_lists)
            )
        else:
            await self.feast_registry_service.update_document(
                document_id=feast_registry.id,
                data=FeastRegistryUpdate(feature_lists=feature_lists),
            )
