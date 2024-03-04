"""
OfflineStoreFeatureTableUpdateService class
"""
from __future__ import annotations

from typing import Any, Callable, Coroutine, Dict, Iterable, List, Optional, Tuple, Union, cast

from collections import defaultdict
from dataclasses import dataclass

from bson import ObjectId

from featurebyte.common.progress import get_ranged_progress_callback
from featurebyte.feast.model.registry import FeastRegistryModel
from featurebyte.feast.schema.registry import FeastRegistryCreate, FeastRegistryUpdate
from featurebyte.feast.service.registry import FeastRegistryService
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.offline_store_feature_table import (
    FeaturesUpdate,
    OfflineStoreFeatureTableModel,
)
from featurebyte.models.offline_store_ingest_query import OfflineStoreIngestQueryGraph
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.service.catalog import CatalogService
from featurebyte.service.entity import EntityService
from featurebyte.service.entity_lookup_feature_table import EntityLookupFeatureTableService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_materialize import FeatureMaterializeService
from featurebyte.service.feature_materialize_scheduler import FeatureMaterializeSchedulerService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService
from featurebyte.service.offline_store_feature_table_comment import (
    ColumnComment,
    OfflineStoreFeatureTableCommentService,
    TableComment,
)
from featurebyte.service.offline_store_feature_table_construction import (
    OfflineStoreFeatureTableConstructionService,
)
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
        # Group features by offline store feature table name
        offline_store_table_name_to_feature_ids: dict[str, set[ObjectId]] = defaultdict(set)
        offline_store_table_name_to_features = defaultdict(list)
        offline_store_table_name_to_graphs = defaultdict(list)
        for feature in features:
            offline_ingest_graphs = (
                feature.offline_store_info.extract_offline_store_ingest_query_graphs()
            )

            for offline_ingest_graph in offline_ingest_graphs:
                table_name = offline_ingest_graph.offline_store_table_name
                if feature.id not in offline_store_table_name_to_feature_ids[table_name]:
                    offline_store_table_name_to_feature_ids[table_name].add(feature.id)
                    offline_store_table_name_to_features[table_name].append(feature)
                offline_store_table_name_to_graphs[table_name].append(offline_ingest_graph)

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


class OfflineStoreFeatureTableManagerService:  # pylint: disable=too-many-instance-attributes,too-many-arguments
    """
    OfflineStoreFeatureTableManagerService class
    """

    def __init__(
        self,
        catalog_id: ObjectId,
        catalog_service: CatalogService,
        feature_store_service: FeatureStoreService,
        offline_store_feature_table_service: OfflineStoreFeatureTableService,
        offline_store_feature_table_construction_service: OfflineStoreFeatureTableConstructionService,
        offline_store_feature_table_comment_service: OfflineStoreFeatureTableCommentService,
        feature_service: FeatureService,
        online_store_compute_query_service: OnlineStoreComputeQueryService,
        entity_service: EntityService,
        feature_materialize_service: FeatureMaterializeService,
        feature_materialize_scheduler_service: FeatureMaterializeSchedulerService,
        feast_registry_service: FeastRegistryService,
        feature_list_service: FeatureListService,
        entity_lookup_feature_table_service: EntityLookupFeatureTableService,
    ):
        self.catalog_id = catalog_id
        self.catalog_service = catalog_service
        self.feature_store_service = feature_store_service
        self.offline_store_feature_table_service = offline_store_feature_table_service
        self.offline_store_feature_table_construction_service = (
            offline_store_feature_table_construction_service
        )
        self.offline_store_feature_table_comment_service = (
            offline_store_feature_table_comment_service
        )
        self.feature_service = feature_service
        self.online_store_compute_query_service = online_store_compute_query_service
        self.entity_service = entity_service
        self.feature_materialize_service = feature_materialize_service
        self.feature_materialize_scheduler_service = feature_materialize_scheduler_service
        self.feast_registry_service = feast_registry_service
        self.feature_list_service = feature_list_service
        self.entity_lookup_feature_table_service = entity_lookup_feature_table_service

    async def handle_online_enabled_features(
        self,
        features: List[FeatureModel],
        update_progress: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
    ) -> None:
        """
        Handles the case where features are enabled for online serving by updating all affected
        feature tables.

        Parameters
        ----------
        features: List[FeatureModel]
            Features to be enabled for online serving
        update_progress: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]
            Optional callback to update progress
        """
        ingest_graph_container = await OfflineIngestGraphContainer.build(features)
        feature_lists = await self._get_online_enabled_feature_lists()
        # Refresh feast registry since it's needed for when materializing the features from offline
        # store feature tables to online store
        await self._create_or_update_feast_registry(feature_lists)
        offline_table_count = len(ingest_graph_container.offline_store_table_name_to_features)

        new_tables = []
        for idx, (
            offline_store_table_name,
            offline_store_table_features,
        ) in enumerate(ingest_graph_container.iterate_features_by_table_name()):
            feature_table_dict = await self._get_compatible_existing_feature_table(
                table_name=offline_store_table_name,
            )
            assert feature_table_dict is not None, f"{offline_store_table_name} not found"

            if len(feature_table_dict["feature_ids"]) == 0:
                # add to new tables as the original table is empty
                new_tables.append(OfflineStoreFeatureTableModel(**feature_table_dict))

            # update existing table
            feature_ids = feature_table_dict["feature_ids"][:]
            feature_ids_set = set(feature_ids)
            for feature in offline_store_table_features:
                if feature.id not in feature_ids_set:
                    feature_ids.append(feature.id)
            if feature_ids != feature_table_dict["feature_ids"]:
                feature_table_model = await self._update_offline_store_feature_table(
                    feature_table_dict,
                    feature_ids,
                )
            else:
                # Note: don't set feature_table_model here since we don't want to run
                # materialize below
                feature_table_model = None

            if update_progress:
                await update_progress(
                    int((idx + 1) / offline_table_count * 60),
                    f"Materializing features to online store for table {offline_store_table_name}",
                )

            if feature_table_model is not None:
                await self.feature_materialize_service.initialize_new_columns(feature_table_model)
                await self.feature_materialize_scheduler_service.start_job_if_not_exist(
                    feature_table_model
                )

        feature_store_model = await self._get_feature_store_model()
        new_tables.extend(
            await self._create_or_update_entity_lookup_feature_tables(
                feature_lists,
                feature_store_model,
                get_ranged_progress_callback(update_progress, 60, 90) if update_progress else None,
            )
        )

        # Add comments to newly created tables and columns
        await self._update_table_and_column_comments(
            new_tables=new_tables,
            new_features=features,
            feature_store_model=feature_store_model,
            update_progress=get_ranged_progress_callback(update_progress, 90, 100)
            if update_progress
            else None,
        )

    async def handle_online_disabled_features(
        self,
        features: List[FeatureModel],
        update_progress: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
    ) -> None:
        """
        Handles the case where a feature is disabled for online serving by updating all affected
        feature tables. In normal case there should only be one.

        Parameters
        ----------
        features: FeatureModel
            Model of the feature to be disabled for online serving
        update_progress: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]
            Optional callback to update progress
        """
        feature_ids_to_remove = {feature.id for feature in features}
        feature_table_data = await self.offline_store_feature_table_service.list_documents_as_dict(
            query_filter={"feature_ids": {"$in": list(feature_ids_to_remove)}},
        )
        feature_lists = await self._get_online_enabled_feature_lists()
        offline_table_count = len(feature_table_data["data"])

        for idx, feature_table_dict in enumerate(feature_table_data["data"]):
            updated_feature_ids = [
                feature_id
                for feature_id in feature_table_dict["feature_ids"]
                if feature_id not in feature_ids_to_remove
            ]
            if updated_feature_ids:
                updated_feature_table = await self._update_offline_store_feature_table(
                    feature_table_dict,
                    updated_feature_ids,
                )
                await self.feature_materialize_service.drop_columns(
                    updated_feature_table, self._get_offline_store_feature_table_columns(features)
                )
            else:
                await self._delete_offline_store_feature_table(feature_table_dict["_id"])

            if update_progress:
                await update_progress(
                    int((idx + 1) / offline_table_count * 90),
                    f"Updating offline store feature table {feature_table_dict['name']} for online disabling features",
                )

        await self._create_or_update_feast_registry(feature_lists)
        feature_store_model = await self._get_feature_store_model()
        await self._delete_entity_lookup_feature_tables(feature_lists, feature_store_model)
        if update_progress:
            await update_progress(100, "Updated entity lookup feature tables")

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
        self,
        feature_table_dict: Dict[str, Any],
        updated_feature_ids: List[ObjectId],
    ) -> OfflineStoreFeatureTableModel:
        feature_job_setting = None
        if feature_table_dict["feature_job_setting"]:
            feature_job_setting = FeatureJobSetting(**feature_table_dict["feature_job_setting"])
        feature_table_model = await self._construct_offline_store_feature_table_model(
            feature_table_name=feature_table_dict["name"],
            feature_ids=updated_feature_ids,
            primary_entity_ids=feature_table_dict["primary_entity_ids"],
            has_ttl=feature_table_dict["has_ttl"],
            feature_job_setting=feature_job_setting,
        )
        update_schema = FeaturesUpdate(**feature_table_model.dict(by_alias=True))
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

        primary_entities_mapping = {}
        async for entity_model in self.entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": primary_entity_ids}}
        ):
            primary_entities_mapping[entity_model.id] = entity_model
        primary_entities = [
            primary_entities_mapping[primary_entity_id] for primary_entity_id in primary_entity_ids
        ]

        required_aggregate_result_tables = await self._get_required_aggregate_result_tables(
            feature_id_to_models=feature_ids_to_model,
            primary_entity_serving_names=[entity.serving_names[0] for entity in primary_entities],
        )

        catalog_model = await self.catalog_service.get_document(self.catalog_id)
        feature_store_model = await self.feature_store_service.get_document(
            catalog_model.default_feature_store_ids[0]
        )

        return await self.offline_store_feature_table_construction_service.get_offline_store_feature_table_model(
            feature_table_name=feature_table_name,
            features=[feature_ids_to_model[feature_id] for feature_id in feature_ids],
            aggregate_result_table_names=required_aggregate_result_tables,
            primary_entities=primary_entities,
            has_ttl=has_ttl,
            feature_job_setting=feature_job_setting,
            source_type=feature_store_model.type,
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

    async def _get_online_enabled_feature_lists(self) -> List[FeatureListModel]:
        feature_lists = []
        async for feature_list_dict in self.feature_list_service.iterate_online_enabled_feature_lists_as_dict():
            feature_lists.append(FeatureListModel(**feature_list_dict))
        return feature_lists

    async def _create_or_update_feast_registry(
        self, feature_lists: List[FeatureListModel]
    ) -> FeastRegistryModel:
        feast_registry = await self.feast_registry_service.get_feast_registry_for_catalog()
        if feast_registry is None:
            return await self.feast_registry_service.create_document(
                FeastRegistryCreate(feature_lists=feature_lists)
            )

        output = await self.feast_registry_service.update_document(
            document_id=feast_registry.id,
            data=FeastRegistryUpdate(feature_lists=feature_lists),
        )
        assert output is not None
        return output

    async def _create_or_update_entity_lookup_feature_tables(
        self,
        feature_lists: List[FeatureListModel],
        feature_store_model: FeatureStoreModel,
        update_progress: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
    ) -> List[OfflineStoreFeatureTableModel]:
        """
        Create or update feature tables to support parent entities lookup for a specific offline
        feature table. This should be called when online enabling features.

        Parameters
        ----------
        feature_lists: List[FeatureListModel]
            Currently online enabled feature lists
        feature_store_model: FeatureStoreModel
            Feature store model
        update_progress: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]
            Optional callback to update progress

        Returns
        -------
        List[OfflineStoreFeatureTableModel]
        """
        # Get list of entity lookup feature tables that are currently required
        service = self.entity_lookup_feature_table_service
        entity_lookup_feature_table_models = await service.get_entity_lookup_feature_tables(
            feature_lists, feature_store_model
        )
        if entity_lookup_feature_table_models is None:
            entity_lookup_feature_table_models = []

        # Commission tables that should exist if they don't already exist
        existing_lookup_feature_tables = {
            feature_table_dict["name"]: feature_table_dict
            async for feature_table_dict in self.offline_store_feature_table_service.list_documents_as_dict_iterator(
                query_filter={"entity_lookup_info": {"$ne": None}}, projection={"_id": 1, "name": 1}
            )
        }
        new_tables = []
        for idx, entity_lookup_feature_table in enumerate(entity_lookup_feature_table_models):
            if update_progress:
                await update_progress(
                    int(idx / len(entity_lookup_feature_table_models) * 100),
                    f"Materializing entity look up table {entity_lookup_feature_table.name}",
                )

            if entity_lookup_feature_table.name not in existing_lookup_feature_tables:
                entity_lookup_feature_table = (
                    await self.offline_store_feature_table_service.create_document(
                        entity_lookup_feature_table
                    )
                )
                await self.feature_materialize_service.initialize_new_columns(
                    entity_lookup_feature_table
                )
                await self.feature_materialize_scheduler_service.start_job_if_not_exist(
                    entity_lookup_feature_table
                )
                new_tables.append(entity_lookup_feature_table)

        return new_tables

    async def _delete_entity_lookup_feature_tables(
        self, feature_lists: List[FeatureListModel], feature_store_model: FeatureStoreModel
    ) -> None:
        """
        Delete offline store feature tables that were created for entity lookup purpose but are not
        no longer required. This should be called when online disabling features.

        Parameters
        ----------
        feature_lists: List[FeatureListModel]
            Currently online enabled feature lists
        feature_store_model: FeatureStoreModel
            Feature store
        """
        # Get list of entity lookup feature tables that are currently required
        current_active_table_names = set()
        service = self.entity_lookup_feature_table_service
        entity_lookup_feature_table_models = await service.get_entity_lookup_feature_tables(
            feature_lists, feature_store_model
        )
        if entity_lookup_feature_table_models is not None:
            for table in entity_lookup_feature_table_models:
                current_active_table_names.add(table.name)

        # Decommission tables that should no longer exist
        async for feature_table_dict in self.offline_store_feature_table_service.list_documents_as_dict_iterator(
            query_filter={"entity_lookup_info": {"$ne": None}}, projection={"_id": 1, "name": 1}
        ):
            if feature_table_dict["name"] not in current_active_table_names:
                await self._delete_offline_store_feature_table(feature_table_dict["_id"])

    async def _delete_offline_store_feature_table(self, feature_table_id: ObjectId) -> None:
        await self.feature_materialize_service.drop_table(
            await self.offline_store_feature_table_service.get_document(
                feature_table_id,
            )
        )
        await self.feature_materialize_scheduler_service.stop_job(
            feature_table_id,
        )
        await self.offline_store_feature_table_service.delete_document(feature_table_id)

    async def _get_feature_store_model(self) -> FeatureStoreModel:
        catalog_model = await self.catalog_service.get_document(self.catalog_id)
        feature_store_model = await self.feature_store_service.get_document(
            catalog_model.default_feature_store_ids[0]
        )
        return feature_store_model

    async def _update_table_and_column_comments(
        self,
        new_tables: List[OfflineStoreFeatureTableModel],
        new_features: List[FeatureModel],
        feature_store_model: FeatureStoreModel,
        update_progress: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
    ) -> None:
        comments: List[Union[TableComment, ColumnComment]] = []
        for feature_table_model in new_tables:
            comments.append(
                await self.offline_store_feature_table_comment_service.generate_table_comment(
                    feature_table_model,
                )
            )
        comments.extend(
            await self.offline_store_feature_table_comment_service.generate_column_comments(
                new_features
            )
        )
        await self.offline_store_feature_table_comment_service.apply_comments(
            feature_store_model, comments, update_progress
        )
