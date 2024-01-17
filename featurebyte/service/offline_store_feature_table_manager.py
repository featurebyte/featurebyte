"""
OfflineStoreFeatureTableUpdateService class
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, cast

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
)
from featurebyte.models.offline_store_ingest_query import (
    OfflineFeatureTableSignature,
    OfflineStoreIngestQueryGraph,
)
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

    table_signature_to_features: Dict[OfflineFeatureTableSignature, List[FeatureModel]]
    table_signature_to_graphs: Dict[
        OfflineFeatureTableSignature, List[OfflineStoreIngestQueryGraph]
    ]

    @classmethod
    async def build(
        cls,
        features: List[FeatureModel],
        entity_id_to_serving_name: Dict[PydanticObjectId, str],
    ) -> OfflineIngestGraphContainer:
        """
        Build OfflineIngestGraphContainer

        Parameters
        ----------
        features : List[FeatureModel]
            List of features
        entity_id_to_serving_name : Dict[PydanticObjectId, str]
            Mapping from entity id to serving name

        Returns
        -------
        OfflineIngestGraphContainer
        """
        # Build mapping from entity id to serving names needed by offline store info extraction
        all_feature_entity_ids = set()
        for feature in features:
            all_feature_entity_ids.update(feature.entity_ids)

        # Group features by offline store feature table name
        table_signature_to_feature_ids: dict[
            OfflineFeatureTableSignature, set[ObjectId]
        ] = defaultdict(set)
        table_signature_to_features = defaultdict(list)
        table_signature_to_graphs = defaultdict(list)
        for feature in features:
            feature_store_id = feature.tabular_source.feature_store_id
            store_info = feature.offline_store_info

            for offline_ingest_graph in store_info.extract_offline_store_ingest_query_graphs():
                table_signature = offline_ingest_graph.get_full_table_signature(
                    feature_store_id=feature_store_id,
                    catalog_id=feature.catalog_id,
                    entity_id_to_serving_name=entity_id_to_serving_name,
                )

                if feature.id not in table_signature_to_feature_ids[table_signature]:
                    table_signature_to_feature_ids[table_signature].add(feature.id)
                    table_signature_to_features[table_signature].append(feature)
                table_signature_to_graphs[table_signature].append(offline_ingest_graph)

                table_signature = offline_ingest_graph.table_signature
                table_signature["catalog_id"] = feature.catalog_id

        return cls(
            table_signature_to_features=table_signature_to_features,
            table_signature_to_graphs=table_signature_to_graphs,
        )

    def get_offline_ingest_graphs(
        self, table_signature: OfflineFeatureTableSignature
    ) -> List[OfflineStoreIngestQueryGraph]:
        """
        Get offline ingest graphs by offline store feature table name

        Parameters
        ----------
        table_signature : OfflineFeatureTableSignature
            Offline store feature table signature

        Returns
        -------
        List[OfflineStoreIngestQueryGraph]
        """
        return self.table_signature_to_graphs[table_signature]

    def iterate_features_by_table_signature(
        self,
    ) -> Iterable[Tuple[OfflineFeatureTableSignature, List[FeatureModel]]]:
        """
        Iterate features by offline store feature table signature

        Yields
        ------
        Tuple[OfflineFeatureTableSignature, List[FeatureModel]]
            Tuple of offline store feature table signature and list of features
        """
        for table_name, features in self.table_signature_to_features.items():
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

    async def handle_online_enabled_features(self, features: List[FeatureModel]) -> None:
        """
        Handles the case where features are enabled for online serving by updating all affected
        feature tables.

        Parameters
        ----------
        features: List[FeatureModel]
            Features to be enabled for online serving
        """
        entity_ids = set()
        for feature in features:
            entity_ids.update(feature.entity_ids)
        feature_lists = await self._get_online_enabled_feature_lists()

        entity_id_to_serving_name = {}
        async for entity_model in self.entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(entity_ids)}}
        ):
            entity_id_to_serving_name[entity_model.id] = entity_model.serving_names[0]

        container = await OfflineIngestGraphContainer.build(features, entity_id_to_serving_name)
        new_tables = []
        table_init_flag_pairs = []
        for table_signature, features in container.iterate_features_by_table_signature():
            feature_table_dict = await self._get_compatible_existing_feature_table(
                table_signature=table_signature
            )
            if feature_table_dict is not None:
                # update existing table
                feature_ids = feature_table_dict["feature_ids"][:]
                feature_ids_set = set(feature_ids)
                for feature in features:
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
            else:
                # create new table
                offline_ingest_graph = container.get_offline_ingest_graphs(
                    table_signature=table_signature
                )[0]
                feature_table_model = await self._construct_offline_store_feature_table_model(
                    feature_ids=[feature.id for feature in features],
                    primary_entity_ids=offline_ingest_graph.primary_entity_ids,
                    has_ttl=offline_ingest_graph.has_ttl,
                    feature_job_setting=offline_ingest_graph.feature_job_setting,
                )
                await self.offline_store_feature_table_service.create_document(feature_table_model)
                new_tables.append(feature_table_model)

            if feature_table_model is None:
                feature_table_model = await self.offline_store_feature_table_service.get_document(
                    feature_table_dict["_id"],
                )
            table_init_flag_pairs.append((feature_table_model, feature_table_dict is None))

            new_tables.extend(
                await self._create_or_update_entity_lookup_feature_tables(
                    feature_table_model, feature_lists
                )
            )

        # Add comments to newly created tables and columns
        await self._update_table_and_column_comments(
            new_tables=new_tables,
            new_features=features,
        )

        # Initialize new columns and start materialization jobs for new tables
        for table, init_flag in table_init_flag_pairs:
            if init_flag:
                await self.feature_materialize_service.initialize_new_columns(table)
                await self.feature_materialize_scheduler_service.start_job_if_not_exist(table)

            await self._create_or_update_entity_lookup_feature_tables(table, feature_lists)

        # If all the features are already online enabled, i.e. the offline feature store tables
        # are up-to-date. But registry still needs to be updated because there could be a new
        # feature service that needs to be created.
        await self._create_or_update_feast_registry(feature_lists)

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
        feature_lists = await self._get_online_enabled_feature_lists()
        for feature_table_dict in feature_table_data["data"]:
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
        await self._create_or_update_feast_registry(feature_lists)
        await self._delete_entity_lookup_feature_tables(feature_lists)

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
        self, table_signature: OfflineFeatureTableSignature
    ) -> Optional[Dict[str, Any]]:
        feature_table_data = await self.offline_store_feature_table_service.list_documents_as_dict(
            query_filter=table_signature.dict(),
        )
        if feature_table_data["total"] > 0:
            return cast(Dict[str, Any], feature_table_data["data"][0])
        return None

    async def _update_offline_store_feature_table(
        self,
        feature_table_dict: Dict[str, Any],
        updated_feature_ids: List[ObjectId],
    ) -> OfflineStoreFeatureTableModel:
        feature_table_model = await self._construct_offline_store_feature_table_model(
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

        catalog_model = await self.catalog_service.get_document(self.catalog_id)
        feature_store_model = await self.feature_store_service.get_document(
            catalog_model.default_feature_store_ids[0]
        )

        return await self.offline_store_feature_table_construction_service.get_offline_store_feature_table_model(
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

    async def _create_or_update_feast_registry(self, feature_lists: List[FeatureListModel]) -> None:
        if not await self.feast_registry_service.is_source_type_supported():
            return

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

    async def _create_or_update_entity_lookup_feature_tables(
        self,
        feature_table_model: OfflineStoreFeatureTableModel,
        feature_lists: List[FeatureListModel],
    ) -> List[OfflineStoreFeatureTableModel]:
        """
        Create or update feature tables to support parent entities lookup for a specific offline
        feature table. This should be called when online enabling features.

        Parameters
        ----------
        feature_table_model: OfflineStoreFeatureTableModel
            Offline store feature table model to check
        feature_lists: List[FeatureListModel]
            Currently online enabled feature lists

        Returns
        -------
        List[OfflineStoreFeatureTableModel]
        """
        # Get list of entity lookup feature tables that are currently required
        service = self.entity_lookup_feature_table_service
        entity_lookup_feature_table_models = await service.get_entity_lookup_feature_tables(
            feature_table_model, feature_lists
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
        for entity_lookup_feature_table in entity_lookup_feature_table_models:
            if entity_lookup_feature_table.name not in existing_lookup_feature_tables:
                await self.offline_store_feature_table_service.create_document(
                    entity_lookup_feature_table
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
        self, feature_lists: List[FeatureListModel]
    ) -> None:
        """
        Delete offline store feature tables that were created for entity lookup purpose but are not
        no longer required. This should be called when online disabling features.

        Parameters
        ----------
        feature_lists: List[FeatureListModel]
            Currently online enabled feature lists
        """
        # Get list of entity lookup feature tables that are currently required
        current_active_table_names = set()
        service = self.entity_lookup_feature_table_service
        async for feature_table_model in self.offline_store_feature_table_service.list_documents_iterator(
            query_filter={"entity_lookup_info": {"$eq": None}}
        ):
            entity_lookup_feature_table_models = await service.get_entity_lookup_feature_tables(
                feature_table_model, feature_lists
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

    async def _update_table_and_column_comments(
        self,
        new_tables: List[OfflineStoreFeatureTableModel],
        new_features: List[FeatureModel],
    ) -> None:
        catalog_model = await self.catalog_service.get_document(self.catalog_id)
        feature_store_model = await self.feature_store_service.get_document(
            catalog_model.default_feature_store_ids[0]
        )
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
            feature_store_model, comments
        )
