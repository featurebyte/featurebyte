"""
OfflineStoreFeatureTableUpdateService class
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, Iterable, List, Optional, Tuple, Union, cast

from bson import ObjectId
from pydantic import TypeAdapter

from featurebyte.common.progress import get_ranged_progress_callback
from featurebyte.common.utils import timer
from featurebyte.feast.model.registry import FeastRegistryModel
from featurebyte.feast.schema.registry import FeastRegistryCreate, FeastRegistryUpdate
from featurebyte.feast.service.registry import FeastRegistryService
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.entity import EntityModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.offline_store_feature_table import (
    FeaturesUpdate,
    OfflineStoreFeatureTableModel,
)
from featurebyte.models.offline_store_ingest_query import OfflineStoreIngestQueryGraph
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSettingUnion
from featurebyte.service.catalog import CatalogService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.entity import EntityService
from featurebyte.service.entity_lookup_feature_table import EntityLookupFeatureTableService
from featurebyte.service.exception import OfflineStoreFeatureTableBadStateError
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

logger = get_logger(__name__)


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


class OfflineStoreFeatureTableManagerService:
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
        entity_service: EntityService,
        feature_materialize_service: FeatureMaterializeService,
        feature_materialize_scheduler_service: FeatureMaterializeSchedulerService,
        feast_registry_service: FeastRegistryService,
        feature_list_service: FeatureListService,
        entity_lookup_feature_table_service: EntityLookupFeatureTableService,
        deployment_service: DeploymentService,
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
        self.entity_service = entity_service
        self.feature_materialize_service = feature_materialize_service
        self.feature_materialize_scheduler_service = feature_materialize_scheduler_service
        self.feast_registry_service = feast_registry_service
        self.feature_list_service = feature_list_service
        self.entity_lookup_feature_table_service = entity_lookup_feature_table_service
        self.deployment_service = deployment_service

    async def handle_online_enabled_features(
        self,
        features: List[FeatureModel],
        feature_list_to_online_enable: FeatureListModel,
        deployment: DeploymentModel,
        update_progress: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
    ) -> None:
        """
        Handles the case where features are enabled for online serving by updating all affected
        feature tables.

        Parameters
        ----------
        features: List[FeatureModel]
            Features to be enabled for online serving
        feature_list_to_online_enable: FeatureListModel
            Feature list model to deploy (may not be enabled yet)
        deployment: DeploymentModel
            Deployment ID
        update_progress: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]
            Optional callback to update progress
        """
        await self._delete_deprecated_entity_lookup_feature_tables()

        ingest_graph_container = await OfflineIngestGraphContainer.build(features)
        feature_lists = await self._get_online_enabled_feature_lists(
            feature_list_to_online_enable=feature_list_to_online_enable
        )
        # Refresh feast registry since it's needed for when materializing the features from offline
        # store feature tables to online store
        await self._create_or_update_feast_registry(
            feature_lists=feature_lists,
            active_feature_list=feature_list_to_online_enable,
            deployment=deployment,
            to_enable=True,
        )
        offline_table_count = len(ingest_graph_container.offline_store_table_name_to_features)
        feature_store_model = await self._get_feature_store_model()

        # Update precomputed lookup feature tables for existing features
        await self._update_precomputed_lookup_feature_tables_enable_deployment(
            feature_list_to_online_enable,
            feature_store_model,
            deployment,
        )

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

            # Initialize precomputed lookup feature tables using updated feature_ids
            await self._update_precomputed_lookup_feature_table_enable_deployment(
                feature_table_dict["_id"],
                feature_ids,
                feature_list_to_online_enable,
                feature_store_model,
                deployment,
            )
            await self.offline_store_feature_table_service.add_deployment_id(
                document_id=feature_table_dict["_id"], deployment_id=deployment.id
            )

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
                message = (
                    f"Materializing features to online store for table {offline_store_table_name} "
                    f"({idx + 1} / {offline_table_count} tables)"
                )
                await update_progress(int((idx + 1) / offline_table_count * 90), message)

            if feature_table_model is not None:
                await self.feature_materialize_service.initialize_new_columns(feature_table_model)
                await self.feature_materialize_scheduler_service.start_job_if_not_exist(
                    feature_table_model
                )

        # Add comments to newly created tables and columns
        await self._update_table_and_column_comments(
            new_tables=new_tables,
            new_features=features,
            feature_store_model=feature_store_model,
            update_progress=(
                get_ranged_progress_callback(update_progress, 90, 100) if update_progress else None
            ),
        )

    async def handle_online_disabled_features(
        self,
        feature_list_to_online_disable: FeatureListModel,
        deployment: DeploymentModel,
        update_progress: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
    ) -> None:
        """
        Handles the case where a feature is disabled for online serving by updating all affected
        feature tables. In normal case there should only be one.

        Parameters
        ----------
        feature_list_to_online_disable: FeatureListModel
            Feature list model to not deploy (may not be disabled yet)
        deployment: DeploymentModel
            Deployment
        update_progress: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]
            Optional callback to update progress
        """
        await self._delete_deprecated_entity_lookup_feature_tables()

        feature_ids_to_remove = await self.feature_service.get_online_disabled_feature_ids()
        feature_table_data = await self.offline_store_feature_table_service.list_documents_as_dict(
            query_filter={
                "$and": [
                    {
                        "$or": [
                            {"feature_ids": {"$in": list(feature_ids_to_remove)}},
                            {"feature_ids": {"$size": 0}},
                        ]
                    },
                    {
                        # Filter out precomputed lookup feature tables since those always have empty
                        # feature_ids
                        "$or": [
                            {"precomputed_lookup_feature_table_info": None},
                            {"precomputed_lookup_feature_table_info": {"$exists": False}},
                        ],
                    },
                ]
            },
        )
        feature_lists = await self._get_online_enabled_feature_lists(
            feature_list_to_online_disable=feature_list_to_online_disable
        )
        offline_table_count = len(feature_table_data["data"])

        # Update precomputed lookup feature tables based on the deployment that is being disabled
        await self._update_precomputed_lookup_feature_tables_disable_deployment(
            deployment.id,
        )

        for idx, feature_table_dict in enumerate(feature_table_data["data"]):
            updated_feature_ids = [
                feature_id
                for feature_id in feature_table_dict["feature_ids"]
                if feature_id not in feature_ids_to_remove
            ]
            await self.offline_store_feature_table_service.remove_deployment_id(
                document_id=feature_table_dict["_id"], deployment_id=deployment.id
            )

            if updated_feature_ids:
                try:
                    updated_feature_table = await self._update_offline_store_feature_table(
                        feature_table_dict,
                        updated_feature_ids,
                    )
                except OfflineStoreFeatureTableBadStateError:
                    logger.error(
                        "Failed to update offline store feature table when disabling a deployment",
                        exc_info=True,
                        extra={"table_name": feature_table_dict["name"]},
                    )
                    await self._delete_offline_store_feature_table(feature_table_dict["_id"])
                    continue
                removed_feature_ids = list(
                    set(feature_table_dict["feature_ids"]) - set(updated_feature_ids)
                )
                removed_features = []
                async for feature in self.feature_service.list_documents_iterator({
                    "_id": {"$in": removed_feature_ids}
                }):
                    removed_features.append(feature)
                columns_to_drop = self._get_offline_store_feature_table_columns(removed_features)
                await self.feature_materialize_service.drop_columns(
                    updated_feature_table, columns_to_drop
                )
                async for lookup_feature_table in self.offline_store_feature_table_service.list_precomputed_lookup_feature_tables_from_source(
                    feature_table_dict["_id"]
                ):
                    await self.feature_materialize_service.drop_columns(
                        lookup_feature_table, columns_to_drop
                    )
            else:
                await self._delete_offline_store_feature_table(feature_table_dict["_id"])

            if update_progress:
                message = (
                    f"Updating offline store feature table {feature_table_dict['name']} for online disabling features "
                    f"({idx + 1} / {offline_table_count} tables)"
                )
                await update_progress(int((idx + 1) / offline_table_count * 90), message)

        await self._create_or_update_feast_registry(
            feature_lists=feature_lists,
            active_feature_list=feature_list_to_online_disable,
            deployment=deployment,
            to_enable=False,
        )
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
            feature_job_setting = TypeAdapter(FeatureJobSettingUnion).validate_python(
                feature_table_dict["feature_job_setting"]
            )
        feature_table_model = await self._construct_offline_store_feature_table_model(
            feature_table_name=feature_table_dict["name"],
            feature_ids=updated_feature_ids,
            primary_entity_ids=feature_table_dict["primary_entity_ids"],
            has_ttl=feature_table_dict["has_ttl"],
            feature_job_setting=feature_job_setting,
        )
        update_schema = FeaturesUpdate(**feature_table_model.model_dump(by_alias=True))
        return cast(
            OfflineStoreFeatureTableModel,
            await self.offline_store_feature_table_service.update_document(
                document_id=feature_table_dict["_id"], data=update_schema
            ),
        )

    async def _get_entities(
        self, entity_ids: Optional[List[PydanticObjectId]]
    ) -> List[EntityModel]:
        entities_mapping = {}
        query_filter = {} if entity_ids is None else {"_id": {"$in": entity_ids}}
        async for entity_model in self.entity_service.list_documents_iterator(
            query_filter=query_filter
        ):
            entities_mapping[entity_model.id] = entity_model
        if entity_ids is not None:
            # Preserve ordering of the provided entity_ids
            entities = [entities_mapping[entity_id] for entity_id in entity_ids]
        else:
            entities = list(entities_mapping.values())
        return entities

    async def _get_feature_ids_to_model(
        self,
        feature_ids: List[ObjectId],
    ) -> Dict[ObjectId, FeatureModel]:
        feature_ids_to_model: Dict[ObjectId, FeatureModel] = {}
        async for feature_model in self.feature_service.list_documents_iterator(
            query_filter={"_id": {"$in": feature_ids}}
        ):
            feature_ids_to_model[feature_model.id] = feature_model
        return feature_ids_to_model

    async def _construct_offline_store_feature_table_model(
        self,
        feature_table_name: str,
        feature_ids: List[ObjectId],
        primary_entity_ids: List[PydanticObjectId],
        has_ttl: bool,
        feature_job_setting: Optional[FeatureJobSettingUnion],
    ) -> OfflineStoreFeatureTableModel:
        feature_ids_to_model = await self._get_feature_ids_to_model(feature_ids)
        primary_entities = await self._get_entities(primary_entity_ids)

        catalog_model = await self.catalog_service.get_document(self.catalog_id)
        feature_store_model = await self.feature_store_service.get_document(
            catalog_model.default_feature_store_ids[0]
        )

        return await self.offline_store_feature_table_construction_service.get_offline_store_feature_table_model(
            feature_table_name=feature_table_name,
            features=[feature_ids_to_model[feature_id] for feature_id in feature_ids],
            primary_entities=primary_entities,
            has_ttl=has_ttl,
            feature_job_setting=feature_job_setting,
            source_info=feature_store_model.get_source_info(),
        )

    async def _get_online_enabled_feature_lists(
        self,
        feature_list_to_online_enable: Optional[FeatureListModel] = None,
        feature_list_to_online_disable: Optional[FeatureListModel] = None,
    ) -> List[FeatureListModel]:
        feature_lists = []
        fl_has_been_enabled = False
        async for (
            feature_list_dict
        ) in self.feature_list_service.iterate_online_enabled_feature_lists_as_dict():
            feature_list = FeatureListModel(**feature_list_dict)
            if (
                feature_list_to_online_disable
                and feature_list.id == feature_list_to_online_disable.id
            ):
                # skip the feature list to be disabled
                continue

            if feature_list.feast_enabled:
                feature_lists.append(feature_list)
                if (
                    feature_list_to_online_enable
                    and feature_list.id == feature_list_to_online_enable.id
                ):
                    fl_has_been_enabled = True

        if feature_list_to_online_enable and not fl_has_been_enabled:
            # add the feature list to be enabled
            feature_lists.append(feature_list_to_online_enable)
        return feature_lists

    async def _create_or_update_feast_registry(
        self,
        feature_lists: List[FeatureListModel],
        active_feature_list: FeatureListModel,
        deployment: DeploymentModel,
        to_enable: bool,
    ) -> Optional[FeastRegistryModel]:
        feast_registry = await self.feast_registry_service.get_feast_registry(deployment)
        if feast_registry is None:
            if to_enable:
                # create a new deployment specific feast registry
                return await self.feast_registry_service.create_document(
                    FeastRegistryCreate(
                        feature_lists=[active_feature_list], deployment_id=deployment.id
                    )
                )

            # no feast registry to update required for disabling a deployment that doesn't have feast registry
            return None

        if feast_registry.deployment_id is None:
            # for non-deployment specific feast registry, no need to update the feast registry using
            # existing online-enabled feature lists
            output = await self.feast_registry_service.update_document(
                document_id=feast_registry.id,
                data=FeastRegistryUpdate(feature_lists=feature_lists),
                populate_remote_attributes=False,
            )
            return output

        # for deployment specific feast registry, update the feast registry with specific feature list
        output = await self.feast_registry_service.update_document(
            document_id=feast_registry.id,
            data=FeastRegistryUpdate(
                feature_lists=[active_feature_list] if to_enable else [],
            ),
            populate_remote_attributes=False,
        )
        return output

    async def _update_precomputed_lookup_feature_tables_enable_deployment(
        self,
        active_feature_list: FeatureListModel,
        feature_store_model: FeatureStoreModel,
        deployment: DeploymentModel,
    ) -> None:
        async for (
            feature_table_dict
        ) in self.offline_store_feature_table_service.list_documents_as_dict_iterator(
            query_filter={"precomputed_lookup_feature_table_info": None},
            projection={"_id": 1, "feature_ids": 1},
        ):
            feature_ids = feature_table_dict["feature_ids"]
            await self._update_precomputed_lookup_feature_table_enable_deployment(
                feature_table_dict["_id"],
                feature_ids,
                active_feature_list,
                feature_store_model,
                deployment,
            )

    async def _update_precomputed_lookup_feature_table_enable_deployment(
        self,
        feature_table_id: PydanticObjectId,
        feature_ids: List[PydanticObjectId],
        active_feature_list: FeatureListModel,
        feature_store_model: FeatureStoreModel,
        deployment: DeploymentModel,
    ) -> None:
        # Reload feature table to get the most updated state of the feature table document
        feature_table_dict = await self.offline_store_feature_table_service.get_document_as_dict(
            feature_table_id,
            projection={
                "_id": 1,
                "name": 1,
                "primary_entity_ids": 1,
                "has_ttl": 1,
            },
        )

        all_entities = await self._get_entities(entity_ids=None)
        with timer("Get precomputed lookup feature table", logger=logger):
            precomputed_lookup_feature_table = (
                await self.entity_lookup_feature_table_service.get_precomputed_lookup_feature_table(
                    primary_entity_ids=feature_table_dict["primary_entity_ids"],
                    feature_ids=feature_ids,
                    feature_list=active_feature_list,
                    full_serving_entity_ids=(
                        deployment.serving_entity_ids or active_feature_list.primary_entity_ids
                    ),
                    feature_table_name=feature_table_dict["name"],
                    feature_table_has_ttl=feature_table_dict["has_ttl"],
                    entity_id_to_serving_name={
                        entity.id: entity.serving_names[0] for entity in all_entities
                    },
                    feature_table_id=feature_table_id,
                    feature_store_model=feature_store_model,
                )
            )

        if precomputed_lookup_feature_table is None:
            return

        # Create the table if it doesn't
        existing_table = None
        async for doc in self.offline_store_feature_table_service.list_documents_iterator(
            query_filter={"name": precomputed_lookup_feature_table.name},
        ):
            existing_table = doc
            break

        if existing_table is None:
            created_table = await self.offline_store_feature_table_service.create_document(
                precomputed_lookup_feature_table
            )
            table_id = created_table.id
        else:
            created_table = None
            table_id = existing_table.id

        # Update deployment_ids references
        await self.offline_store_feature_table_service.add_deployment_id(
            document_id=table_id, deployment_id=deployment.id
        )

        # Initialize the table if necessary. This has to be done after deployment_ids is updated.
        if created_table is not None:
            await self.feature_materialize_service.initialize_precomputed_lookup_feature_table(
                feature_table_id, [created_table]
            )

    async def _update_precomputed_lookup_feature_tables_disable_deployment(
        self,
        deployment_id: PydanticObjectId,
    ) -> None:
        service = self.offline_store_feature_table_service
        async for table in service.list_precomputed_lookup_feature_tables_for_deployment(
            deployment_id
        ):
            await self.offline_store_feature_table_service.remove_deployment_id(
                document_id=table.id, deployment_id=deployment_id
            )
            updated_table_dict = (
                await self.offline_store_feature_table_service.get_document_as_dict(
                    table.id,
                    projection={"deployment_ids": 1},
                )
            )
            if len(updated_table_dict.get("deployment_ids", [])) == 0:
                await self._delete_offline_store_feature_table(table.id)

    async def _delete_deprecated_entity_lookup_feature_tables(self) -> None:
        # Clean up dynamic entity lookup feature tables which are no longer used
        service = self.offline_store_feature_table_service
        async for doc in service.list_deprecated_entity_lookup_feature_tables_as_dict():
            await self._delete_offline_store_feature_table(doc["_id"])

    async def _delete_offline_store_feature_table(self, feature_table_id: ObjectId) -> None:
        feature_table_dict = await self.offline_store_feature_table_service.get_document_as_dict(
            feature_table_id, projection={"_id": 1, "precomputed_lookup_feature_table_info": 1}
        )
        if feature_table_dict.get("precomputed_lookup_feature_table_info") is None:
            await self.feature_materialize_scheduler_service.stop_job(
                feature_table_id,
            )
        await self.feature_materialize_service.drop_table(
            await self.offline_store_feature_table_service.get_document(
                feature_table_id,
            )
        )
        await self.offline_store_feature_table_service.delete_document(feature_table_id)

        # Clean up precomputed lookup feature tables. Usually this is a no-op since those tables
        # would have been cleaned up by _update_precomputed_lookup_feature_tables_disable_deployment
        # already. The cleanup here is useful for handling bad state.
        async for lookup_feature_table in self.offline_store_feature_table_service.list_precomputed_lookup_feature_tables_from_source(
            source_feature_table_id=feature_table_id,
        ):
            await self.feature_materialize_service.drop_table(lookup_feature_table)
            await self.offline_store_feature_table_service.delete_document(lookup_feature_table.id)

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

    async def update_table_deployment_reference(
        self, feature_ids: List[PydanticObjectId], deployment_id: ObjectId, to_enable: bool
    ) -> None:
        """
        Update deployment reference in offline store feature tables

        Parameters
        ----------
        feature_ids: List[PydanticObjectId]
            Features IDs used to find offline store feature tables
        deployment_id: ObjectId
            Deployment to update
        to_enable: bool
            Whether to enable or disable the deployment
        """
        if to_enable:
            update = {"$addToSet": {"deployment_ids": deployment_id}}
        else:
            update = {"$pull": {"deployment_ids": deployment_id}}

        await self.offline_store_feature_table_service.update_documents(
            query_filter={"feature_ids": {"$in": feature_ids}},
            update=update,
        )
