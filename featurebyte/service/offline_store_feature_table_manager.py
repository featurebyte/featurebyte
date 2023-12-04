"""
OfflineStoreFeatureTableUpdateService class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, cast

from bson import ObjectId

from featurebyte import FeatureJobSetting
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature import FeatureModel, OfflineStoreIngestQueryGraph
from featurebyte.models.offline_store_feature_table import (
    OfflineStoreFeatureTableModel,
    OfflineStoreFeatureTableUpdate,
    get_offline_store_feature_table_model,
)
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService
from featurebyte.service.online_store_compute_query_service import OnlineStoreComputeQueryService

FEATURE_TABLE_NAME_PREFIX = "FEATURE_TABLE_"


class OfflineStoreFeatureTableManagerService:
    """
    OfflineStoreFeatureTableManagerService class
    """

    def __init__(
        self,
        offline_store_feature_table_service: OfflineStoreFeatureTableService,
        feature_service: FeatureService,
        online_store_compute_query_service: OnlineStoreComputeQueryService,
        entity_service: EntityService,
    ):
        self.offline_store_feature_table_service = offline_store_feature_table_service
        self.feature_service = feature_service
        self.online_store_compute_query_service = online_store_compute_query_service
        self.entity_service = entity_service

    async def handle_online_enabled_feature(self, feature: FeatureModel) -> None:
        """
        Handles the case where a feature is enabled for online serving by updating all affected
        feature tables.

        Parameters
        ----------
        feature: FeatureModel
            Model of the feature to be enabled for online serving
        """
        offline_ingest_graphs = feature.extract_offline_store_ingest_query_graphs()
        for offline_ingest_graph in offline_ingest_graphs:
            feature_table_dict = await self._get_compatible_existing_feature_table(
                offline_ingest_graph=offline_ingest_graph
            )
            if feature_table_dict is not None:
                # update existing table
                feature_ids = feature_table_dict["feature_ids"][:]
                if feature.id not in feature_ids:
                    feature_ids.append(feature.id)
                    await self._update_offline_store_feature_table(feature_table_dict, feature_ids)
            else:
                # create new table
                feature_table_name = await self._get_offline_store_table_name(offline_ingest_graph)
                feature_table_model = await self._construct_offline_store_feature_table_model(
                    feature_table_name=feature_table_name,
                    feature_ids=[feature.id],
                    primary_entity_ids=offline_ingest_graph.primary_entity_ids,
                    has_ttl=offline_ingest_graph.has_ttl,
                    feature_job_setting=offline_ingest_graph.feature_job_setting,
                )
                await self.offline_store_feature_table_service.create_document(feature_table_model)

    async def handle_online_disabled_feature(self, feature: FeatureModel) -> None:
        """
        Handles the case where a feature is disabled for online serving by updating all affected
        feature tables. In normal case there should only be one.

        Parameters
        ----------
        feature: FeatureModel
            Model of the feature to be disabled for online serving
        """
        feature_table_data = await self.offline_store_feature_table_service.list_documents_as_dict(
            query_filter={"feature_ids": feature.id}
        )
        for feature_table_dict in feature_table_data["data"]:
            updated_feature_ids = feature_table_dict["feature_ids"][:]
            updated_feature_ids.remove(feature.id)
            if updated_feature_ids:
                await self._update_offline_store_feature_table(
                    feature_table_dict, updated_feature_ids
                )
            else:
                await self.offline_store_feature_table_service.delete_document(
                    document_id=feature_table_dict["_id"],
                )

    async def _get_offline_store_table_name(
        self, offline_ingest_graph: OfflineStoreIngestQueryGraph
    ) -> str:
        # Note: Temporary naming scheme, to be delegated to offline_ingest_graph entirely.
        primary_entity_names = []
        async for entity_model in self.entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": offline_ingest_graph.primary_entity_ids}}
        ):
            assert entity_model.name is not None
            primary_entity_names.append(entity_model.name)

        table_name = FEATURE_TABLE_NAME_PREFIX + "_X_".join(sorted(primary_entity_names))

        if offline_ingest_graph.feature_job_setting is not None:
            parts = [
                offline_ingest_graph.feature_job_setting.frequency_seconds,
                offline_ingest_graph.feature_job_setting.blind_spot_seconds,
                offline_ingest_graph.feature_job_setting.time_modulo_frequency_seconds,
            ]
            feature_job_setting_identifier = "_".join([str(p) for p in parts])
            table_name += "_" + feature_job_setting_identifier

        return table_name

    async def _get_compatible_existing_feature_table(
        self, offline_ingest_graph: OfflineStoreIngestQueryGraph
    ) -> Optional[Dict[str, Any]]:
        feature_table_data = await self.offline_store_feature_table_service.list_documents_as_dict(
            query_filter={
                "primary_entity_ids": offline_ingest_graph.primary_entity_ids,
                "has_ttl": offline_ingest_graph.has_ttl,
                "feature_job_setting": offline_ingest_graph.feature_job_setting.dict()
                if offline_ingest_graph.feature_job_setting is not None
                else None,
            },
        )
        if feature_table_data["total"] > 0:
            return cast(Dict[str, Any], feature_table_data["data"][0])
        return None

    async def _update_offline_store_feature_table(
        self, feature_table_dict: Dict[str, Any], updated_feature_ids: List[ObjectId]
    ) -> None:
        feature_table_model = await self._construct_offline_store_feature_table_model(
            feature_table_name=feature_table_dict["name"],
            feature_ids=updated_feature_ids,
            primary_entity_ids=feature_table_dict["primary_entity_ids"],
            has_ttl=feature_table_dict["has_ttl"],
            feature_job_setting=FeatureJobSetting(**feature_table_dict["feature_job_setting"]),
        )
        update_schema = OfflineStoreFeatureTableUpdate(**feature_table_model.dict())
        await self.offline_store_feature_table_service.update_document(
            document_id=feature_table_dict["_id"], data=update_schema
        )

    async def _construct_offline_store_feature_table_model(
        self,
        feature_table_name: str,
        feature_ids: List[ObjectId],
        primary_entity_ids: List[PydanticObjectId],
        has_ttl: bool,
        feature_job_setting: Optional[FeatureJobSetting],
    ) -> OfflineStoreFeatureTableModel:
        aggregate_result_table_names = set()

        feature_ids_to_model: Dict[ObjectId, FeatureModel] = {}
        async for feature_model in self.feature_service.list_documents_iterator(
            query_filter={"_id": {"$in": feature_ids}}
        ):
            feature_ids_to_model[feature_model.id] = feature_model
            aggregate_result_table_names.update(feature_model.online_store_table_names)

        aggregate_result_table_names = sorted(aggregate_result_table_names)  # type: ignore[assignment]

        # Get aggregate result tables
        aggregate_result_table_name_to_serving_names = {}
        async for online_store_compute_query_model in self.online_store_compute_query_service.list_documents_iterator(
            query_filter={"table_name": {"$in": aggregate_result_table_names}}
        ):
            aggregate_result_table_name_to_serving_names[
                online_store_compute_query_model.table_name
            ] = online_store_compute_query_model.serving_names

        primary_entities = []
        async for entity_model in self.entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": primary_entity_ids}}
        ):
            primary_entities.append(entity_model)

        primary_entity_serving_names = sorted(
            [entity.serving_names[0] for entity in primary_entities]
        )
        required_aggregate_result_tables = []
        for (
            aggregate_result_table_name,
            aggregate_result_table_serving_names,
        ) in aggregate_result_table_name_to_serving_names.items():
            if sorted(aggregate_result_table_serving_names) == primary_entity_serving_names:
                required_aggregate_result_tables.append(aggregate_result_table_name)

        return get_offline_store_feature_table_model(
            feature_table_name=feature_table_name,
            features=[feature_ids_to_model[feature_id] for feature_id in feature_ids],
            aggregate_result_table_names=required_aggregate_result_tables,
            primary_entities=primary_entities,
            has_ttl=has_ttl,
            feature_job_setting=feature_job_setting,
        )
