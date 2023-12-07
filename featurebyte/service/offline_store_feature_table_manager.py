"""
OfflineStoreFeatureTableUpdateService class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, cast

from bson import ObjectId

from featurebyte import FeatureJobSetting
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature import FeatureModel
from featurebyte.models.offline_store_feature_table import (
    OfflineStoreFeatureTableModel,
    OfflineStoreFeatureTableUpdate,
    get_offline_store_feature_table_model,
)
from featurebyte.models.offline_store_ingest_query import OfflineStoreIngestQueryGraph
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_materialize import FeatureMaterializeService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService
from featurebyte.service.online_store_compute_query_service import OnlineStoreComputeQueryService
from featurebyte.service.session_manager import SessionManagerService


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
        feature_materialize_service: FeatureMaterializeService,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
    ):
        self.offline_store_feature_table_service = offline_store_feature_table_service
        self.feature_service = feature_service
        self.online_store_compute_query_service = online_store_compute_query_service
        self.entity_service = entity_service
        self.feature_materialize_service = feature_materialize_service
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service

    async def handle_online_enabled_feature(self, feature: FeatureModel) -> None:
        """
        Handles the case where a feature is enabled for online serving by updating all affected
        feature tables.

        Parameters
        ----------
        feature: FeatureModel
            Model of the feature to be enabled for online serving
        """
        feature_entities = []
        async for entity_model in self.entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": feature.entity_ids}}
        ):
            feature_entities.append(entity_model)

        # FIXME: cleanup this logic once integrate this with feast feature store
        if feature.offline_store_info is None:
            feature.initialize_offline_store_info(
                entity_id_to_serving_name={
                    entity.id: entity.serving_names[0] for entity in feature_entities
                }
            )

        offline_store_info = feature.offline_store_info
        assert offline_store_info is not None, "Offline store info should not be None"
        offline_ingest_graphs = offline_store_info.extract_offline_store_ingest_query_graphs()
        session = None

        for offline_ingest_graph in offline_ingest_graphs:
            feature_table_dict = await self._get_compatible_existing_feature_table(
                offline_ingest_graph=offline_ingest_graph
            )

            if feature_table_dict is not None:
                # update existing table
                feature_ids = feature_table_dict["feature_ids"][:]
                if feature.id not in feature_ids:
                    feature_ids.append(feature.id)
                    feature_table_model = await self._update_offline_store_feature_table(
                        feature_table_dict, feature_ids
                    )
                else:
                    feature_table_model = None
            else:
                # create new table
                feature_table_model = await self._construct_offline_store_feature_table_model(
                    feature_table_name=offline_ingest_graph.offline_store_table_name,
                    feature_ids=[feature.id],
                    primary_entity_ids=offline_ingest_graph.primary_entity_ids,
                    has_ttl=offline_ingest_graph.has_ttl,
                    feature_job_setting=offline_ingest_graph.feature_job_setting,
                )
                await self.offline_store_feature_table_service.create_document(feature_table_model)

            if feature_table_model is not None:
                if session is None:
                    feature_store = await self.feature_store_service.get_document(
                        document_id=feature_table_model.feature_cluster.feature_store_id
                    )
                    session = await self.session_manager_service.get_feature_store_session(
                        feature_store
                    )
                await self.feature_materialize_service.initialize_new_columns(
                    session, feature_table_model
                )

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
    ) -> OfflineStoreFeatureTableModel:
        feature_table_model = await self._construct_offline_store_feature_table_model(
            feature_table_name=feature_table_dict["name"],
            feature_ids=updated_feature_ids,
            primary_entity_ids=feature_table_dict["primary_entity_ids"],
            has_ttl=feature_table_dict["has_ttl"],
            feature_job_setting=FeatureJobSetting(**feature_table_dict["feature_job_setting"]),
        )
        update_schema = OfflineStoreFeatureTableUpdate(**feature_table_model.dict())
        return await self.offline_store_feature_table_service.update_document(
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
        query_filter = {
            "table_name": {"$in": aggregate_result_table_names},
            "serving_names": {
                "$all": primary_entity_serving_names,
                "$size": len(primary_entity_serving_names),
            },
        }
        async for online_store_compute_query_model in self.online_store_compute_query_service.list_documents_iterator(
            query_filter=query_filter
        ):
            filtered_table_names.append(online_store_compute_query_model.table_name)

        return sorted(set(filtered_table_names))
