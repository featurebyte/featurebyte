"""
HistoricalFeatureTable creation task
"""

from __future__ import annotations

from typing import Any, List, Optional

from featurebyte.logging import get_logger
from featurebyte.models.historical_feature_table import FeatureInfo, HistoricalFeatureTableModel
from featurebyte.schema.worker.task.historical_feature_table import (
    HistoricalFeatureTableTaskPayload,
)
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.historical_features import HistoricalFeaturesService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.system_metrics import SystemMetricsService
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin
from featurebyte.worker.util.observation_set_helper import ObservationSetHelper

logger = get_logger(__name__)


class HistoricalFeatureTableTask(DataWarehouseMixin, BaseTask[HistoricalFeatureTableTaskPayload]):
    """
    HistoricalFeatureTableTask creates a HistoricalFeatureTable by computing historical features
    """

    payload_class = HistoricalFeatureTableTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        observation_set_helper: ObservationSetHelper,
        feature_service: FeatureService,
        feature_list_service: FeatureListService,
        historical_feature_table_service: HistoricalFeatureTableService,
        historical_features_service: HistoricalFeaturesService,
        system_metrics_service: SystemMetricsService,
    ):
        super().__init__(task_manager=task_manager)
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.observation_set_helper = observation_set_helper
        self.feature_service = feature_service
        self.feature_list_service = feature_list_service
        self.historical_feature_table_service = historical_feature_table_service
        self.historical_features_service = historical_features_service
        self.system_metrics_service = system_metrics_service

    async def get_task_description(self, payload: HistoricalFeatureTableTaskPayload) -> str:
        return f'Save historical feature table "{payload.name}"'

    async def execute(self, payload: HistoricalFeatureTableTaskPayload) -> Any:
        feature_store = await self.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)

        observation_set = await self.observation_set_helper.get_observation_set(
            payload.observation_table_id, payload.observation_set_storage_path
        )
        location = await self.historical_feature_table_service.generate_materialized_table_location(
            payload.feature_store_id
        )

        fl_get_historical_features = payload.featurelist_get_historical_features
        features_info: Optional[List[FeatureInfo]] = None
        if fl_get_historical_features.feature_list_id:
            feature_list_doc = await self.feature_list_service.get_document_as_dict(
                document_id=fl_get_historical_features.feature_list_id,
                projection={"feature_ids": 1},
            )
            features_info = []
            async for feature_doc in self.feature_service.list_documents_as_dict_iterator(
                query_filter={"_id": {"$in": feature_list_doc["feature_ids"]}},
                projection={"name": 1, "_id": 1},
            ):
                features_info.append(
                    FeatureInfo(feature_name=feature_doc["name"], feature_id=feature_doc["_id"])
                )
        elif fl_get_historical_features.feature_clusters:
            features_info = []
            for cluster in fl_get_historical_features.feature_clusters:
                if cluster.feature_node_definition_hashes:
                    for info in cluster.feature_node_definition_hashes:
                        if info.feature_id and info.feature_name:
                            features_info.append(
                                FeatureInfo(
                                    feature_id=info.feature_id, feature_name=info.feature_name
                                )
                            )

            # reset num_features to None if the list is empty (to revert to the old behavior)
            features_info = features_info or None

        async with self.drop_table_on_error(
            db_session=db_session,
            list_of_table_details=[location.table_details],
            payload=payload,
        ):
            result = await self.historical_features_service.compute(
                observation_set=observation_set,
                compute_request=fl_get_historical_features,
                output_table_details=location.table_details,
            )
            (
                columns_info,
                num_rows,
            ) = await self.historical_feature_table_service.get_columns_info_and_num_rows(
                db_session, location.table_details
            )
            logger.debug(
                "Creating a new HistoricalFeatureTable", extra=location.table_details.model_dump()
            )
            historical_feature_table = HistoricalFeatureTableModel(
                _id=payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                observation_table_id=payload.observation_table_id,
                feature_list_id=fl_get_historical_features.feature_list_id,
                columns_info=columns_info,
                num_rows=num_rows,
                features_info=features_info,
                is_view=result.is_output_view,
            )
            await self.historical_feature_table_service.create_document(historical_feature_table)
            metrics_data = result.historical_features_metrics
            metrics_data.historical_feature_table_id = payload.output_document_id
            await self.system_metrics_service.create_metrics(metrics_data=metrics_data)
