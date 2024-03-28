"""
HistoricalFeatureTable creation task
"""

from __future__ import annotations

from typing import Any

from featurebyte.logging import get_logger
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.schema.worker.task.historical_feature_table import (
    HistoricalFeatureTableTaskPayload,
)
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.historical_features import HistoricalFeaturesService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin
from featurebyte.worker.util.observation_set_helper import ObservationSetHelper

logger = get_logger(__name__)


class HistoricalFeatureTableTask(DataWarehouseMixin, BaseTask[HistoricalFeatureTableTaskPayload]):
    """
    HistoricalFeatureTableTask creates a HistoricalFeatureTable by computing historical features
    """

    payload_class = HistoricalFeatureTableTaskPayload

    def __init__(  # pylint: disable=too-many-arguments
        self,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        observation_set_helper: ObservationSetHelper,
        historical_feature_table_service: HistoricalFeatureTableService,
        historical_features_service: HistoricalFeaturesService,
    ):
        super().__init__()
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.observation_set_helper = observation_set_helper
        self.historical_feature_table_service = historical_feature_table_service
        self.historical_features_service = historical_features_service

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
        async with self.drop_table_on_error(
            db_session=db_session,
            table_details=location.table_details,
            payload=payload,
        ):
            result = await self.historical_features_service.compute(
                observation_set=observation_set,
                compute_request=payload.featurelist_get_historical_features,
                output_table_details=location.table_details,
            )
            (
                columns_info,
                num_rows,
            ) = await self.historical_feature_table_service.get_columns_info_and_num_rows(
                db_session, location.table_details
            )
            logger.debug(
                "Creating a new HistoricalFeatureTable", extra=location.table_details.dict()
            )
            historical_feature_table = HistoricalFeatureTableModel(
                _id=payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                observation_table_id=payload.observation_table_id,
                feature_list_id=payload.featurelist_get_historical_features.feature_list_id,
                columns_info=columns_info,
                num_rows=num_rows,
                is_view=result.is_output_view,
            )
            await self.historical_feature_table_service.create_document(historical_feature_table)
