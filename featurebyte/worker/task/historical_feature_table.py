"""
HistoricalFeatureTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.logging import get_logger
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.schema.worker.task.historical_feature_table import (
    HistoricalFeatureTableTaskPayload,
)
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.historical_features import HistoricalFeaturesService
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin
from featurebyte.worker.util.observation_set_helper import ObservationSetHelper

logger = get_logger(__name__)


class HistoricalFeatureTableTask(DataWarehouseMixin, BaseTask):
    """
    HistoricalFeatureTableTask creates a HistoricalFeatureTable by computing historical features
    """

    payload_class = HistoricalFeatureTableTaskPayload

    async def execute(self) -> Any:
        """
        Execute HistoricalFeatureTableTask
        """
        payload = cast(HistoricalFeatureTableTaskPayload, self.payload)
        feature_store = await self.app_container.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.get_db_session(feature_store)

        observation_set_helper: ObservationSetHelper = self.app_container.observation_set_helper
        observation_set = await observation_set_helper.get_observation_set(
            payload.observation_table_id, payload.observation_set_storage_path
        )

        historical_feature_table_service: HistoricalFeatureTableService = (
            self.app_container.historical_feature_table_service
        )
        location = await historical_feature_table_service.generate_materialized_table_location(
            self.get_credential, payload.feature_store_id
        )

        async with self.drop_table_on_error(
            db_session=db_session, table_details=location.table_details
        ):
            historical_features_service: HistoricalFeaturesService = (
                self.app_container.historical_features_service
            )
            await historical_features_service.compute(
                observation_set=observation_set,
                compute_request=payload.featurelist_get_historical_features,
                get_credential=self.get_credential,
                output_table_details=location.table_details,
                progress_callback=self.update_progress,
            )
            (
                columns_info,
                num_rows,
            ) = await historical_feature_table_service.get_columns_info_and_num_rows(
                db_session, location.table_details
            )
            logger.debug(
                "Creating a new HistoricalFeatureTable", extra=location.table_details.dict()
            )
            historical_feature_table = HistoricalFeatureTableModel(
                _id=payload.output_document_id,
                user_id=self.payload.user_id,
                name=payload.name,
                location=location,
                observation_table_id=payload.observation_table_id,
                feature_list_id=payload.featurelist_get_historical_features.feature_list_id,
                columns_info=columns_info,
                num_rows=num_rows,
            )
            await historical_feature_table_service.create_document(historical_feature_table)
