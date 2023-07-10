"""
TargetTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from pathlib import Path

from featurebyte.logging import get_logger
from featurebyte.models.target_table import TargetTableModel
from featurebyte.schema.worker.task.target_table import TargetTableTaskPayload
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.target_table import TargetTableService
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class TargetTableTask(DataWarehouseMixin, BaseTask):
    """
    TargetTableTask creates a TargetTable by computing historical features
    """

    payload_class = TargetTableTaskPayload

    async def execute(self) -> Any:
        """
        Execute TargetTableTask
        """
        # TODO: fully implement in follow-up
        # There's still some steps to _actually_ compute the target table. This currently just creates a mongo db
        # document
        payload = cast(TargetTableTaskPayload, self.payload)
        feature_store = await self.app_container.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.get_db_session(feature_store)

        target_table_service: TargetTableService = self.app_container.target_table_service

        location = await target_table_service.generate_materialized_table_location(
            self.get_credential, payload.feature_store_id
        )
        (
            columns_info,
            num_rows,
        ) = await target_table_service.get_columns_info_and_num_rows(
            db_session, location.table_details
        )
        target_table = TargetTableModel(
            _id=payload.output_document_id,
            user_id=self.payload.user_id,
            name=payload.name,
            location=location,
            observation_table_id=payload.observation_table_id,
            target_id=payload.target_id,
            columns_info=columns_info,
            num_rows=num_rows,
        )
        await target_table_service.create_document(target_table)
