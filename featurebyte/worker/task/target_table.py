"""
TargetTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.logging import get_logger
from featurebyte.models.target_table import TargetTableModel
from featurebyte.schema.target import ComputeTargetRequest
from featurebyte.schema.worker.task.target_table import TargetTableTaskPayload
from featurebyte.service.target_helper.compute_target import TargetComputer
from featurebyte.service.target_table import TargetTableService
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin
from featurebyte.worker.util.observation_set_helper import ObservationSetHelper

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
        payload = cast(TargetTableTaskPayload, self.payload)
        feature_store = await self.app_container.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.get_db_session(feature_store)

        observation_set_helper: ObservationSetHelper = self.app_container.observation_set_helper
        observation_set = await observation_set_helper.get_observation_set(
            payload.observation_table_id, payload.observation_set_storage_path
        )

        target_table_service: TargetTableService = self.app_container.target_table_service
        location = await target_table_service.generate_materialized_table_location(
            self.get_credential, payload.feature_store_id
        )
        async with self.drop_table_on_error(
            db_session=db_session, table_details=location.table_details
        ):
            target_computer: TargetComputer = self.app_container.target_computer
            await target_computer.compute(
                observation_set=observation_set,
                compute_request=ComputeTargetRequest(
                    feature_store_id=payload.feature_store_id,
                    graph=payload.graph,
                    node_names=payload.node_names,
                    serving_names_mapping=payload.serving_names_mapping,
                    target_id=payload.target_id,
                ),
                get_credential=self.get_credential,
                output_table_details=location.table_details,
                progress_callback=self.update_progress,
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
