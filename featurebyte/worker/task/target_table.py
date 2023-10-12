"""
TargetTable creation task
"""
from __future__ import annotations

from typing import Any

from featurebyte.logging import get_logger
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.schema.target import ComputeTargetRequest
from featurebyte.schema.worker.task.target_table import TargetTableTaskPayload
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.target_helper.compute_target import TargetComputer
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin
from featurebyte.worker.util.observation_set_helper import ObservationSetHelper
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

logger = get_logger(__name__)


class TargetTableTask(DataWarehouseMixin, BaseTask[TargetTableTaskPayload]):
    """
    TargetTableTask creates a TargetTable by computing historical features
    """

    payload_class = TargetTableTaskPayload

    def __init__(  # pylint: disable=too-many-arguments
        self,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        observation_set_helper: ObservationSetHelper,
        observation_table_service: ObservationTableService,
        target_computer: TargetComputer,
        task_progress_updater: TaskProgressUpdater,
    ):
        super().__init__()
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.observation_set_helper = observation_set_helper
        self.observation_table_service = observation_table_service
        self.target_computer = target_computer
        self.task_progress_updater = task_progress_updater

    async def get_task_description(self, payload: TargetTableTaskPayload) -> str:
        return f'Save target table "{payload.name}"'

    async def execute(self, payload: TargetTableTaskPayload) -> Any:
        feature_store = await self.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)
        observation_set = await self.observation_set_helper.get_observation_set(
            payload.observation_table_id, payload.observation_set_storage_path
        )
        location = await self.observation_table_service.generate_materialized_table_location(
            payload.feature_store_id
        )
        async with self.drop_table_on_error(
            db_session=db_session, table_details=location.table_details, payload=payload
        ):
            await self.target_computer.compute(
                observation_set=observation_set,
                compute_request=ComputeTargetRequest(
                    feature_store_id=payload.feature_store_id,
                    graph=payload.graph,
                    node_names=payload.node_names,
                    serving_names_mapping=payload.serving_names_mapping,
                    target_id=payload.target_id,
                ),
                output_table_details=location.table_details,
                progress_callback=self.task_progress_updater.update_progress,
            )

            additional_metadata = (
                await self.observation_table_service.validate_materialized_table_and_get_metadata(
                    db_session,
                    location.table_details,
                    feature_store=feature_store,
                    serving_names_remapping=payload.serving_names_mapping,
                    skip_entity_validation_checks=payload.skip_entity_validation_checks,
                )
            )

            observation_table = ObservationTableModel(
                _id=payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                context_id=payload.context_id,
                request_input=payload.request_input,
                **additional_metadata,
            )
            await self.observation_table_service.create_document(observation_table)
