"""
TargetTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from uuid import UUID

from featurebyte.logging import get_logger
from featurebyte.models.base import User
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.persistent import Persistent
from featurebyte.schema.target import ComputeTargetRequest
from featurebyte.schema.worker.task.target_table import TargetTableTaskPayload
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.target_helper.compute_target import TargetComputer
from featurebyte.storage import Storage
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin
from featurebyte.worker.util.observation_set_helper import ObservationSetHelper

logger = get_logger(__name__)


class TargetTableTask(DataWarehouseMixin, BaseTask):
    """
    TargetTableTask creates a TargetTable by computing historical features
    """

    payload_class = TargetTableTaskPayload

    def __init__(  # pylint: disable=too-many-arguments
        self,
        task_id: UUID,
        payload: dict[str, Any],
        progress: Any,
        user: User,
        persistent: Persistent,
        storage: Storage,
        temp_storage: Storage,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        observation_set_helper: ObservationSetHelper,
        observation_table_service: ObservationTableService,
        target_computer: TargetComputer,
    ):
        super().__init__(
            task_id=task_id,
            payload=payload,
            progress=progress,
            user=user,
            persistent=persistent,
            storage=storage,
            temp_storage=temp_storage,
        )
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.observation_set_helper = observation_set_helper
        self.observation_table_service = observation_table_service
        self.target_computer = target_computer

    async def get_task_description(self) -> str:
        payload = cast(TargetTableTaskPayload, self.payload)
        return f'Save target table "{payload.name}"'

    async def execute(self) -> Any:
        """
        Execute TargetTableTask
        """
        payload = cast(TargetTableTaskPayload, self.payload)
        feature_store = await self.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.get_db_session(feature_store)
        observation_set = await self.observation_set_helper.get_observation_set(
            payload.observation_table_id, payload.observation_set_storage_path
        )
        location = await self.observation_table_service.generate_materialized_table_location(
            payload.feature_store_id
        )
        async with self.drop_table_on_error(
            db_session=db_session, table_details=location.table_details
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
                progress_callback=self.update_progress,
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
                _id=self.payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                context_id=payload.context_id,
                request_input=payload.request_input,
                **additional_metadata,
            )
            await self.observation_table_service.create_document(observation_table)
