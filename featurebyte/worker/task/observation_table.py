"""
ObservationTable creation task
"""

from __future__ import annotations

from typing import Any

from featurebyte.logging import get_logger
from featurebyte.models.observation_table import ObservationTableModel, TargetInput
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class ObservationTableTask(DataWarehouseMixin, BaseTask[ObservationTableTaskPayload]):
    """
    ObservationTable Task
    """

    payload_class = ObservationTableTaskPayload

    def __init__(  # pylint: disable=too-many-arguments
        self,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        observation_table_service: ObservationTableService,
    ):
        super().__init__()
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.observation_table_service = observation_table_service

    async def get_task_description(self, payload: ObservationTableTaskPayload) -> str:
        return f'Save observation table "{payload.name}" from source table.'

    async def execute(self, payload: ObservationTableTaskPayload) -> Any:
        feature_store = await self.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)
        location = await self.observation_table_service.generate_materialized_table_location(
            payload.feature_store_id,
        )
        await payload.request_input.materialize(
            session=db_session,
            destination=location.table_details,
            sample_rows=payload.sample_rows,
        )
        await self.observation_table_service.add_row_index_column(
            session=db_session, table_details=location.table_details
        )

        async with self.drop_table_on_error(db_session, location.table_details, payload):
            payload_input = payload.request_input
            assert not isinstance(payload_input, TargetInput)
            additional_metadata = (
                await self.observation_table_service.validate_materialized_table_and_get_metadata(
                    db_session,
                    location.table_details,
                    feature_store=feature_store,
                    skip_entity_validation_checks=payload.skip_entity_validation_checks,
                    primary_entity_ids=payload.primary_entity_ids,
                    target_namespace_id=payload.target_namespace_id,
                )
            )

            logger.debug("Creating a new ObservationTable", extra=location.table_details.dict())
            primary_entity_ids = payload.primary_entity_ids or []
            observation_table = ObservationTableModel(
                _id=payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                context_id=payload.context_id,
                request_input=payload.request_input,
                purpose=payload.purpose,
                primary_entity_ids=primary_entity_ids,
                has_row_index=True,
                target_namespace_id=payload.target_namespace_id,
                **additional_metadata,
            )
            await self.observation_table_service.create_document(observation_table)
