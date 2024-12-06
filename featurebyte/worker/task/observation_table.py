"""
ObservationTable creation task
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from dateutil import tz

from featurebyte.enum import SpecialColumnName
from featurebyte.logging import get_logger
from featurebyte.models.observation_table import ObservationTableModel, TargetInput
from featurebyte.query_graph.sql.feature_historical import (
    HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR,
)
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class ObservationTableTask(DataWarehouseMixin, BaseTask[ObservationTableTaskPayload]):
    """
    ObservationTable Task
    """

    payload_class = ObservationTableTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        observation_table_service: ObservationTableService,
        entity_service: EntityService,
    ):
        super().__init__(task_manager=task_manager)
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.observation_table_service = observation_table_service
        self.entity_service = entity_service

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

        # exclude rows with null values in key columns (POINT_IN_TIME, entity columns, target_column)
        columns_to_exclude_missing_values = [str(SpecialColumnName.POINT_IN_TIME)]
        if payload.primary_entity_ids is not None:
            for entity_id in payload.primary_entity_ids:
                entity = await self.entity_service.get_document(document_id=entity_id)
                columns_to_exclude_missing_values.extend(entity.serving_names)
        if payload.target_column is not None:
            columns_to_exclude_missing_values.append(payload.target_column)

        # limit POINT_IN_TIME to a certain recency to avoid historical request failures
        max_timestamp = datetime.utcnow() - timedelta(
            hours=HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR
        )
        if payload.sample_to_timestamp is not None:
            if payload.sample_to_timestamp.tzinfo is not None:
                sample_to_timestamp = payload.sample_to_timestamp.astimezone(tz.UTC).replace(
                    tzinfo=None
                )
            else:
                sample_to_timestamp = payload.sample_to_timestamp

            sample_to_timestamp = min(sample_to_timestamp, max_timestamp)
        else:
            sample_to_timestamp = max_timestamp

        await payload.request_input.materialize(
            session=db_session,
            destination=location.table_details,
            sample_rows=payload.sample_rows,
            sample_from_timestamp=payload.sample_from_timestamp,
            sample_to_timestamp=sample_to_timestamp,
            columns_to_exclude_missing_values=columns_to_exclude_missing_values,
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

            logger.debug(
                "Creating a new ObservationTable", extra=location.table_details.model_dump()
            )
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
                sample_rows=payload.sample_rows,
                sample_from_timestamp=payload.sample_from_timestamp,
                sample_to_timestamp=payload.sample_to_timestamp,
                **additional_metadata,
            )
            await self.observation_table_service.create_document(observation_table)
