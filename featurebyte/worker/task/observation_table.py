"""
ObservationTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.logger import logger
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.service.context import ContextService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.session.manager import SessionManager
from featurebyte.worker.task.base import BaseTask


class ObservationTableTask(BaseTask):
    """
    ObservationTable Task
    """

    payload_class = ObservationTableTaskPayload

    async def execute(self) -> Any:
        """
        Execute ObservationTable task
        """
        payload = cast(ObservationTableTaskPayload, self.payload)
        persistent = self.get_persistent()

        feature_store_service = FeatureStoreService(
            user=self.user, persistent=persistent, catalog_id=self.payload.catalog_id
        )
        feature_store = await feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        session_manager = SessionManager(
            credentials={
                feature_store.name: await self.get_credential(
                    user_id=payload.user_id, feature_store_name=feature_store.name
                )
            }
        )
        db_session = await session_manager.get_session(feature_store)

        context_service = ContextService(
            user=self.user, persistent=persistent, catalog_id=self.payload.catalog_id
        )
        observation_table_service = ObservationTableService(
            user=self.user,
            persistent=persistent,
            catalog_id=self.payload.catalog_id,
            context_service=context_service,
            feature_store_service=feature_store_service,
        )
        location = await observation_table_service.generate_materialized_table_location(
            self.get_credential,
            payload.feature_store_id,
        )
        query = payload.observation_input.get_materialize_sql(
            destination=location.table_details, source_type=feature_store.type
        )
        await db_session.execute_query(query)

        additional_metadata = (
            await observation_table_service.validate_materialized_table_and_get_metadata(
                db_session, location.table_details
            )
        )

        logger.debug("Creating a new ObservationTable", extras=location.table_details.dict())
        observation_table = ObservationTableModel(
            _id=self.payload.output_document_id,
            user_id=payload.user_id,
            name=payload.name,
            location=location,
            context_id=payload.context_id,
            observation_input=payload.observation_input,
            **additional_metadata,
        )

        created_doc = await observation_table_service.create_document(observation_table)
        assert created_doc.id == payload.output_document_id
