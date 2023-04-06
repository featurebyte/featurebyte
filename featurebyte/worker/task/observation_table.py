"""
ObservationTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from bson import ObjectId

from featurebyte.logger import logger
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
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

        destination_table_name = f"OBSERVATION_TABLE_{ObjectId()}"
        location = TabularSource(
            feature_store_id=payload.feature_store_id,
            table_details=TableDetails(
                database_name=db_session.database_name,
                schema_name=db_session.schema_name,
                table_name=destination_table_name,
            ),
        )

        # TODO: add actual materialization logic here before creating the ObservationTable document

        logger.debug("Creating a new ObservationTable", extras=location.table_details.dict())
        observation_table = ObservationTableModel(
            _id=self.payload.output_document_id,
            user_id=payload.user_id,
            name=payload.name,
            location=location,
            context_id=payload.context_id,
            observation_input=payload.observation_input,
        )

        context_service = ContextService(
            user=self.user, persistent=persistent, catalog_id=self.payload.catalog_id
        )
        observation_table_service = ObservationTableService(
            user=self.user,
            persistent=persistent,
            catalog_id=self.payload.catalog_id,
            context_service=context_service,
        )
        created_doc = await observation_table_service.create_document(observation_table)
        assert created_doc.id == payload.output_document_id
