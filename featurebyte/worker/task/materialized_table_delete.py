"""
Materialized Table Delete Task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.models.materialized_table import MaterializedTableModel
from featurebyte.schema.worker.task.materialized_table_delete import (
    MaterializedTableCollectionName,
    MaterializedTableDeleteTaskPayload,
)
from featurebyte.service.validator.materialized_table_delete import (
    ObservationTableDeleteValidator,
    check_delete_batch_request_table,
    check_delete_static_source_table,
)
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin


class MaterializedTableDeleteTask(DataWarehouseMixin, BaseTask):
    """
    Materialized Table Delete Task
    """

    payload_class = MaterializedTableDeleteTaskPayload

    @property
    def task_payload(self) -> MaterializedTableDeleteTaskPayload:
        """
        Task payload

        Returns
        -------
        MaterializedTableDeleteTaskPayload
        """
        return cast(MaterializedTableDeleteTaskPayload, self.payload)

    async def _delete_batch_request_table(self) -> MaterializedTableModel:
        document = await check_delete_batch_request_table(
            batch_request_table_service=self.app_container.batch_request_table_service,
            batch_feature_table_service=self.app_container.batch_feature_table_service,
            document_id=self.task_payload.document_id,
        )
        await self.app_container.batch_request_table_service.delete_document(
            document_id=self.task_payload.document_id
        )
        return cast(MaterializedTableModel, document)

    async def _delete_batch_feature_table(self) -> MaterializedTableModel:
        document = await self.app_container.batch_feature_table_service.get_document(
            document_id=self.task_payload.document_id
        )
        await self.app_container.batch_feature_table_service.delete_document(
            document_id=document.id
        )
        return cast(MaterializedTableModel, document)

    async def _delete_observation_table(self) -> MaterializedTableModel:
        validator: ObservationTableDeleteValidator = (
            self.app_container.observation_table_delete_validator
        )
        document = await validator.check_delete_observation_table(
            observation_table_id=self.task_payload.document_id,
        )
        await self.app_container.observation_table_service.delete_document(
            document_id=self.task_payload.document_id
        )
        return cast(MaterializedTableModel, document)

    async def _delete_historical_feature_table(self) -> MaterializedTableModel:
        document = await self.app_container.historical_feature_table_service.get_document(
            document_id=self.task_payload.document_id
        )
        await self.app_container.historical_feature_table_service.delete_document(
            document_id=document.id
        )
        return cast(MaterializedTableModel, document)

    async def _delete_target_table(self) -> MaterializedTableModel:
        document = await self.app_container.target_table_service.get_document(
            document_id=self.task_payload.document_id
        )
        await self.app_container.target_table_service.delete_document(document_id=document.id)
        return cast(MaterializedTableModel, document)

    async def _delete_static_source_table(self) -> MaterializedTableModel:
        document = await check_delete_static_source_table(
            static_source_table_service=self.app_container.static_source_table_service,
            table_service=self.app_container.table_service,
            document_id=self.task_payload.document_id,
        )
        await self.app_container.static_source_table_service.delete_document(
            document_id=self.task_payload.document_id
        )
        return cast(MaterializedTableModel, document)

    async def execute(self) -> Any:
        """
        Execute Deployment Create & Update Task
        """
        # table to delete action mapping
        table_to_delete_action = {
            MaterializedTableCollectionName.BATCH_REQUEST: self._delete_batch_request_table,
            MaterializedTableCollectionName.BATCH_FEATURE: self._delete_batch_feature_table,
            MaterializedTableCollectionName.OBSERVATION: self._delete_observation_table,
            MaterializedTableCollectionName.HISTORICAL_FEATURE: self._delete_historical_feature_table,
            MaterializedTableCollectionName.STATIC_SOURCE: self._delete_static_source_table,
            MaterializedTableCollectionName.TARGET: self._delete_target_table,
        }

        # delete document stored at mongo
        deleted_document = await table_to_delete_action[self.task_payload.collection_name]()

        # delete table stored at data warehouse
        feature_store = await self.app_container.feature_store_service.get_document(
            document_id=deleted_document.location.feature_store_id
        )
        db_session = await self.get_db_session(feature_store=feature_store)
        await db_session.drop_table(
            table_name=deleted_document.location.table_details.table_name,
            schema_name=deleted_document.location.table_details.schema_name,  # type: ignore
            database_name=deleted_document.location.table_details.database_name,  # type: ignore
        )
        return
