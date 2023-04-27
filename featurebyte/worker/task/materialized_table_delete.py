"""
Materialized Table Delete Task
"""
from __future__ import annotations

from typing import Any, cast

from bson import ObjectId

from featurebyte.exception import DocumentDeletionError
from featurebyte.models.materialized_table import MaterializedTableModel
from featurebyte.schema.worker.task.materialized_table_delete import (
    MaterializedTableCollectionName,
    MaterializedTableDeleteTaskPayload,
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
        """
        return cast(MaterializedTableDeleteTaskPayload, self.payload)

    @staticmethod
    def _get_error_message(
        document_id: ObjectId, table_name: str, ref_table_name: str, ref_document_id: ObjectId
    ) -> str:
        return (
            f"Cannot delete {table_name} Table {document_id} because it is referenced by "
            f"{ref_table_name} Table {ref_document_id}"
        )

    async def _delete_batch_request_table(self) -> MaterializedTableModel:
        document = await self.app_container.batch_request_table_service.get_document(
            document_id=self.task_payload.document_id
        )
        reference_results = self.app_container.batch_feature_table_service.list_documents(
            query={"batch_request_table_id": document.id}
        )
        if reference_results["total"]:
            raise DocumentDeletionError(
                self._get_error_message(
                    document_id=document.id,
                    table_name="Batch Request",
                    ref_table_name="Batch Feature",
                    ref_document_id=reference_results["data"][0]["_id"],
                )
            )
        await self.app_container.batch_request_table_service.delete_document(
            document_id=document.id
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
        document = await self.app_container.observation_table_service.get_document(
            document_id=self.task_payload.document_id
        )
        reference_results = self.app_container.historical_feature_table_service.list_documents(
            query={"observation_table_id": document.id}
        )
        if reference_results["total"]:
            raise DocumentDeletionError(
                self._get_error_message(
                    document_id=document.id,
                    table_name="Observation",
                    ref_table_name="Historical Feature",
                    ref_document_id=reference_results["data"][0]["_id"],
                )
            )
        await self.app_container.observation_table_service.delete_document(document_id=document.id)
        return cast(MaterializedTableModel, document)

    async def _delete_historical_feature_table(self) -> MaterializedTableModel:
        document = await self.app_container.historical_feature_table_service.get_document(
            document_id=self.task_payload.document_id
        )
        await self.app_container.historical_feature_table_service.delete_document(
            document_id=document.id
        )
        return cast(MaterializedTableModel, document)

    async def execute(self) -> Any:
        """
        Execute Deployment Create & Update Task
        """
        payload = cast(MaterializedTableDeleteTaskPayload, self.payload)
        table_action_pairs = [
            (MaterializedTableCollectionName.BATCH_REQUEST, self._delete_batch_request_table),
            (MaterializedTableCollectionName.BATCH_FEATURE, self._delete_batch_feature_table),
            (MaterializedTableCollectionName.OBSERVATION, self._delete_observation_table),
            (
                MaterializedTableCollectionName.HISTORICAL_FEATURE,
                self._delete_historical_feature_table,
            ),
        ]

        for table_name, delete_table in table_action_pairs:
            if payload.collection_name == table_name:
                # delete document stored at mongo
                deleted_document = await delete_table()

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
