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
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.static_source_table import StaticSourceTableService
from featurebyte.service.table import TableService
from featurebyte.service.target_table import TargetTableService
from featurebyte.service.task_manager import TaskManager
from featurebyte.service.validator.materialized_table_delete import (
    ObservationTableDeleteValidator,
    check_delete_batch_request_table,
    check_delete_static_source_table,
)
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin


class MaterializedTableDeleteTask(DataWarehouseMixin, BaseTask[MaterializedTableDeleteTaskPayload]):
    """
    Materialized Table Delete Task
    """

    payload_class = MaterializedTableDeleteTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        batch_request_table_service: BatchRequestTableService,
        batch_feature_table_service: BatchFeatureTableService,
        observation_table_service: ObservationTableService,
        historical_feature_table_service: HistoricalFeatureTableService,
        static_source_table_service: StaticSourceTableService,
        target_table_service: TargetTableService,
        feature_store_service: FeatureStoreService,
        observation_table_delete_validator: ObservationTableDeleteValidator,
        table_service: TableService,
        session_manager_service: SessionManagerService,
    ):
        super().__init__(task_manager=task_manager)
        self.batch_request_table_service = batch_request_table_service
        self.batch_feature_table_service = batch_feature_table_service
        self.observation_table_service = observation_table_service
        self.historical_feature_table_service = historical_feature_table_service
        self.static_source_table_service = static_source_table_service
        self.target_table_service = target_table_service
        self.feature_store_service = feature_store_service
        self.observation_table_delete_validator = observation_table_delete_validator
        self.table_service = table_service
        self.session_manager_service = session_manager_service

    async def get_task_description(self, payload: MaterializedTableDeleteTaskPayload) -> str:
        service_map = {
            MaterializedTableCollectionName.BATCH_REQUEST: self.batch_request_table_service,
            MaterializedTableCollectionName.BATCH_FEATURE: self.batch_feature_table_service,
            MaterializedTableCollectionName.OBSERVATION: self.observation_table_service,
            MaterializedTableCollectionName.HISTORICAL_FEATURE: self.historical_feature_table_service,
            MaterializedTableCollectionName.STATIC_SOURCE: self.static_source_table_service,
        }
        service = service_map[payload.collection_name]
        materialized_table = await service.get_document(document_id=payload.document_id)  # type: ignore[attr-defined]
        description = payload.collection_name.replace("_", " ")
        if not description.endswith(" table"):
            description = f"{description} table"
        return f'Delete {description} "{materialized_table.name}"'

    async def _delete_batch_request_table(
        self, payload: MaterializedTableDeleteTaskPayload
    ) -> MaterializedTableModel:
        document = await check_delete_batch_request_table(
            batch_request_table_service=self.batch_request_table_service,
            batch_feature_table_service=self.batch_feature_table_service,
            document_id=payload.document_id,
        )
        await self._delete_table_at_data_warehouse(document)
        await self.batch_request_table_service.delete_document(document_id=payload.document_id)
        return cast(MaterializedTableModel, document)

    async def _delete_batch_feature_table(
        self, payload: MaterializedTableDeleteTaskPayload
    ) -> MaterializedTableModel:
        document = await self.batch_feature_table_service.get_document(
            document_id=payload.document_id
        )
        await self._delete_table_at_data_warehouse(document)
        await self.batch_feature_table_service.delete_document(document_id=document.id)
        return cast(MaterializedTableModel, document)

    async def _delete_observation_table(
        self, payload: MaterializedTableDeleteTaskPayload
    ) -> MaterializedTableModel:
        validator: ObservationTableDeleteValidator = self.observation_table_delete_validator
        document = await validator.check_delete_observation_table(
            observation_table_id=payload.document_id,
        )
        await self._delete_table_at_data_warehouse(document)
        await self.observation_table_service.delete_document(document_id=payload.document_id)
        return cast(MaterializedTableModel, document)

    async def _delete_historical_feature_table(
        self, payload: MaterializedTableDeleteTaskPayload
    ) -> MaterializedTableModel:
        document = await self.historical_feature_table_service.get_document(
            document_id=payload.document_id
        )
        await self._delete_table_at_data_warehouse(document)
        await self.historical_feature_table_service.delete_document(document_id=document.id)
        return cast(MaterializedTableModel, document)

    async def _delete_static_source_table(
        self, payload: MaterializedTableDeleteTaskPayload
    ) -> MaterializedTableModel:
        document = await check_delete_static_source_table(
            static_source_table_service=self.static_source_table_service,
            table_service=self.table_service,
            document_id=payload.document_id,
        )
        await self._delete_table_at_data_warehouse(document)
        await self.static_source_table_service.delete_document(document_id=payload.document_id)
        return cast(MaterializedTableModel, document)

    async def _delete_table_at_data_warehouse(self, document: MaterializedTableModel) -> None:
        # delete table stored at data warehouse
        feature_store = await self.feature_store_service.get_document(
            document_id=document.location.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)

        for warehouse_table in document.warehouse_tables:
            await db_session.drop_table(
                table_name=warehouse_table.table_name,
                schema_name=warehouse_table.schema_name,  # type: ignore
                database_name=warehouse_table.database_name,  # type: ignore
            )

    async def execute(self, payload: MaterializedTableDeleteTaskPayload) -> Any:
        # table to delete action mapping
        table_to_delete_action = {
            MaterializedTableCollectionName.BATCH_REQUEST: self._delete_batch_request_table,
            MaterializedTableCollectionName.BATCH_FEATURE: self._delete_batch_feature_table,
            MaterializedTableCollectionName.OBSERVATION: self._delete_observation_table,
            MaterializedTableCollectionName.HISTORICAL_FEATURE: self._delete_historical_feature_table,
            MaterializedTableCollectionName.STATIC_SOURCE: self._delete_static_source_table,
        }

        # delete document stored at mongo
        await table_to_delete_action[payload.collection_name](payload)
