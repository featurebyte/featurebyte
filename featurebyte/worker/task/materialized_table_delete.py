"""
Materialized Table Delete Task
"""
from __future__ import annotations

from typing import Any, cast

from uuid import UUID

from featurebyte.models.base import User
from featurebyte.models.materialized_table import MaterializedTableModel
from featurebyte.persistent import Persistent
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
from featurebyte.service.validator.materialized_table_delete import (
    ObservationTableDeleteValidator,
    check_delete_batch_request_table,
    check_delete_static_source_table,
)
from featurebyte.storage import Storage
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin


class MaterializedTableDeleteTask(DataWarehouseMixin, BaseTask):
    """
    Materialized Table Delete Task
    """

    payload_class = MaterializedTableDeleteTaskPayload

    def __init__(  # pylint: disable=too-many-arguments
        self,
        task_id: UUID,
        payload: dict[str, Any],
        progress: Any,
        user: User,
        persistent: Persistent,
        storage: Storage,
        temp_storage: Storage,
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
        super().__init__(
            task_id=task_id,
            payload=payload,
            progress=progress,
            user=user,
            persistent=persistent,
            storage=storage,
            temp_storage=temp_storage,
        )
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

    async def get_task_description(self) -> str:
        payload = cast(MaterializedTableDeleteTaskPayload, self.payload)
        service_map = {
            MaterializedTableCollectionName.BATCH_REQUEST: self.batch_request_table_service,
            MaterializedTableCollectionName.BATCH_FEATURE: self.batch_feature_table_service,
            MaterializedTableCollectionName.OBSERVATION: self.observation_table_service,
            MaterializedTableCollectionName.HISTORICAL_FEATURE: self.historical_feature_table_service,
            MaterializedTableCollectionName.STATIC_SOURCE: self.static_source_table_service,
            MaterializedTableCollectionName.TARGET: self.target_table_service,
        }
        service = service_map[payload.collection_name]
        materialized_table = await service.get_document(document_id=payload.document_id)
        description = payload.collection_name.replace("_", " ")
        if not description.endswith(" table"):
            description = f"{description} table"
        return f'Delete {description} "{materialized_table.name}"'

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
            batch_request_table_service=self.batch_request_table_service,
            batch_feature_table_service=self.batch_feature_table_service,
            document_id=self.task_payload.document_id,
        )
        await self.batch_request_table_service.delete_document(
            document_id=self.task_payload.document_id
        )
        return cast(MaterializedTableModel, document)

    async def _delete_batch_feature_table(self) -> MaterializedTableModel:
        document = await self.batch_feature_table_service.get_document(
            document_id=self.task_payload.document_id
        )
        await self.batch_feature_table_service.delete_document(document_id=document.id)
        return cast(MaterializedTableModel, document)

    async def _delete_observation_table(self) -> MaterializedTableModel:
        validator: ObservationTableDeleteValidator = self.observation_table_delete_validator
        document = await validator.check_delete_observation_table(
            observation_table_id=self.task_payload.document_id,
        )
        await self.observation_table_service.delete_document(
            document_id=self.task_payload.document_id
        )
        return cast(MaterializedTableModel, document)

    async def _delete_historical_feature_table(self) -> MaterializedTableModel:
        document = await self.historical_feature_table_service.get_document(
            document_id=self.task_payload.document_id
        )
        await self.historical_feature_table_service.delete_document(document_id=document.id)
        return cast(MaterializedTableModel, document)

    async def _delete_target_table(self) -> MaterializedTableModel:
        document = await self.target_table_service.get_document(
            document_id=self.task_payload.document_id
        )
        await self.target_table_service.delete_document(document_id=document.id)
        return cast(MaterializedTableModel, document)

    async def _delete_static_source_table(self) -> MaterializedTableModel:
        document = await check_delete_static_source_table(
            static_source_table_service=self.static_source_table_service,
            table_service=self.table_service,
            document_id=self.task_payload.document_id,
        )
        await self.static_source_table_service.delete_document(
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
        feature_store = await self.feature_store_service.get_document(
            document_id=deleted_document.location.feature_store_id
        )
        db_session = await self.get_db_session(feature_store=feature_store)
        await db_session.drop_table(
            table_name=deleted_document.location.table_details.table_name,
            schema_name=deleted_document.location.table_details.schema_name,  # type: ignore
            database_name=deleted_document.location.table_details.database_name,  # type: ignore
        )
        return
