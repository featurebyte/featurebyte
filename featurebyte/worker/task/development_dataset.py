"""
DevelopmentDataset creation task
"""

from __future__ import annotations

from typing import Any

from featurebyte.logging import get_logger
from featurebyte.schema.development_dataset import (
    DevelopmentDatasetCreate,
    DevelopmentDatasetServiceUpdate,
)
from featurebyte.schema.worker.task.development_dataset import (
    DevelopmentDatasetAddTablesTaskPayload,
    DevelopmentDatasetCreateTaskPayload,
    DevelopmentDatasetDeleteTaskPayload,
)
from featurebyte.service.development_dataset import DevelopmentDatasetService
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class DevelopmentDatasetCreateTask(
    DataWarehouseMixin, BaseTask[DevelopmentDatasetCreateTaskPayload]
):
    """
    DevelopmentDatasetCreate Task
    """

    payload_class = DevelopmentDatasetCreateTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        development_dataset_service: DevelopmentDatasetService,
    ):
        super().__init__(task_manager=task_manager)
        self.development_dataset_service = development_dataset_service

    async def get_task_description(self, payload: DevelopmentDatasetCreateTaskPayload) -> str:
        return f'Create development dataset "{payload.name}"'

    async def execute(self, payload: DevelopmentDatasetCreateTaskPayload) -> Any:
        payload_dict = payload.model_dump(by_alias=True)
        payload_dict["_id"] = payload.output_document_id
        await self.development_dataset_service.create_document(
            DevelopmentDatasetCreate(**payload_dict)
        )


class DevelopmentDatasetDeleteTask(
    DataWarehouseMixin, BaseTask[DevelopmentDatasetDeleteTaskPayload]
):
    """
    DevelopmentDatasetDelete Task
    """

    payload_class = DevelopmentDatasetDeleteTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        development_dataset_service: DevelopmentDatasetService,
    ):
        super().__init__(task_manager=task_manager)
        self.development_dataset_service = development_dataset_service

    async def get_task_description(self, payload: DevelopmentDatasetDeleteTaskPayload) -> str:
        development_dataset = await self.development_dataset_service.get_document(
            document_id=payload.output_document_id
        )
        return f'Delete development dataset "{development_dataset.name}"'

    async def execute(self, payload: DevelopmentDatasetDeleteTaskPayload) -> Any:
        await self.development_dataset_service.delete_document(
            document_id=payload.output_document_id
        )


class DevelopmentDatasetAddTablesTask(
    DataWarehouseMixin, BaseTask[DevelopmentDatasetAddTablesTaskPayload]
):
    """
    DevelopmentDatasetAddTables Task
    """

    payload_class = DevelopmentDatasetAddTablesTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        development_dataset_service: DevelopmentDatasetService,
    ):
        super().__init__(task_manager=task_manager)
        self.development_dataset_service = development_dataset_service

    async def get_task_description(self, payload: DevelopmentDatasetAddTablesTaskPayload) -> str:
        development_dataset = await self.development_dataset_service.get_document(
            document_id=payload.output_document_id
        )
        return f'Add tables to development dataset "{development_dataset.name}"'

    async def execute(self, payload: DevelopmentDatasetAddTablesTaskPayload) -> Any:
        await self.development_dataset_service.update_document(
            document_id=payload.output_document_id,
            data=DevelopmentDatasetServiceUpdate(
                development_tables=payload.development_tables,
            ),
        )
