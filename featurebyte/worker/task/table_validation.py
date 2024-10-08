"""
TableValidationTask class
"""

from __future__ import annotations

from typing import Any

from featurebyte.enum import TableDataType
from featurebyte.logging import get_logger
from featurebyte.schema.worker.task.table_validation import (
    TableValidationTaskPayload,
)
from featurebyte.service.scd_table_validation import SCDTableValidationService
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseTask

logger = get_logger(__name__)


class TableValidationTask(BaseTask[TableValidationTaskPayload]):
    """
    TableValidationTask class
    """

    payload_class = TableValidationTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        scd_table_validation_service: SCDTableValidationService,
    ):
        super().__init__(task_manager=task_manager)
        self.scd_table_validation_service = scd_table_validation_service

    async def get_task_description(self, payload: TableValidationTaskPayload) -> str:
        return f'Validate table "{payload.table_name}"'

    async def execute(self, payload: TableValidationTaskPayload) -> Any:
        validation_service_mapping = {
            TableDataType.SCD_TABLE: self.scd_table_validation_service,
        }
        if payload.table_type in validation_service_mapping:
            service = validation_service_mapping[payload.table_type]
            await service.validate_and_update(table_id=payload.table_id)
