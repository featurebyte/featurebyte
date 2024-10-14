"""
TableValidationTask class
"""

from __future__ import annotations

from typing import Any

from featurebyte.logging import get_logger
from featurebyte.schema.worker.task.table_validation import TableValidationTaskPayload
from featurebyte.service.table_facade import TableFacadeService
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
        table_facade_service: TableFacadeService,
    ):
        super().__init__(task_manager=task_manager)
        self.table_facade_service = table_facade_service

    async def get_task_description(self, payload: TableValidationTaskPayload) -> str:
        return f'Validate table "{payload.table_name}"'

    async def execute(self, payload: TableValidationTaskPayload) -> Any:
        service = self.table_facade_service.get_specific_table_validation_service(
            payload.table_type
        )
        await service.validate_and_update(table_id=payload.table_id)
