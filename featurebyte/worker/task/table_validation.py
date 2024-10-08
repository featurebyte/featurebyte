"""
TableValidationTask class
"""

from __future__ import annotations

from typing import Any, Union

from featurebyte.enum import TableDataType
from featurebyte.logging import get_logger
from featurebyte.schema.worker.task.table_validation import (
    TableValidationTaskPayload,
)
from featurebyte.service.dimension_table_validation import DimensionTableValidationService
from featurebyte.service.event_table_validation import EventTableValidationService
from featurebyte.service.item_table_validation import ItemTableValidationService
from featurebyte.service.scd_table_validation import SCDTableValidationService
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseTask

logger = get_logger(__name__)

TableValidationServiceType = Union[
    SCDTableValidationService,
    EventTableValidationService,
    ItemTableValidationService,
    DimensionTableValidationService,
]


class TableValidationTask(BaseTask[TableValidationTaskPayload]):
    """
    TableValidationTask class
    """

    payload_class = TableValidationTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        scd_table_validation_service: SCDTableValidationService,
        event_table_validation_service: EventTableValidationService,
        item_table_validation_service: ItemTableValidationService,
        dimension_table_validation_service: DimensionTableValidationService,
    ):
        super().__init__(task_manager=task_manager)
        self.scd_table_validation_service = scd_table_validation_service
        self.event_table_validation_service = event_table_validation_service
        self.item_table_validation_service = item_table_validation_service
        self.dimension_table_validation_service = dimension_table_validation_service

    async def get_task_description(self, payload: TableValidationTaskPayload) -> str:
        return f'Validate table "{payload.table_name}"'

    async def execute(self, payload: TableValidationTaskPayload) -> Any:
        validation_service_mapping: dict[str, TableValidationServiceType] = {
            TableDataType.SCD_TABLE: self.scd_table_validation_service,
            TableDataType.EVENT_TABLE: self.event_table_validation_service,
            TableDataType.ITEM_TABLE: self.item_table_validation_service,
            TableDataType.DIMENSION_TABLE: self.dimension_table_validation_service,
        }
        if payload.table_type in validation_service_mapping:
            service = validation_service_mapping[payload.table_type]
            await service.validate_and_update(
                table_id=payload.table_id,
                task_id=str(self.task_id) if self.task_id is not None else None,
            )
