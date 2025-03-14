"""
Table validation task payload schema
"""

from __future__ import annotations

from typing import ClassVar, Literal

from featurebyte.enum import TableDataType, WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.schema.worker.task.base import BaseTaskPayload


class TableValidationTaskPayload(BaseTaskPayload):
    """
    TableValidationTaskPayload class
    """

    command: ClassVar[WorkerCommand] = WorkerCommand.TABLE_VALIDATION
    table_name: str
    table_type: Literal[
        TableDataType.EVENT_TABLE,
        TableDataType.ITEM_TABLE,
        TableDataType.DIMENSION_TABLE,
        TableDataType.SCD_TABLE,
        TableDataType.TIME_SERIES_TABLE,
    ]
    table_id: PydanticObjectId
