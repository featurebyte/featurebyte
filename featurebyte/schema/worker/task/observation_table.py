"""
ObservationTableTaskPayload schema
"""

from __future__ import annotations

from typing import ClassVar, Optional

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.schema.observation_table import ObservationTableCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class ObservationTableTaskPayload(BaseTaskPayload, ObservationTableCreate):
    """
    ObservationTable creation task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.OBSERVATION_TABLE_CREATE
    output_collection_name: ClassVar[str] = ObservationTableModel.collection_name()
    is_revocable: ClassVar[bool] = True
    is_rerunnable: ClassVar[bool] = True

    # instance variables
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
    target_namespace_id: Optional[PydanticObjectId] = Field(default=None)
    # internal use only
    to_add_row_index: bool = Field(default=True)
