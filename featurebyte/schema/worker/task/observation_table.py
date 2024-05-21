"""
ObservationTableTaskPayload schema
"""

from __future__ import annotations

from typing import Optional

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

    output_collection_name = ObservationTableModel.collection_name()
    command = WorkerCommand.OBSERVATION_TABLE_CREATE
    target_namespace_id: Optional[PydanticObjectId]
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
