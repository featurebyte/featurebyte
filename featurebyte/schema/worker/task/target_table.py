"""
Target table task payload
"""

from typing import Any, ClassVar, Optional

from pydantic import BaseModel, Field, model_validator

from featurebyte.enum import WorkerCommand
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.schema.target_table import TargetTableCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class TargetTableTaskPayload(BaseTaskPayload, TargetTableCreate):
    """
    TargetTable creation task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.TARGET_TABLE_CREATE
    output_collection_name: ClassVar[str] = ObservationTableModel.collection_name()

    # instance variables
    observation_set_storage_path: Optional[str] = Field(default=None)
    task_type: TaskType = Field(default=TaskType.CPU_TASK)

    @model_validator(mode="before")
    @classmethod
    def _check_observation_data(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        observation_set_storage_path = values.get("observation_set_storage_path", None)
        observation_table_id = values.get("observation_table_id", None)
        if observation_table_id is None and observation_set_storage_path is None:
            raise ValueError(
                "Either observation_table_id or observation_set_storage_path must be provided"
            )
        return values
