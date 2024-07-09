"""
ObservationTableUploadTaskPayload schema
"""

from __future__ import annotations

from typing import ClassVar, Optional

from pydantic import Field

from featurebyte.enum import UploadFileFormat, WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.schema.observation_table import ObservationTableUpload
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class ObservationTableUploadTaskPayload(BaseTaskPayload, ObservationTableUpload):
    """
    ObservationTableUploadTaskPayload creation task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.OBSERVATION_TABLE_UPLOAD
    output_collection_name: ClassVar[str] = ObservationTableModel.collection_name()

    # instance variables
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
    observation_set_storage_path: str
    file_format: UploadFileFormat
    uploaded_file_name: str  # the name of the file that was uploaded by the user
    target_namespace_id: Optional[PydanticObjectId] = Field(default=None)
