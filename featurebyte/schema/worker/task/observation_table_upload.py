"""
ObservationTableUploadTaskPayload schema
"""

from __future__ import annotations

from typing import Optional

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

    output_collection_name = ObservationTableModel.collection_name()
    command = WorkerCommand.OBSERVATION_TABLE_UPLOAD
    observation_set_storage_path: str
    file_format: UploadFileFormat
    # This is the name of the file that was uploaded by the user
    uploaded_file_name: str
    target_namespace_id: Optional[PydanticObjectId]
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
