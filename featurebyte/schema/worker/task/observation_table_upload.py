"""
ObservationTableUploadTaskPayload schema
"""
from __future__ import annotations

from featurebyte.enum import WorkerCommand
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.schema.observation_table import ObservationTableUpload
from featurebyte.schema.worker.task.base import BaseTaskPayload


class ObservationTableUploadTaskPayload(BaseTaskPayload, ObservationTableUpload):
    """
    ObservationTableUploadTaskPayload creation task payload
    """

    output_collection_name = ObservationTableModel.collection_name()
    command = WorkerCommand.OBSERVATION_TABLE_UPLOAD
    observation_set_storage_path: str
