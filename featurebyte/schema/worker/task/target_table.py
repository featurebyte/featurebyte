"""
Target table task payload
"""
from typing import Optional

from featurebyte.enum import WorkerCommand
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.schema.target_table import TargetTableCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload


class TargetTableTaskPayload(BaseTaskPayload, TargetTableCreate):
    """
    TargetTable creation task payload
    """

    output_collection_name = ObservationTableModel.collection_name()
    command = WorkerCommand.TARGET_TABLE_CREATE
    observation_set_storage_path: Optional[str]
