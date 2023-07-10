from typing import Optional

from featurebyte.enum import WorkerCommand
from featurebyte.models.target_table import TargetTableModel
from featurebyte.schema.target_table import TargetTableCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload


class TargetTableTaskPayload(BaseTaskPayload, TargetTableCreate):
    """
    TargetTable creation task payload
    """

    output_collection_name = TargetTableModel.collection_name()
    command = WorkerCommand.TARGET_TABLE_CREATE
    observation_set_storage_path: Optional[str]
