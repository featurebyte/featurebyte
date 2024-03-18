"""
TestTaskPayload schema
"""
from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.tile import TileScheduledJobParameters
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskPriority


class TileTaskPayload(BaseTaskPayload):
    """
    Tile Task Payload
    """

    command = WorkerCommand.TILE_COMPUTE
    priority = TaskPriority.CRITICAL

    feature_store_id: PydanticObjectId
    parameters: TileScheduledJobParameters
