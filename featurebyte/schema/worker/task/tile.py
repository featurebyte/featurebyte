"""
TestTaskPayload schema
"""
from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.tile import TileScheduledJobParameters
from featurebyte.schema.worker.task.base import BaseTaskPayload


class TileTaskPayload(BaseTaskPayload):
    """
    Tile Task Payload
    """

    command = WorkerCommand.TILE_COMPUTE
    priority: int = 0  # highest priority for production workload

    feature_store_id: PydanticObjectId
    parameters: TileScheduledJobParameters
