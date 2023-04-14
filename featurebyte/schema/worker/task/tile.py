"""
TestTaskPayload schema
"""
from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.schema.worker.task.base import BaseTaskPayload


class TileTaskPayload(BaseTaskPayload):
    """
    Tile Task Payload
    """

    command = WorkerCommand.TILE_COMPUTE

    feature_store_id: PydanticObjectId
    module_path: str
    class_name: str
    instance_str: str
