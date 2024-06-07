"""
TestTaskPayload schema
"""

from typing import ClassVar

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.tile import TileScheduledJobParameters
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskPriority


class TileTaskPayload(BaseTaskPayload):
    """
    Tile Task Payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.TILE_COMPUTE

    # instance variables
    priority: TaskPriority = Field(default=TaskPriority.CRITICAL)
    feature_store_id: PydanticObjectId
    parameters: TileScheduledJobParameters
