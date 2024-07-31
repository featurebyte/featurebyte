"""
QueryCacheCleanupTaskPayload schema
"""

from typing import ClassVar

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskPriority


class QueryCacheCleanupTaskPayload(BaseTaskPayload):
    """
    Query cache cleanup task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.QUERY_CACHE_CLEANUP

    # instance variables
    priority: TaskPriority = Field(default=TaskPriority.CRITICAL)
    feature_store_id: PydanticObjectId
