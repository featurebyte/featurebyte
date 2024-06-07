"""
OnlineStoreCleanupTaskPayload schema
"""

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskPriority


class OnlineStoreCleanupTaskPayload(BaseTaskPayload):
    """
    Online store cleanup task payload
    """

    command: WorkerCommand = WorkerCommand.ONLINE_STORE_TABLE_CLEANUP
    priority: TaskPriority = TaskPriority.CRITICAL

    feature_store_id: PydanticObjectId
    online_store_table_name: str
