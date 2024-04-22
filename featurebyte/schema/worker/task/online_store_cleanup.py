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

    command = WorkerCommand.ONLINE_STORE_TABLE_CLEANUP
    priority = TaskPriority.CRITICAL

    feature_store_id: PydanticObjectId
    online_store_table_name: str
