"""
TestTaskPayload schema
"""
from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.schema.worker.task.base import BaseTaskPayload


class OnlineStoreCleanupTaskPayload(BaseTaskPayload):
    """
    Tile Task Payload
    """

    command = WorkerCommand.ONLINE_STORE_TABLE_CLEANUP
    feature_store_id: PydanticObjectId
    online_store_table_name: str
