"""
OnlineStoreInitializeTaskPayload schema
"""

from typing import Optional

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.catalog import CatalogModel
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class CatalogOnlineStoreInitializeTaskPayload(BaseTaskPayload):
    """
    Online store initialize task payload
    """

    command = WorkerCommand.CATALOG_ONLINE_STORE_UPDATE
    online_store_id: Optional[PydanticObjectId]
    output_collection_name = CatalogModel.collection_name()
    task_type = TaskType.CPU_TASK
