"""
OnlineStoreInitializeTaskPayload schema
"""

from typing import ClassVar, Optional

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.catalog import CatalogModel
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class CatalogOnlineStoreInitializeTaskPayload(BaseTaskPayload):
    """
    Online store initialize task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.CATALOG_ONLINE_STORE_UPDATE
    output_collection_name: ClassVar[str] = CatalogModel.collection_name()

    # instance variables
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
    online_store_id: Optional[PydanticObjectId]
    populate_offline_feature_tables: Optional[bool] = Field(default=None)
