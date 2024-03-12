"""
ScheduledFeatureMaterializeTaskPayload schema
"""
from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class ScheduledFeatureMaterializeTaskPayload(BaseTaskPayload):
    """
    Scheduled feature materialize task payload
    """

    command = WorkerCommand.SCHEDULED_FEATURE_MATERIALIZE
    offline_store_feature_table_name: str
    offline_store_feature_table_id: PydanticObjectId
    task_type = TaskType.CPU_TASK
