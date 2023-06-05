"""
TestTaskPayload schema
"""
from typing import Optional

from featurebyte.enum import WorkerCommand
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class TestTaskPayload(BaseTaskPayload):
    """
    Test Task Payload
    """

    command = WorkerCommand.TEST
    task_type = TaskType.CPU_TASK

    @property
    def task_output_path(self) -> Optional[str]:
        """
        Redirect route used to retrieve the task result

        Returns
        -------
        Optional[str]
        """
        return None
