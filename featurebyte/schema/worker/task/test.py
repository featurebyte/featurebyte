"""
TestTaskPayload schema
"""

from typing import Optional

from featurebyte.enum import WorkerCommand
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class TestTaskPayload(BaseTaskPayload):
    """
    Test CPU Task Payload
    """

    command: WorkerCommand = WorkerCommand.TEST
    task_type: TaskType = TaskType.CPU_TASK
    sleep: int = 0
    is_revocable: bool = True

    @property
    def task_output_path(self) -> Optional[str]:
        """
        Redirect route used to retrieve the task result

        Returns
        -------
        Optional[str]
        """
        return None


class TestIOTaskPayload(BaseTaskPayload):
    """
    Test IO Task Payload
    """

    command: WorkerCommand = WorkerCommand.TEST
    task_type: TaskType = TaskType.IO_TASK

    @property
    def task_output_path(self) -> Optional[str]:
        """
        Redirect route used to retrieve the task result

        Returns
        -------
        Optional[str]
        """
        return None
