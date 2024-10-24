"""
TestTaskPayload schema
"""

from typing import ClassVar, Optional

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class TestTaskPayload(BaseTaskPayload):
    """
    Test CPU Task Payload
    """

    # add this to fix PytestCollectionWarning
    __test__ = False

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.TEST
    is_revocable: ClassVar[bool] = True
    is_rerunnable: ClassVar[bool] = True

    # instance variables
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
    sleep: int = Field(default=0)
    run_child_task: bool = Field(default=False)

    @property
    def task_output_path(self) -> Optional[str]:
        """
        Redirect route used to retrieve the task result

        Returns
        -------
        Optional[str]
        """
        return None


class TestIOTaskPayload(TestTaskPayload):
    """
    Test IO Task Payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.IO_TEST
    is_revocable: ClassVar[bool] = False
    is_rerunnable: ClassVar[bool] = False

    # instance variables
    task_type: TaskType = Field(default=TaskType.IO_TASK)
