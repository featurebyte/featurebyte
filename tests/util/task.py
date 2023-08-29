"""
Tasks used for testing purpose
"""
import time
from enum import Enum

from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task_executor import TaskExecutor as WorkerTaskExecutor


class TaskExecutor(WorkerTaskExecutor):
    """TaskExecutor class"""

    command_type = str


class Command(str, Enum):
    """Command enum used for testing"""

    LONG_RUNNING_COMMAND = "long_running_command"
    ERROR_COMMAND = "error_command"
    UNKNOWN_TASK_COMMAND = "unknown_task_command"


class LongRunningPayload(BaseTaskPayload):
    """LongRunningPayload class"""

    output_collection_name = "long_running_result_collection"
    command = Command.LONG_RUNNING_COMMAND


class LongRunningTask(BaseTask):
    """LongRunningTask class"""

    payload_class = LongRunningPayload

    async def get_task_description(self) -> str:
        return "Execute long running task"

    async def execute(self) -> None:
        """Delay for 1 second to simulate long-running task"""
        step = 10
        for i in range(step):
            time.sleep(1.0 / step)
            percent = int((i + 1) * (100.0 / step))
            await self.update_progress(percent=percent)


class ErrorTaskPayload(BaseTaskPayload):
    """ErrorTaskPayload class"""

    output_collection_name = "anything"
    command = Command.ERROR_COMMAND


class ErrorTask(BaseTask):
    """ErrorTask class"""

    payload_class = ErrorTaskPayload

    async def get_task_description(self) -> str:
        return "Execute task that errors"

    async def execute(self) -> None:
        """Make it error"""
        assert 0
