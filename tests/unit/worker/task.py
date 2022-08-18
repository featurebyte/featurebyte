"""
Tasks used for testing purpose
"""
import time
from enum import Enum

from featurebyte.worker.task.base import TASK_MAP, BaseTask, BaseTaskPayload


class TaskExecutor:
    """TaskExecutor class"""

    def __init__(self, payload):
        command = payload["command"]
        task = TASK_MAP[command](payload=payload)
        task.execute()


class Command(str, Enum):
    """Command enum used for testing"""

    LONG_RUNNING_COMMAND = "long_running_command"
    ERROR_COMMAND = "error_command"


class LongRunningPayload(BaseTaskPayload):
    """LongRunningPayload class"""

    collection_name = "long_running_result_collection"
    command = Command.LONG_RUNNING_COMMAND


class LongRunningTask(BaseTask):
    """LongRunningTask class"""

    payload_class = LongRunningPayload

    def execute(self) -> None:
        """Delay for 1 second to simulate long-running task"""
        time.sleep(1)


class ErrorTaskPayload(BaseTaskPayload):
    """LongRunningPayload class"""

    collection_name = "anything"
    command = Command.ERROR_COMMAND


class ErrorTask(BaseTask):
    """LongRunningTask class"""

    payload_class = ErrorTaskPayload

    def execute(self) -> None:
        """Make it error"""
        assert 0
