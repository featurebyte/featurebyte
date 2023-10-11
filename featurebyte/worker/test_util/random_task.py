"""
Random task util. Mainly used in tests, but placing in src so that we can register for DI.
"""
import time

from featurebyte.enum import StrEnum
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.worker.task.base import BaseTask


class TestCommand(StrEnum):
    """Command enum used for testing"""

    RANDOM_COMMAND = "random_command"


class RandomTaskPayload(BaseTaskPayload):
    """RandomTaskPayload class"""

    output_collection_name = "random_collection"
    command = TestCommand.RANDOM_COMMAND


class RandomTask(BaseTask):
    """RandomTask class"""

    payload_class = RandomTaskPayload

    async def get_task_description(self) -> str:
        return "Execute random task"

    async def execute(self) -> None:
        """Run some task"""
        await self.persistent.insert_one(
            collection_name="random_collection",
            document={
                "_id": self.payload.output_document_id,
                "user_id": self.user.id,
                "output_document_id": self.payload.output_document_id,
            },
            user_id=self.user.id,
        )


class Command(StrEnum):
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
