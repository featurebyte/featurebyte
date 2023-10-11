"""
Random task util. Mainly used in tests, but placing in src so that we can register for DI.
"""
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
