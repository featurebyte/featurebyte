"""
Task progress updater
"""

from typing import Any, Optional

from uuid import UUID

from featurebyte.models.task import Task
from featurebyte.schema.worker.progress import ProgressModel


class TaskProgressUpdater:
    """
    Update progress for a task
    """

    def __init__(self, persistent: Any, task_id: UUID, user: Any, progress: Any):
        self.persistent = persistent
        self.task_id = task_id
        self.user = user
        self.progress = progress

    async def update_progress(
        self, percent: int, message: Optional[str] = None, **kwargs: Any
    ) -> None:
        """
        Update progress

        Parameters
        ----------
        percent: int
            Completed progress percentage
        message: str | None
            Optional message
        **kwargs: Any
            Optional parameters
        """
        progress = ProgressModel(percent=percent, message=message, **kwargs)
        progress_dict = progress.dict(exclude_none=True)

        # write to persistent
        await self.persistent.update_one(
            collection_name=Task.collection_name(),
            query_filter={"_id": str(self.task_id)},
            update={"$set": {"progress": progress_dict}},
            disable_audit=True,
            user_id=self.user.id,
        )

        if self.progress:
            # publish to redis
            self.progress.put(progress_dict)
