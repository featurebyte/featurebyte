"""
Task progress updater
"""

import asyncio
import os
from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from featurebyte.models.task import ProgressHistory, Task
from featurebyte.schema.worker.progress import ProgressModel

MAX_PROGRESS_HISTORY = 500


class TaskProgressUpdater:
    """
    Update progress for a task
    """

    def __init__(self, persistent: Any, task_id: UUID, user: Any, progress: Any):
        self.persistent = persistent
        self.task_id = task_id
        self.user = user
        self.progress = progress

    @property
    def max_progress_history(self) -> int:
        """
        Maximum progress history

        Returns
        -------
        int
            Maximum progress history
        """
        return int(os.getenv("FEATUREBYTE_MAX_PROGRESS_HISTORY", MAX_PROGRESS_HISTORY))

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
        progress_dict = progress.model_dump(exclude_none=True)

        # write to persistent
        await self.persistent.update_one(
            collection_name=Task.collection_name(),
            query_filter={"_id": str(self.task_id)},
            update={
                "$set": {"progress": progress_dict},
                "$push": {
                    "progress_history.data": {
                        "percent": progress.percent,
                        "message": progress.message,
                        "timestamp": datetime.utcnow(),
                    }
                },
            },
            disable_audit=True,
            user_id=self.user.id,
        )

        # retrieve progress history & check if it needs to be compressed
        tasks = []
        result = await self.persistent.find_one(
            collection_name=Task.collection_name(),
            query_filter={"_id": str(self.task_id)},
            projection={"progress_history": 1},
        )
        if result and result.get("progress_history"):
            progress_history = ProgressHistory(**result["progress_history"])
            if len(progress_history.data) > self.max_progress_history:
                progress_history = progress_history.compress(max_messages=self.max_progress_history)
                tasks.append(
                    self.persistent.update_one(
                        collection_name=Task.collection_name(),
                        query_filter={"_id": str(self.task_id)},
                        update={"$set": {"progress_history": progress_history.model_dump()}},
                        disable_audit=True,
                        user_id=self.user.id,
                    )
                )

        if self.progress:
            # publish to redis
            tasks.append(self.progress.put(progress_dict))

        if tasks:
            await asyncio.gather(*tasks)
