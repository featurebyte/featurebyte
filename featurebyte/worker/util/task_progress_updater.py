"""
Task progress updater
"""

import asyncio
import os
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Generator, Optional
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
        self._from_to_percent: tuple[float, float] = (0, 100)

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

    @contextmanager
    def set_progress_range(
        self, from_percent: float, to_percent: float
    ) -> Generator[None, None, None]:
        """
        Context manager to temporarily set a progress range.
        The original range is automatically restored when exiting the context.

        Parameters
        ----------
        from_percent: float
            The starting percentage of the range (0-100)
        to_percent: float
            The ending percentage of the range (0-100)

        Yields
        ------
        None
            Sets the progress range within the context

        Raises
        ------
        ValueError
            If from_percent is not less than to_percent or if either value is out of bounds
        """
        if not (0 <= from_percent < to_percent <= 100):
            raise ValueError(
                "from_percent must be less than to_percent and both must be between 0 and 100"
            )

        # Save the current range
        original_range = self._from_to_percent
        try:
            # Set the new range
            self._from_to_percent = (from_percent, to_percent)
            yield
        finally:
            # Restore the original range
            self._from_to_percent = original_range

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
        scaled_percent = percent
        if self._from_to_percent != (0, 100):
            from_percent, to_percent = self._from_to_percent
            scaled_percent = int(from_percent + (percent / 100) * (to_percent - from_percent))

        progress = ProgressModel(percent=scaled_percent, message=message, **kwargs)
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
