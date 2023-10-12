"""
Base models for task and task payload
"""
from __future__ import annotations

from typing import Any, Optional

from abc import abstractmethod
from uuid import UUID

from redis import Redis

from featurebyte.logging import get_logger
from featurebyte.models.base import User
from featurebyte.models.task import Task
from featurebyte.persistent import Persistent
from featurebyte.schema.worker.progress import ProgressModel
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.storage import Storage

logger = get_logger(__name__)


class BaseTask:  # pylint: disable=too-many-instance-attributes
    """
    Base class for Task
    """

    payload_class: type[BaseTaskPayload] = BaseTaskPayload

    def __init__(  # pylint: disable=too-many-arguments
        self,
        task_id: UUID,
        payload: dict[str, Any],
        progress: Any,
        user: User,
        persistent: Persistent,
        storage: Storage,
        temp_storage: Storage,
    ):
        if self.payload_class == BaseTaskPayload:
            raise NotImplementedError
        self.task_id = task_id
        self.payload = self.payload_class(**payload)
        self.progress = progress
        self.user = user
        self.persistent = persistent
        self.storage = storage
        self.temp_storage = temp_storage

    async def update_progress(self, percent: int, message: str | None = None) -> None:
        """
        Update progress

        Parameters
        ----------
        percent: int
            Completed progress percentage
        message: str | None
            Optional message
        """
        progress = ProgressModel(percent=percent, message=message)
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

    @abstractmethod
    async def execute(self) -> Any:
        """
        Execute the task
        """

    @abstractmethod
    async def get_task_description(self) -> str:
        """
        Get the task description

        Returns
        -------
        str
        """
        raise NotImplementedError()


class BaseLockTask(BaseTask):
    """
    BaseLockTask is used to run a task with a lock. At most one task with the same lock
    can be executed at the same time. The lock is released when the task is finished.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        task_id: UUID,
        payload: dict[str, Any],
        progress: Any,
        user: User,
        persistent: Persistent,
        storage: Storage,
        temp_storage: Storage,
        redis: Redis[Any],
    ):
        super().__init__(
            task_id=task_id,
            payload=payload,
            progress=progress,
            user=user,
            persistent=persistent,
            storage=storage,
            temp_storage=temp_storage,
        )
        self.redis = redis

    async def execute(self) -> Any:
        """
        Execute the task

        Returns
        -------
        Any
        """
        lock = self.redis.lock(self.lock_key, timeout=self.lock_timeout)
        try:
            if lock.acquire(blocking=self.lock_blocking):
                return await self._execute()

            # handle the case when the lock is not acquired
            return self.handle_lock_not_acquired()
        finally:
            if lock.owned():
                lock.release()

    @property
    @abstractmethod
    def lock_key(self) -> str:
        """
        Key to lock the task. This is used to prevent multiple tasks with the same
        lock_key running at the same time.

        Returns
        -------
        str
        """

    @property
    @abstractmethod
    def lock_timeout(self) -> Optional[int]:
        """
        Lock timeout in seconds (optional). The lock will be released after the timeout.

        Returns
        -------
        Optional[int]
        """

    @property
    @abstractmethod
    def lock_blocking(self) -> bool:
        """
        Whether to block when acquiring the lock. If set to False, the task will be
        skipped if the lock is not acquired. Otherwise, the task will wait until the
        lock is acquired.

        Returns
        -------
        bool
        """

    @abstractmethod
    def handle_lock_not_acquired(self) -> Any:
        """
        Handle the case when the lock is not acquired. This method will be called when
        the lock is not acquired.
        """

    @abstractmethod
    async def _execute(self) -> Any:
        """
        Execute the task when the lock is acquired
        """
