"""
Base models for task and task payload
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from abc import abstractmethod
from enum import Enum
from uuid import UUID

from featurebyte.logging import get_logger
from featurebyte.models.base import User
from featurebyte.models.task import Task
from featurebyte.persistent import Persistent
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.schema.worker.progress import ProgressModel
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.storage import Storage

TASK_MAP: Dict[Enum, type[BaseTask]] = {}


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
        get_credential: Any,
        app_container: LazyAppContainer,
    ):
        if self.payload_class == BaseTaskPayload:
            raise NotImplementedError
        self.task_id = task_id
        self.payload = self.payload_class(**payload)
        self.get_credential = get_credential
        self.progress = progress
        self.app_container = app_container

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        if cls.payload_class.command is None:
            # handle the case where the command is not defined (e.g. abstract class)
            return

        assert isinstance(cls.payload_class.command, Enum)
        command = cls.payload_class.command
        if command in TASK_MAP:
            logger.warning("Existing task command overridden.", extra={"command": command.value})
        TASK_MAP[command] = cls

    @property
    def user(self) -> User:
        """
        Get user instance

        Returns
        -------
        User
        """
        return self.app_container.user  # type: ignore

    @property
    def persistent(self) -> Persistent:
        """
        Get persistent instance

        Returns
        -------
        Persistent
        """
        return self.app_container.persistent  # type: ignore

    @property
    def storage(self) -> Storage:
        """
        Get storage instance

        Returns
        -------
        Storage
        """
        return self.app_container.storage  # type: ignore

    @property
    def temp_storage(self) -> Storage:
        """
        Get temp storage instance

        Returns
        -------
        Storage
        """
        return self.app_container.temp_storage  # type: ignore

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

    async def execute(self) -> Any:
        """
        Execute the task

        Returns
        -------
        Any
        """
        lock = self.app_container.redis.lock(self.lock_key, timeout=self.lock_timeout)
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
