"""
Base models for task and task payload
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from abc import abstractmethod
from enum import Enum

from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.registry import app_container_config
from featurebyte.schema.worker.progress import ProgressModel
from featurebyte.schema.worker.task.base import BaseTaskPayload

TASK_MAP: Dict[Enum, type[BaseTask]] = {}


class BaseTask:  # pylint: disable=too-many-instance-attributes
    """
    Base class for Task
    """

    payload_class: type[BaseTaskPayload] = BaseTaskPayload

    def __init__(
        self,
        payload: dict[str, Any],
        progress: Any,
        user: Any,
        get_persistent: Any,
        get_storage: Any,
        get_temp_storage: Any,
        get_credential: Any,
        get_celery: Any,
        get_redis: Any,
    ):
        if self.payload_class == BaseTaskPayload:
            raise NotImplementedError
        self.payload = self.payload_class(**payload)
        self.user = user
        self.get_persistent = get_persistent
        self.get_storage = get_storage
        self.get_temp_storage = get_temp_storage
        self.get_credential = get_credential
        self.get_celery = get_celery
        self.get_redis = get_redis
        self.progress = progress
        self._app_container: Optional[LazyAppContainer] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        if cls.payload_class.command is None:
            # handle the case where the command is not defined (e.g. abstract class)
            return

        assert isinstance(cls.payload_class.command, Enum)
        command = cls.payload_class.command
        if command in TASK_MAP:
            raise ValueError(f'Command "{command}" has been implemented.')
        TASK_MAP[command] = cls

    def update_progress(self, percent: int, message: str | None = None) -> None:
        """
        Update progress

        Parameters
        ----------
        percent: int
            Completed progress percentage
        message: str | None
            Optional message
        """
        if self.progress:
            progress = ProgressModel(percent=percent, message=message)
            self.progress.put(progress.dict(exclude_none=True))

    @property
    def app_container(self) -> LazyAppContainer:
        """
        Get an AppContainer instance

        Returns
        -------
        LazyAppContainer
        """
        if self._app_container is None:
            self._app_container = LazyAppContainer(
                user=self.user,
                persistent=self.get_persistent(),
                temp_storage=self.get_temp_storage(),
                celery=self.get_celery(),
                redis=self.get_redis(),
                storage=self.get_storage(),
                catalog_id=self.payload.catalog_id,
                app_container_config=app_container_config,
            )
        return self._app_container

    @abstractmethod
    async def execute(self) -> Any:
        """
        Execute the task
        """


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
    def lock_timeout(self) -> int:
        """
        Lock timeout in seconds

        Returns
        -------
        int
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
