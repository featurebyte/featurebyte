"""
Base models for task and task payload
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Generic, Optional, Type, TypeVar

from redis import Redis

from featurebyte.logging import get_logger
from featurebyte.schema.task import TaskId
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.service.task_manager import TaskManager

logger = get_logger(__name__)


TaskT = TypeVar("TaskT", bound=BaseTaskPayload)


class BaseTask(Generic[TaskT]):
    """
    Base class for Task
    """

    payload_class: Type[TaskT]

    def __init__(self, task_manager: TaskManager):
        self.task_manager = task_manager

        # track the task ID
        self._task_id: Optional[TaskId] = None

    @property
    def task_id(self) -> Optional[TaskId]:
        """
        Task ID

        Returns
        -------
        Optional[TaskId]
        """
        return self._task_id

    def set_task_id(self, task_id: TaskId) -> None:
        """
        Set task ID

        Parameters
        ----------
        task_id: TaskId
            Task ID
        """
        self._task_id = task_id

    async def submit_child_task(
        self, payload: BaseTaskPayload, mark_as_scheduled_task: bool = False
    ) -> str:
        """
        Submit the task to the task manager

        Parameters
        ----------
        payload: BaseTaskPayload
            Task payload
        mark_as_scheduled_task: bool
            Whether to mark the task as a scheduled task

        Returns
        -------
        str
        """
        assert self.task_id is not None, "Task ID is not set"
        task_id = await self.task_manager.submit(
            payload=payload,
            mark_as_scheduled_task=mark_as_scheduled_task,
            parent_task_id=str(self.task_id),
        )
        return task_id

    def get_payload_obj(self, payload_data: dict[str, Any]) -> TaskT:
        """
        Get payload object from payload data.

        Parameters
        ----------
        payload_data: dict[str, Any]
            Payload data

        Returns
        -------
        TaskT
        """
        return self.payload_class(**payload_data)

    @abstractmethod
    async def execute(self, payload: TaskT) -> Any:
        """
        Execute the task

        Parameters
        ----------
        payload: TaskT
            Task payload
        """

    async def handle_task_revoke(self, payload: TaskT) -> None:
        """
        Handle clean up on task revocation

        Parameters
        ----------
        payload: TaskT
            Task payload
        """

    @abstractmethod
    async def get_task_description(self, payload: TaskT) -> str:
        """
        Get the task description

        Parameters
        ----------
        payload: TaskT
            Task payload

        Returns
        -------
        str
        """
        raise NotImplementedError()


class BaseLockTask(BaseTask[TaskT]):
    """
    BaseLockTask is used to run a task with a lock. At most one task with the same lock
    can be executed at the same time. The lock is released when the task is finished.
    """

    def __init__(
        self,
        task_manager: TaskManager,
        redis: Redis[Any],
    ):
        super().__init__(task_manager=task_manager)
        self.redis = redis

    async def execute(self, payload: TaskT) -> Any:
        """
        Execute the task with special handling for locking.

        Parameters
        ----------
        payload: TaskT
            Task payload

        Returns
        -------
        Any
        """
        lock = self.redis.lock(self.lock_key(payload), timeout=self.lock_timeout)
        try:
            if lock.acquire(blocking=self.lock_blocking):
                return await self._execute(payload)

            # handle the case when the lock is not acquired
            return self.handle_lock_not_acquired(payload)
        finally:
            if lock.owned():
                lock.release()

    @abstractmethod
    def lock_key(self, payload: TaskT) -> str:
        """
        Key to lock the task. This is used to prevent multiple tasks with the same
        lock_key running at the same time.

        Parameters
        ----------
        payload: TaskT
            Task payload

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
    def handle_lock_not_acquired(self, payload: TaskT) -> Any:
        """
        Handle the case when the lock is not acquired. This method will be called when
        the lock is not acquired.

        Parameters
        ----------
        payload: TaskT
            Task payload

        Returns
        -------
        Any
        """

    @abstractmethod
    async def _execute(self, payload: TaskT) -> Any:
        """
        Execute the task when the lock is acquired

        Parameters
        ----------
        payload: TaskT
            Task payload

        Returns
        -------
        Any
        """
