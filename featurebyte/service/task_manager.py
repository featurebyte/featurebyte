"""
TaskManager service is responsible to submit task message
"""
from __future__ import annotations

from typing import Optional, Union

from abc import abstractmethod
from uuid import UUID

from bson.objectid import ObjectId

from featurebyte.schema.task import Task
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.worker.process_store import ProcessStore

TaskId = Union[ObjectId, UUID]


class AbstractTaskManager:
    """
    AbstractTaskManager defines interface for TaskManager
    """

    def __init__(self, user_id: Optional[ObjectId]):
        """
        TaskManager constructor

        Parameters
        ----------
        user_id: Optional[ObjectId]
            User ID
        """
        self.user_id = user_id

    @abstractmethod
    async def submit(self, payload: BaseTaskPayload) -> TaskId:
        """
        Submit task request given task payload

        Parameters
        ----------
        payload: BaseTaskPayload
            Task payload object

        Returns
        -------
        TaskId
            Task identifier used to check task status
        """

    @abstractmethod
    async def get_task(self, task_id: str) -> Optional[Task]:
        """
        Retrieve task status given ID

        Parameters
        ----------
        task_id: str
            Task ID

        Returns
        -------
        Task
        """

    @abstractmethod
    async def list_tasks(
        self,
        page: int = 1,
        page_size: int = 10,
        ascending: bool = True,
    ) -> tuple[list[Task], int]:
        """
        List task statuses of this user

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Page size
        ascending: bool
            Sorting order

        Returns
        -------
        tuple[list[Task], int]
        """


class TaskManager(AbstractTaskManager):
    """
    TaskManager class is responsible for submitting task request & task status retrieval
    """

    async def submit(self, payload: BaseTaskPayload) -> TaskId:
        assert self.user_id == payload.user_id
        task_id = await ProcessStore().submit(
            payload=payload.json(), output_path=payload.task_output_path
        )
        return task_id

    async def get_task(self, task_id: str) -> Optional[Task]:
        process_store = ProcessStore()
        process_dict = await process_store.get(user_id=self.user_id, task_id=ObjectId(task_id))
        return Task(**process_dict) if process_dict else None

    async def list_tasks(
        self,
        page: int = 1,
        page_size: int = 10,
        ascending: bool = True,
    ) -> tuple[list[Task], int]:
        output = []
        process_store = ProcessStore()
        for _, process_data_dict in await process_store.list(user_id=self.user_id):
            if process_data_dict:
                output.append(Task(**process_data_dict))

        output = sorted(output, key=lambda ts: ts.id, reverse=not ascending)
        start_idx = (page - 1) * page_size
        return output[start_idx : (start_idx + page_size)], len(output)
