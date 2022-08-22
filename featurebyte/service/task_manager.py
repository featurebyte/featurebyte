"""
TaskManager service is responsible to submit task message
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from abc import abstractmethod

from bson.objectid import ObjectId

from featurebyte.schema.task_status import TaskStatus
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.worker.process_store import ProcessStore


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
    async def submit(self, payload: BaseTaskPayload) -> ObjectId:
        """
        Submit task request given task payload

        Parameters
        ----------
        payload: BaseTaskPayload
            Task payload object
        output_path: str
            Task output path

        Returns
        -------
        ObjectId
            Task identifier used to check task status
        """

    @abstractmethod
    async def get_task_status(self, task_status_id: ObjectId) -> Optional[TaskStatus]:
        """
        Retrieve task status given ID

        Parameters
        ----------
        task_status_id: ObjectId
            Task status ID

        Returns
        -------
        TaskStatus
        """

    @abstractmethod
    async def list_task_status(
        self,
        page: int = 1,
        page_size: int = 10,
        ascending: bool = True,
    ) -> tuple[list[TaskStatus], int]:
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
        tuple[list[TaskStatus], int]
        """


class TaskManager(AbstractTaskManager):
    """
    TaskManager class is responsible for submitting task request & task status retrieval
    """

    async def submit(self, payload: BaseTaskPayload) -> ObjectId:
        assert self.user_id == payload.user_id
        task_id = await ProcessStore().submit(
            payload=payload.json(), output_path=payload.task_output_path
        )
        return task_id

    @staticmethod
    def _get_task_status(
        task_status_id: ObjectId, process_data: Optional[Dict[str, Any]]
    ) -> Optional[TaskStatus]:
        if process_data is None:
            return None
        if process_data["process"].exitcode is None:
            status = "STARTED"
        elif process_data["process"].exitcode == 0:
            status = "SUCCESS"
        else:
            status = "FAILURE"
        return TaskStatus(
            id=task_status_id,
            status=status,
            output_path=process_data["output_path"],
            payload=process_data["payload"],
        )

    async def get_task_status(self, task_status_id: ObjectId) -> Optional[TaskStatus]:
        process_store = ProcessStore()
        process_dict = await process_store.get(user_id=self.user_id, task_status_id=task_status_id)
        return self._get_task_status(task_status_id=task_status_id, process_data=process_dict)

    async def list_task_status(
        self,
        page: int = 1,
        page_size: int = 10,
        ascending: bool = True,
    ) -> tuple[list[TaskStatus], int]:
        output = []
        for task_status_id, process_data in await ProcessStore().list(user_id=self.user_id):
            task_status = self._get_task_status(task_status_id, process_data)
            if task_status:
                output.append(task_status)

        output = sorted(output, key=lambda ts: ts.id, reverse=not ascending)
        start_idx = (page - 1) * page_size
        return output[start_idx : (start_idx + page_size)], len(output)
