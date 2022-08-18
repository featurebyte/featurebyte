"""
TaskManager service is responsible to submit task message
"""
from __future__ import annotations

from typing import Optional

from multiprocessing import Process

from bson.objectid import ObjectId

from featurebyte.schema.task_status import TaskStatus
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.worker.process_store import ProcessStore


class TaskManager:
    """
    TaskManager class is responsible for submitting task request & task status retrieval
    """

    def __init__(self, user_id: Optional[ObjectId]):
        self.user_id = user_id

    def submit(self, payload: BaseTaskPayload) -> ObjectId:
        """
        Submit task request given task payload

        Parameters
        ----------
        payload: BaseTaskPayload
            Task payload object

        Returns
        -------
        ObjectId
            Task identifier used to check task status
        """
        assert self.user_id == payload.user_id
        task_id = ProcessStore().submit(payload=payload.dict())
        return task_id

    @staticmethod
    def _get_task_status(
        task_status_id: ObjectId, process: Optional[Process]
    ) -> Optional[TaskStatus]:
        if process is None:
            return None
        if process.exitcode is None:
            return TaskStatus(id=task_status_id, status="running")
        if process.exitcode == 0:
            return TaskStatus(id=task_status_id, status="complete")
        return TaskStatus(id=task_status_id, status="error")

    def get_task_status(self, task_status_id: ObjectId) -> Optional[TaskStatus]:
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
        process_store = ProcessStore()
        return self._get_task_status(
            task_status_id=task_status_id,
            process=process_store.get(user_id=self.user_id, task_status_id=task_status_id),
        )

    def list_task_status(
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
        output = []
        for task_status_id, process in ProcessStore().list(user_id=self.user_id):
            task_status = self._get_task_status(task_status_id, process)
            if task_status:
                output.append(task_status)

        output = sorted(output, key=lambda ts: ts.id, reverse=not ascending)
        start_idx = (page - 1) * page_size
        return output[start_idx : (start_idx + page_size)], len(output)
