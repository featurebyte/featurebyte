"""
TaskManager service is responsible to submit task message
"""
from __future__ import annotations

from typing import Any, Optional

from multiprocessing import Process

from bson.objectid import ObjectId

from featurebyte.schema.task_status import TaskStatus, TaskStatusList
from featurebyte.worker.enum import Command
from featurebyte.worker.process_store import ProcessStore


class TaskManager:
    """
    TaskManager class is responsible for submitting task request & task status retrieval
    """

    def __init__(self, user_id: Optional[ObjectId]):
        self.user_id = user_id

    def submit(
        self,
        command: Command,
        payload: dict[str, Any],
    ) -> tuple[ObjectId, ObjectId]:
        """
        Submit task request given task payload

        Parameters
        ----------
        command: Command
            Task command
        payload: dict[str, Any]
            Task payload

        Returns
        -------
        document_id
            Identifier used to identify the task output
        task_id
            Task identifier used to check task status
        """

        document_id = ObjectId()
        task_id = ProcessStore().submit(
            payload={
                **payload,
                "command": command,
                "document_id": document_id,
                "user_id": self.user_id,
            },
        )
        return document_id, task_id

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

    def check_status(self, task_status_id: ObjectId) -> Optional[TaskStatus]:
        """
        Check task status given ID

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

    def list_status(self) -> TaskStatusList:
        """
        List task statuses of this user

        Returns
        -------
        TaskStatusList
        """
        task_status_list = []
        for task_status_id, process in ProcessStore().list(user_id=self.user_id):
            task_status = self._get_task_status(task_status_id, process)
            if task_status:
                task_status_list.append(task_status)
        return TaskStatusList(data=task_status_list)
