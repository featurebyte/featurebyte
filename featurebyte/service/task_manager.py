"""
TaskManager service is responsible to submit task message
"""
from __future__ import annotations

from typing import Optional, Union, cast

from abc import abstractmethod
from uuid import UUID

from bson.objectid import ObjectId

from featurebyte.models.task import Task as TaskModel
from featurebyte.schema.task import Task
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.utils.persistent import get_persistent
from featurebyte.worker import celery

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
        """
        Submit task to celery

        Parameters
        ----------
        payload: BaseTaskPayload
            Payload to submit

        Returns
        -------
        TaskId
            Task ID
        """
        assert self.user_id == payload.user_id
        kwargs = payload.json_dict()
        kwargs["task_output_path"] = payload.task_output_path
        task = celery.send_task("featurebyte.worker.task_executor.execute_task", kwargs=kwargs)
        return cast(TaskId, task.id)

    async def get_task(self, task_id: str) -> Task | None:
        """
        Get task information

        Parameters
        ----------
        task_id: str
            Task ID

        Returns
        -------
        Task
            Task object
        """
        task_id = str(task_id)
        task_result = celery.AsyncResult(task_id)
        payload = {}
        output_path = None
        traceback = None

        # try to find in persistent first
        persistent = get_persistent()
        document = await persistent.find_one(
            collection_name=TaskModel.collection_name(),
            query_filter={"_id": task_id},
        )

        if document:
            output_path = document.get("kwargs", {}).get("task_output_path")
            payload = document.get("kwargs", {})
            traceback = document.get("traceback")
        elif not task_result:
            return None

        return Task(
            id=UUID(task_id),
            status=task_result.status,
            output_path=output_path,
            payload=payload,
            traceback=traceback,
        )

    async def list_tasks(
        self,
        page: int = 1,
        page_size: int = 10,
        ascending: bool = True,
    ) -> tuple[list[Task], int]:
        # Perform the query
        persistent = get_persistent()
        results, total = await persistent.find(
            collection_name=TaskModel.collection_name(),
            query_filter={},
            page=page,
            page_size=page_size,
            sort_by="date_done",
            sort_dir="asc" if ascending else "desc",
        )

        tasks = [
            Task(
                **document,
                id=document["_id"],
                payload=document.get("kwargs", {}),
                output_path=document.get("kwargs", {}).get("task_output_path"),
            )
            for document in results
        ]
        return tasks, total
