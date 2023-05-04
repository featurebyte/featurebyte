"""
TaskManager service is responsible to submit task message
"""
from __future__ import annotations

from typing import Any, Optional, Union, cast

import datetime
from abc import abstractmethod
from uuid import UUID

from bson.objectid import ObjectId

from featurebyte.logging import get_logger
from featurebyte.models.periodic_task import Crontab, Interval, PeriodicTask
from featurebyte.models.task import Task as TaskModel
from featurebyte.persistent import Persistent
from featurebyte.schema.task import Task
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.service.periodic_task import PeriodicTaskService
from featurebyte.worker import celery

TaskId = Union[ObjectId, UUID]


logger = get_logger(__name__)


class AbstractTaskManager:
    """
    AbstractTaskManager defines interface for TaskManager
    """

    def __init__(self, user: Any, persistent: Persistent, catalog_id: ObjectId) -> None:
        """
        TaskManager constructor

        Parameters
        ----------
        user: Any
            User
        persistent: Persistent
            Persistent
        catalog_id: ObjectId
            Catalog ID
        """
        self.user = user
        self.persistent = persistent
        self.catalog_id = catalog_id

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
        assert self.user.id == payload.user_id
        kwargs = payload.json_dict()
        kwargs["task_output_path"] = payload.task_output_path
        task = celery.send_task(payload.task, kwargs=kwargs)
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
        document = await self.persistent.find_one(
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
        results, total = await self.persistent.find(
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

    async def schedule_interval_task(
        self,
        name: str,
        payload: BaseTaskPayload,
        interval: Interval,
        time_modulo_frequency_second: Optional[int] = None,
        start_after: Optional[datetime.datetime] = None,
    ) -> ObjectId:
        """
        Schedule task to run periodically

        Parameters
        ----------
        name: str
            Task name
        payload: BaseTaskPayload
            Payload to use for scheduled task
        interval: Interval
            Interval specification
        time_modulo_frequency_second: Optional[int]
            Time modulo frequency in seconds
        start_after: Optional[datetime.datetime]
            Start after this time

        Returns
        -------
        ObjectId
            PeriodicTask ID
        """
        assert self.user.id == payload.user_id
        if time_modulo_frequency_second:
            last_run_at = datetime.datetime.utcnow()
        else:
            last_run_at = None

        periodic_task = PeriodicTask(
            name=name,
            task=payload.task,
            interval=interval,
            args=[],
            kwargs=payload.json_dict(),
            time_modulo_frequency_second=time_modulo_frequency_second,
            start_after=start_after,
            last_run_at=last_run_at,
            queue=payload.queue,
        )
        periodic_task_service = PeriodicTaskService(
            user=self.user,
            persistent=self.persistent,
            catalog_id=self.catalog_id,
        )
        await periodic_task_service.create_document(data=periodic_task)
        return periodic_task.id

    async def schedule_cron_task(
        self,
        name: str,
        payload: BaseTaskPayload,
        crontab: Crontab,
        start_after: Optional[datetime.datetime] = None,
    ) -> ObjectId:
        """
        Schedule task to run on cron setting

        Parameters
        ----------
        name: str
            Task name
        payload: BaseTaskPayload
            Payload to use for scheduled task
        crontab: Crontab
            Cron specification
        start_after: Optional[datetime.datetime]
            Start after this time

        Returns
        -------
        ObjectId
            PeriodicTask ID
        """
        assert self.user.id == payload.user_id
        periodic_task = PeriodicTask(
            name=name,
            task=payload.task,
            crontab=crontab,
            args=[],
            kwargs=payload.json_dict(),
            start_after=start_after,
        )
        periodic_task_service = PeriodicTaskService(
            user=self.user,
            persistent=self.persistent,
            catalog_id=self.catalog_id,
        )
        await periodic_task_service.create_document(data=periodic_task)
        return periodic_task.id

    async def get_periodic_task(self, periodic_task_id: ObjectId) -> PeriodicTask:
        """
        Retrieve periodic task

        Parameters
        ----------
        periodic_task_id: ObjectId
            PeriodicTask ID

        Returns
        -------
        PeriodicTask
        """
        periodic_task_service = PeriodicTaskService(
            user=self.user,
            persistent=self.persistent,
            catalog_id=self.catalog_id,
        )
        return await periodic_task_service.get_document(document_id=periodic_task_id)

    async def get_periodic_task_by_name(self, name: str) -> Optional[PeriodicTask]:
        """
        Retrieve periodic task

        Parameters
        ----------
        name: str
            name of the periodic task

        Returns
        -------
        PeriodicTask
        """
        periodic_task_service = PeriodicTaskService(
            user=self.user,
            persistent=self.persistent,
            catalog_id=self.catalog_id,
        )

        result = await periodic_task_service.list_documents(
            page=1,
            page_size=0,
            query_filter={"name": name},
        )

        data = result["data"]
        if data:
            return PeriodicTask(**data[0])

        return None

    async def delete_periodic_task(self, periodic_task_id: ObjectId) -> None:
        """
        Delete periodic task

        Parameters
        ----------
        periodic_task_id: ObjectId
            PeriodicTask ID
        """
        periodic_task_service = PeriodicTaskService(
            user=self.user,
            persistent=self.persistent,
            catalog_id=self.catalog_id,
        )
        await periodic_task_service.delete_document(document_id=periodic_task_id)

    async def delete_periodic_task_by_name(self, name: str) -> None:
        """
        Delete periodic task by name

        Parameters
        ----------
        name: str
            Document Name
        """
        periodic_task_service = PeriodicTaskService(
            user=self.user,
            persistent=self.persistent,
            catalog_id=self.catalog_id,
        )
        result = await periodic_task_service.list_documents(
            page=1,
            page_size=0,
            query_filter={"name": name},
        )

        data = result["data"]
        if not data:
            logger.error(f"Document with name {name} not found")
        else:
            await periodic_task_service.delete_document(document_id=data[0]["_id"])
