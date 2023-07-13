"""
TaskManager service is responsible to submit task message
"""
from __future__ import annotations

from typing import Any, Optional, Union, cast

import datetime
from uuid import UUID

from bson.objectid import ObjectId
from celery import Celery

from featurebyte.logging import get_logger
from featurebyte.models.periodic_task import Crontab, Interval, PeriodicTask
from featurebyte.models.task import Task as TaskModel
from featurebyte.persistent import Persistent
from featurebyte.schema.task import Task
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE
from featurebyte.service.periodic_task import PeriodicTaskService

TaskId = Union[ObjectId, UUID]


logger = get_logger(__name__)


class TaskManager:
    """
    TaskManager class is responsible for submitting task request & task status retrieval
    """

    def __init__(
        self, user: Any, persistent: Persistent, celery: Celery, catalog_id: Optional[ObjectId]
    ) -> None:
        self.user = user
        self.persistent = persistent
        self.celery = celery
        self.catalog_id = catalog_id

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
        task = self.celery.send_task(payload.task, kwargs=kwargs)
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
        task_result = self.celery.AsyncResult(task_id)
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
        page_size: int = DEFAULT_PAGE_SIZE,
        ascending: bool = True,
    ) -> tuple[list[Task], int]:
        """
        List tasks.

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Page size
        ascending: bool
            Sort direction

        Returns
        -------
        tuple[list[Task], int]
        """
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
        time_limit: Optional[int] = None,
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
        time_limit: Optional[int]
            Execution time limit in seconds

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

        # if time limit is not set default to interval length
        if not time_limit:
            time_limit = int(
                datetime.timedelta(**{str(interval.period): interval.every}).total_seconds()
            )

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
            soft_time_limit=time_limit,
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
        time_limit: Optional[int] = None,
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
        time_limit: Optional[int]
            Execution time limit in seconds

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
            soft_time_limit=time_limit,
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

        result = await periodic_task_service.list_documents_as_dict(
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
        result = await periodic_task_service.list_documents_as_dict(
            page=1,
            page_size=0,
            query_filter={"name": name},
        )

        data = result["data"]
        if not data:
            logger.error(f"Document with name {name} not found")
        else:
            await periodic_task_service.delete_document(document_id=data[0]["_id"])
