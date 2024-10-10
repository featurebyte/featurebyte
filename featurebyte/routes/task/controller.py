"""
JobStatus API route controller
"""

from __future__ import annotations

from http import HTTPStatus

from fastapi import HTTPException

from featurebyte.persistent.base import SortDir
from featurebyte.schema.task import Task, TaskList, TaskUpdate
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE
from featurebyte.service.task_manager import TaskManager


class TaskController:
    """
    TaskController
    """

    def __init__(self, task_manager: TaskManager):
        self.task_manager = task_manager

    async def get_task(self, task_id: str) -> Task:
        """
        Check task status

        Parameters
        ----------
        task_id: str
            Task ID

        Returns
        -------
        Task

        Raises
        ------
        HTTPException
            When the task status not found
        """
        task_status = await self.task_manager.get_task(task_id=task_id)
        if task_status is None:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=f'Task (id: "{task_id}") not found.',
            )
        return task_status

    async def list_tasks(
        self,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_dir: SortDir = "desc",
    ) -> TaskList:
        """
        List task statuses of the given user

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_dir: SortDir
            Sorting the returning documents in ascending order or descending order

        Returns
        -------
        TaskList
        """
        task_statuses, total = await self.task_manager.list_tasks(
            page=page, page_size=page_size, ascending=(sort_dir == "asc")
        )
        return TaskList(page=page, page_size=page_size, total=total, data=task_statuses)

    async def update_task(self, task_id: str, update: TaskUpdate) -> Task:
        """
        Update task

        Parameters
        ----------
        task_id: str
            Task ID
        update: TaskUpdate
            TaskUpdate object

        Returns
        -------
        Task
        """
        if update.revoke:
            await self.task_manager.revoke_task(task_id)
        return await self.get_task(task_id)

    async def resubmit_task(self, task_id: str) -> Task:
        """
        Resubmit task for execution

        Parameters
        ----------
        task_id: str
            Task ID

        Returns
        -------
        Task
        """
        new_task_id = await self.task_manager.rerun_task(task_id)
        return await self.get_task(new_task_id)
