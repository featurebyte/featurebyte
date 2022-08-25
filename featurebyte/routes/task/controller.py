"""
JobStatus API route controller
"""
from __future__ import annotations

from typing import Literal

from http import HTTPStatus

from fastapi import HTTPException

from featurebyte.schema.task import Task, TaskId, TaskList
from featurebyte.service.task_manager import AbstractTaskManager


class TaskController:
    """
    TaskController
    """

    @classmethod
    async def get_task(cls, task_manager: AbstractTaskManager, task_id: str) -> Task:
        """
        Check task status

        Parameters
        ----------
        task_manager: AbstractTaskManager
            Task manager
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
        task_status = await task_manager.get_task(task_id=task_id)
        if task_status is None:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=f'Task (id: "{task_id}") not found.',
            )
        return task_status

    @classmethod
    async def list_tasks(
        cls,
        task_manager: AbstractTaskManager,
        page: int = 1,
        page_size: int = 10,
        sort_dir: Literal["asc", "desc"] = "desc",
    ) -> TaskList:
        """
        List task statuses of the given user

        Parameters
        ----------
        task_manager: AbstractTaskManager
            Task manager
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_dir: "asc" or "desc"
            Sorting the returning documents in ascending order or descending order

        Returns
        -------
        TaskList
        """
        task_statuses, total = await task_manager.list_tasks(
            page=page, page_size=page_size, ascending=(sort_dir == "asc")
        )
        return TaskList(page=page, page_size=page_size, total=total, data=task_statuses)
