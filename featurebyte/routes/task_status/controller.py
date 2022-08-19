"""
JobStatus API route controller
"""
from __future__ import annotations

from typing import Any, Literal

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.schema.task_status import TaskStatus, TaskStatusList
from featurebyte.service.task_manager import AbstractTaskManager, TaskManager


class TaskStatusController:
    """
    TaskStatusController
    """

    task_manager_class: type[AbstractTaskManager] = TaskManager

    @classmethod
    async def get_task_status(cls, user: Any, task_status_id: ObjectId) -> TaskStatus:
        """
        Check task status

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        task_status_id: ObjectId
            Task status ID

        Returns
        -------
        TaskStatus

        Raises
        ------
        HTTPException
            When the task status not found
        """
        task_manager = cls.task_manager_class(user_id=user.id)
        task_status = task_manager.get_task_status(task_status_id=ObjectId(task_status_id))
        if task_status is None:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=f'TaskStatus (id: "{task_status_id}") not found.',
            )
        return task_status

    @classmethod
    async def list_task_status(
        cls,
        user: Any,
        page: int = 1,
        page_size: int = 10,
        sort_dir: Literal["asc", "desc"] = "desc",
    ) -> TaskStatusList:
        """
        List task statuses of the given user

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_dir: "asc" or "desc"
            Sorting the returning documents in ascending order or descending order

        Returns
        -------
        TaskStatusList
        """
        task_manager = cls.task_manager_class(user_id=user.id)
        task_statuses, total = task_manager.list_task_status(
            page=page, page_size=page_size, ascending=(sort_dir == "asc")
        )
        return TaskStatusList(page=page, page_size=page_size, total=total, data=task_statuses)
