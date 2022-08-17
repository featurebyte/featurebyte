"""
JobStatus API route controller
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.schema.task_status import TaskStatus, TaskStatusList
from featurebyte.service.task_manager import TaskManager


class TaskStatusController:
    """
    JobStatus controller
    """

    task_manager_class: type[TaskManager] = TaskManager

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
        task_status = task_manager.check_status(task_status_id=task_status_id)
        if task_status is None:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=f'TaskStatus (id: "{task_status_id}") not found.',
            )
        return task_status

    @classmethod
    async def list_task_status(cls, user: Any) -> TaskStatusList:
        """
        List task statuses of the given user

        Parameters
        ----------
        user: Any
            User class to provide user identifier

        Returns
        -------
        TaskStatusList
        """
        task_manager = cls.task_manager_class(user_id=user.id)
        return task_manager.list_status()
