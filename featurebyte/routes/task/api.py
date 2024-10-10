"""
Job status route
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Optional

from fastapi import APIRouter, Request

from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseRouter
from featurebyte.routes.common.schema import PageQuery, PageSizeQuery, SortDirQuery
from featurebyte.schema.task import Task, TaskList, TaskUpdate


class TaskRouter(BaseRouter):
    """
    Task router
    """

    def __init__(self) -> None:
        super().__init__(router=APIRouter(prefix="/task"))
        self.router.add_api_route(
            "/{task_id}",
            self.get_task,
            methods=["GET"],
            response_model=Task,
        )
        self.router.add_api_route(
            "/{task_id}",
            self.update_task,
            methods=["PATCH"],
            response_model=Task,
        )
        self.router.add_api_route(
            "",
            self.list_tasks,
            methods=["GET"],
            response_model=TaskList,
        )
        self.router.add_api_route(
            "/{task_id}",
            self.resubmit_task,
            methods=["POST"],
            response_model=Task,
            status_code=HTTPStatus.CREATED,
        )

    @staticmethod
    async def get_task(request: Request, task_id: str) -> Task:
        """
        Retrieve TaskStatus
        """
        controller = request.state.app_container.task_controller
        task: Task = await controller.get_task(task_id=task_id)
        return task

    @staticmethod
    async def update_task(request: Request, task_id: str, update: TaskUpdate) -> Task:
        """
        Update task
        """
        controller = request.state.app_container.task_controller
        task: Task = await controller.update_task(task_id=task_id, update=update)
        return task

    @staticmethod
    async def list_tasks(
        request: Request,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
    ) -> TaskList:
        """
        List TaskStatus
        """
        controller = request.state.app_container.task_controller
        task_list: TaskList = await controller.list_tasks(
            page=page,
            page_size=page_size,
            sort_dir=sort_dir,
        )
        return task_list

    @staticmethod
    async def resubmit_task(request: Request, task_id: str) -> Task:
        """
        Resubmit task
        """
        controller = request.state.app_container.task_controller
        task: Task = await controller.resubmit_task(task_id=task_id)
        return task
