"""
Job status route
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Request

from featurebyte.routes.common.schema import PageQuery, PageSizeQuery, SortDirQuery
from featurebyte.schema.task import Task, TaskList

router = APIRouter(prefix="/task")


@router.get("/{task_id}", response_model=Task)
async def get_task(request: Request, task_id: str) -> Task:
    """
    Retrieve TaskStatus
    """
    controller = request.state.app_container.task_controller
    task: Task = await controller.get_task(task_id=task_id)
    return task


@router.get("", response_model=TaskList)
async def list_tasks(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_dir: Optional[str] = SortDirQuery,
) -> TaskList:
    """
    List TaskStatus"""
    controller = request.state.app_container.task_controller
    task_list: TaskList = await controller.list_tasks(
        page=page,
        page_size=page_size,
        sort_dir=sort_dir,
    )
    return task_list
