"""
Job status route
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Request

from featurebyte.routes.common.schema import PageQuery, PageSizeQuery, SortDirQuery
from featurebyte.schema.task_status import TaskStatus, TaskStatusList

router = APIRouter(prefix="/task_status")


@router.get("/{task_status_id}", response_model=TaskStatus)
async def get_task_status(request: Request, task_status_id: str) -> TaskStatus:
    """
    Retrieve TaskStatus
    """
    task_status: TaskStatus = await request.state.controller.get_task_status(
        user=request.state.user, task_status_id=task_status_id
    )
    return task_status


@router.get("", response_model=TaskStatusList)
async def list_task_status(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_dir: Optional[str] = SortDirQuery,
) -> TaskStatusList:
    """
    List TaskStatus"""
    task_status_list: TaskStatusList = await request.state.controller.list_task_status(
        user=request.state.user,
        page=page,
        page_size=page_size,
        sort_dir=sort_dir,
    )
    return task_status_list
