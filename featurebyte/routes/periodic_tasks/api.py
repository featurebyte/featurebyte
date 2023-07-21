"""
Periodic Task API routes
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.periodic_task import PeriodicTask
from featurebyte.routes.common.schema import (
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
)
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.periodic_task import PeriodicTaskList

router = APIRouter(prefix="/periodic_task")


@router.get("/{periodic_task_id}", response_model=PeriodicTask)
async def get_periodic_task(request: Request, periodic_task_id: PydanticObjectId) -> PeriodicTask:
    """
    Get Periodic Task
    """
    controller = request.state.app_container.periodic_task_controller
    entity: PeriodicTask = await controller.get(document_id=periodic_task_id)
    return entity


@router.get("", response_model=PeriodicTaskList)
async def list_periodic_tasks(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> PeriodicTaskList:
    """
    List Periodic Tasks
    """
    controller = request.state.app_container.periodic_task_controller
    periodic_task_list: PeriodicTaskList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return periodic_task_list


@router.patch("/{periodic_task_id}/description", response_model=PeriodicTask)
async def update_periodic_task_description(
    request: Request,
    periodic_task_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> PeriodicTask:
    """
    Update periodic_task description
    """
    controller = request.state.app_container.periodic_task_controller
    periodic_task: PeriodicTask = await controller.update_description(
        document_id=periodic_task_id,
        description=data.description,
    )
    return periodic_task
