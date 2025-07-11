"""
Table API routes
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.proxy_table import TableModel
from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseRouter
from featurebyte.routes.common.schema import (
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
)
from featurebyte.schema.table import TableList

router = APIRouter(prefix="/table")


class TableRouter(BaseRouter):
    """
    Table router
    """

    def __init__(self) -> None:
        super().__init__(router=router)


@router.get("", response_model=TableList)
async def list_table(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[SortDir] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> TableList:
    """
    List Table
    """
    controller = request.state.app_container.table_controller
    table_list: TableList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
        name=name,
    )
    return table_list


@router.get("/{table_id}", response_model=TableModel)
async def get_table(request: Request, table_id: PydanticObjectId) -> TableModel:
    """
    Retrieve Table
    """
    controller = request.state.app_container.table_controller
    table: TableModel = await controller.get(
        document_id=table_id,
    )
    return table
