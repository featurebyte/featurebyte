"""
TabularData API routes
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Request

from featurebyte.routes.common.schema import (
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
)
from featurebyte.schema.tabular_data import TabularDataList

router = APIRouter(prefix="/tabular_data")


@router.get("", response_model=TabularDataList)
async def list_tabular_data(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> TabularDataList:
    """
    List Item Datas
    """
    controller = request.state.app_container.tabular_data_controller
    tabular_data_list: TabularDataList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return tabular_data_list
