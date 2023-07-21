"""
Catalog API routes
"""
from __future__ import annotations

from typing import List, Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.catalog import CatalogModel, CatalogNameHistoryEntry
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
    VerboseQuery,
)
from featurebyte.schema.catalog import CatalogCreate, CatalogList, CatalogUpdate
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.info import CatalogInfo

router = APIRouter(prefix="/catalog")


@router.post("", response_model=CatalogModel, status_code=HTTPStatus.CREATED)
async def create_catalog(request: Request, data: CatalogCreate) -> CatalogModel:
    """
    Create Catalog
    """
    controller = request.state.app_container.catalog_controller
    catalog: CatalogModel = await controller.create_catalog(data=data)
    return catalog


@router.get("/{catalog_id}", response_model=CatalogModel)
async def get_catalog(request: Request, catalog_id: PydanticObjectId) -> CatalogModel:
    """
    Get catalog
    """
    controller = request.state.app_container.catalog_controller
    catalog: CatalogModel = await controller.get(document_id=catalog_id)
    return catalog


@router.get("", response_model=CatalogList)
async def list_catalogs(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> CatalogList:
    """
    List catalog
    """
    controller = request.state.app_container.catalog_controller
    catalog_list: CatalogList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return catalog_list


@router.patch("/{catalog_id}", response_model=CatalogModel)
async def update_catalog(
    request: Request,
    catalog_id: PydanticObjectId,
    data: CatalogUpdate,
) -> CatalogModel:
    """
    Update catalog
    """
    controller = request.state.app_container.catalog_controller
    catalog: CatalogModel = await controller.update_catalog(
        catalog_id=catalog_id,
        data=data,
    )
    return catalog


@router.get("/audit/{catalog_id}", response_model=AuditDocumentList)
async def list_catalog_audit_logs(
    request: Request,
    catalog_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List catalog audit logs
    """
    controller = request.state.app_container.catalog_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=catalog_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get(
    "/history/name/{catalog_id}",
    response_model=List[CatalogNameHistoryEntry],
)
async def list_name_history(
    request: Request,
    catalog_id: PydanticObjectId,
) -> List[CatalogNameHistoryEntry]:
    """
    List catalog name history
    """
    controller = request.state.app_container.catalog_controller
    history_values = await controller.list_field_history(
        document_id=catalog_id,
        field="name",
    )

    return [
        CatalogNameHistoryEntry(
            created_at=record.created_at,
            name=record.value,
        )
        for record in history_values
    ]


@router.get("/{catalog_id}/info", response_model=CatalogInfo)
async def get_catalog_info(
    request: Request,
    catalog_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> CatalogInfo:
    """
    Retrieve catalog info
    """
    controller = request.state.app_container.catalog_controller
    info = await controller.get_info(
        document_id=catalog_id,
        verbose=verbose,
    )
    return cast(CatalogInfo, info)


@router.patch("/{catalog_id}/description", response_model=CatalogModel)
async def update_catalog_description(
    request: Request,
    catalog_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> CatalogModel:
    """
    Update catalog description
    """
    controller = request.state.app_container.catalog_controller
    catalog: CatalogModel = await controller.update_description(
        document_id=catalog_id,
        description=data.description,
    )
    return catalog
