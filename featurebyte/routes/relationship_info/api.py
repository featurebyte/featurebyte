"""
RelationshipInfo API routes
"""
from __future__ import annotations

from typing import Optional, cast

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.relationship import RelationshipInfoModel
from featurebyte.routes.base_router import BaseRouter
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
)
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.relationship_info import (
    RelationshipInfoInfo,
    RelationshipInfoList,
    RelationshipInfoUpdate,
)

router = APIRouter(prefix="/relationship_info")


class RelationshipInfoRouter(BaseRouter):
    """
    Relationship info router
    """

    def __init__(self) -> None:
        super().__init__(router=router)


@router.get("", response_model=RelationshipInfoList)
async def list_relationship_info(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> RelationshipInfoList:
    """
    List RelationshipInfo's
    """
    controller = request.state.app_container.relationship_info_controller
    relationship_info_list: RelationshipInfoList = await controller.list_relationship_info(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return relationship_info_list


@router.get("/{relationship_info_id}", response_model=RelationshipInfoModel)
async def get_relationship_info(
    request: Request, relationship_info_id: PydanticObjectId
) -> RelationshipInfoModel:
    """
    Retrieve relationship info
    """
    controller = request.state.app_container.relationship_info_controller
    relationship_info: RelationshipInfoModel = await controller.get(
        document_id=relationship_info_id,
    )
    return relationship_info


@router.patch("/{relationship_info_id}")
async def update_relationship_info(
    request: Request,
    relationship_info_id: PydanticObjectId,
    data: RelationshipInfoUpdate,
) -> RelationshipInfoModel:
    """
    Update RelationshipInfo
    """
    controller = request.state.app_container.relationship_info_controller
    relationship_info = await controller.relationship_info_service.update_document(
        relationship_info_id, data
    )
    return cast(RelationshipInfoModel, relationship_info)


@router.get("/{relationship_info_id}/info", response_model=RelationshipInfoInfo)
async def get_relationship_info_info(
    request: Request,
    relationship_info_id: PydanticObjectId,
) -> RelationshipInfoInfo:
    """
    Retrieve RelationshipInfo info
    """
    controller = request.state.app_container.relationship_info_controller
    info = await controller.get_info(
        document_id=relationship_info_id,
    )
    return cast(RelationshipInfoInfo, info)


@router.get("/audit/{relationship_info_id}", response_model=AuditDocumentList)
async def list_relationship_info_audit_logs(
    request: Request,
    relationship_info_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List relationship_info audit logs
    """
    controller = request.state.app_container.relationship_info_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=relationship_info_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.patch("/{relationship_info_id}/description", response_model=RelationshipInfoModel)
async def update_relationship_info_description(
    request: Request,
    relationship_info_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> RelationshipInfoModel:
    """
    Update relationship_info description
    """
    controller = request.state.app_container.relationship_info_controller
    relationship_info: RelationshipInfoModel = await controller.update_description(
        document_id=relationship_info_id,
        description=data.description,
    )
    return relationship_info
