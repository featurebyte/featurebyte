"""
UseCase API routes
"""
from typing import Optional

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.use_case import UseCaseModel
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
from featurebyte.schema.use_case import UseCaseCreate, UseCaseList, UseCaseUpdate

router = APIRouter(prefix="/use_case")


@router.post("", response_model=UseCaseModel, status_code=HTTPStatus.CREATED)
async def create_use_case(
    request: Request,
    data: UseCaseCreate,
) -> UseCaseModel:
    """
    Create a UseCase
    """
    controller = request.state.app_container.use_case_controller
    use_case: UseCaseModel = await controller.create_use_case(
        data=data,
    )
    return use_case


@router.get("/{use_case_id}", response_model=UseCaseModel)
async def get_use_case(request: Request, use_case_id: PydanticObjectId) -> UseCaseModel:
    """
    Get Use Case
    """
    controller = request.state.app_container.use_case_controller
    use_case: UseCaseModel = await controller.get(document_id=use_case_id)
    return use_case


@router.patch("/{use_case_id}", response_model=UseCaseModel)
async def update_use_case(
    request: Request, use_case_id: PydanticObjectId, data: UseCaseUpdate
) -> UseCaseModel:
    """
    Update Use Case
    """
    controller = request.state.app_container.use_case_controller
    use_case: UseCaseModel = await controller.update_use_case(use_case_id=use_case_id, data=data)
    return use_case


@router.get("", response_model=UseCaseList)
async def list_use_cases(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> UseCaseList:
    """
    List Use Case
    """
    controller = request.state.app_container.use_case_controller
    doc_list: UseCaseList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return doc_list


@router.get("/audit/{use_case_id}", response_model=AuditDocumentList)
async def list_use_case_audit_logs(
    request: Request,
    use_case_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Use Case audit logs
    """
    controller = request.state.app_container.use_case_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=use_case_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.patch("/{use_case_id}/description", response_model=UseCaseModel)
async def update_use_case_description(
    request: Request,
    use_case_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> UseCaseModel:
    """
    Update Use Case description
    """
    controller = request.state.app_container.use_case_controller
    doc: UseCaseModel = await controller.update_description(
        document_id=use_case_id,
        description=data.description,
    )
    return doc
