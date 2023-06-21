"""
UserDefinedFunction API routes
"""
from typing import Optional

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.user_defined_function import UserDefinedFunctionModel
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
)
from featurebyte.schema.user_defined_function import (
    UserDefinedFunctionCreate,
    UserDefinedFunctionList,
)

router = APIRouter(prefix="/user_defined_function")


@router.post("", response_model=UserDefinedFunctionModel, status_code=HTTPStatus.CREATED)
async def create_user_defined_function(
    request: Request, data: UserDefinedFunctionCreate
) -> UserDefinedFunctionModel:
    """
    Create UserDefinedFunction
    """
    controller = request.state.app_container.user_defined_function_controller
    user_defined_function: UserDefinedFunctionModel = await controller.create_user_defined_function(
        data=data
    )
    return user_defined_function


@router.get("/{user_defined_function_id}", response_model=UserDefinedFunctionModel)
async def get_user_defined_function(
    request: Request, user_defined_function_id: PydanticObjectId
) -> UserDefinedFunctionModel:
    """
    Get UserDefinedFunction
    """
    controller = request.state.app_container.user_defined_function_controller
    user_defined_function: UserDefinedFunctionModel = await controller.get(
        document_id=user_defined_function_id
    )
    return user_defined_function


@router.get("", response_model=UserDefinedFunctionList)
async def list_user_defined_functions(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> UserDefinedFunctionList:
    """
    List UserDefinedFunctions
    """
    controller = request.state.app_container.user_defined_function_controller
    user_defined_functions: UserDefinedFunctionList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return user_defined_functions


@router.get("/audit/{user_defined_function_id}", response_model=AuditDocumentList)
async def get_user_defined_function_audit_log(
    request: Request,
    user_defined_function_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List UserDefinedFunction audit log
    """
    controller = request.state.app_container.user_defined_function_controller
    audit_doc_log: AuditDocumentList = await controller.list_audit(
        document_id=user_defined_function_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_log
