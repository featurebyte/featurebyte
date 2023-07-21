"""
UserDefinedFunction API routes
"""
from typing import Optional, cast

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
    VerboseQuery,
)
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.info import UserDefinedFunctionInfo
from featurebyte.schema.user_defined_function import (
    UserDefinedFunctionCreate,
    UserDefinedFunctionList,
    UserDefinedFunctionUpdate,
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
        data=data, get_credential=request.state.get_credential
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


@router.patch("/{user_defined_function_id}", response_model=UserDefinedFunctionModel)
async def update_user_defined_function(
    request: Request, user_defined_function_id: PydanticObjectId, data: UserDefinedFunctionUpdate
) -> UserDefinedFunctionModel:
    """
    Update UserDefinedFunction
    """
    controller = request.state.app_container.user_defined_function_controller
    user_defined_function: UserDefinedFunctionModel = await controller.update_user_defined_function(
        document_id=user_defined_function_id, data=data, get_credential=request.state.get_credential
    )
    return user_defined_function


@router.delete("/{user_defined_function_id}", response_model=DeleteResponse)
async def delete_user_defined_function(
    request: Request, user_defined_function_id: PydanticObjectId
) -> DeleteResponse:
    """
    Delete UserDefinedFunction
    """
    controller = request.state.app_container.user_defined_function_controller
    await controller.delete_user_defined_function(document_id=user_defined_function_id)
    return DeleteResponse()


@router.get("", response_model=UserDefinedFunctionList)
async def list_user_defined_functions(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
    feature_store_id: Optional[PydanticObjectId] = None,
) -> UserDefinedFunctionList:
    """
    List UserDefinedFunctions
    """
    controller = request.state.app_container.user_defined_function_controller
    user_defined_functions: UserDefinedFunctionList = await controller.list_user_defined_functions(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
        feature_store_id=feature_store_id,
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


@router.get("/{user_defined_function_id}/info", response_model=UserDefinedFunctionInfo)
async def get_user_defined_function_info(
    request: Request, user_defined_function_id: PydanticObjectId, verbose: bool = VerboseQuery
) -> UserDefinedFunctionInfo:
    """
    Get UserDefinedFunction info
    """
    controller = request.state.app_container.user_defined_function_controller
    info = await controller.get_info(document_id=user_defined_function_id, verbose=verbose)
    return cast(UserDefinedFunctionInfo, info)


@router.patch("/{user_defined_function_id}/description", response_model=UserDefinedFunctionModel)
async def update_user_defined_function_description(
    request: Request,
    user_defined_function_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> UserDefinedFunctionModel:
    """
    Update user_defined_function description
    """
    controller = request.state.app_container.user_defined_function_controller
    user_defined_function: UserDefinedFunctionModel = await controller.update_description(
        document_id=user_defined_function_id,
        description=data.description,
    )
    return user_defined_function
