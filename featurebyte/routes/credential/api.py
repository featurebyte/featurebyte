"""
Credential API routes
"""
from __future__ import annotations

from typing import Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
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
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.credential import (
    CredentialCreate,
    CredentialList,
    CredentialRead,
    CredentialUpdate,
)
from featurebyte.schema.info import CredentialInfo

router = APIRouter(prefix="/credential")


@router.post("", response_model=CredentialRead, status_code=HTTPStatus.CREATED)
async def create_credential(
    request: Request,
    data: CredentialCreate,
) -> CredentialRead:
    """
    Create credential
    """
    controller = request.state.app_container.credential_controller
    return cast(CredentialRead, await controller.create_credential(data=data))


@router.get(
    "/{credential_id}",
    response_model=CredentialRead,
    status_code=HTTPStatus.OK,
)
async def retrieve_credential(
    request: Request,
    credential_id: PydanticObjectId,
) -> CredentialRead:
    """
    Retrieve credential
    """
    controller = request.state.app_container.credential_controller
    return cast(
        CredentialRead,
        await controller.get(
            document_id=credential_id,
        ),
    )


@router.get("", response_model=CredentialList, status_code=HTTPStatus.OK)
async def list_credential(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
    feature_store_id: Optional[PydanticObjectId] = None,
) -> CredentialList:
    """
    List credentials
    """
    controller = request.state.app_container.credential_controller
    return cast(
        CredentialList,
        await controller.list(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            search=search,
            name=name,
            query_filter={} if feature_store_id is None else {"feature_store_id": feature_store_id},
        ),
    )


@router.patch(
    "/{credential_id}",
    response_model=CredentialRead,
    status_code=HTTPStatus.OK,
)
async def update_credential(
    request: Request,
    credential_id: PydanticObjectId,
    data: CredentialUpdate,
) -> CredentialRead:
    """
    Update credential
    """
    controller = request.state.app_container.credential_controller
    return cast(
        CredentialRead,
        await controller.update_credential(
            credential_id=credential_id,
            data=data,
        ),
    )


@router.get("/{credential_id}/info", response_model=CredentialInfo)
async def get_credential_info(
    request: Request,
    credential_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> CredentialInfo:
    """
    Retrieve catalog info
    """
    controller = request.state.app_container.credential_controller
    info = await controller.get_info(
        credential_id=credential_id,
        verbose=verbose,
    )
    return cast(CredentialInfo, info)


@router.get("/audit/{credential_id}", response_model=AuditDocumentList)
async def list_credential_audit_logs(
    request: Request,
    credential_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List credential audit logs
    """
    controller = request.state.app_container.credential_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=credential_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.delete("/{credential_id}", status_code=HTTPStatus.OK)
async def delete_credential(
    request: Request,
    credential_id: PydanticObjectId,
) -> DeleteResponse:
    """
    Delete credential
    """
    controller = request.state.app_container.credential_controller
    await controller.delete_credential(
        credential_id=credential_id,
    )
    return DeleteResponse()


@router.patch("/{credential_id}/description", response_model=CredentialRead)
async def update_credential_description(
    request: Request,
    credential_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> CredentialRead:
    """
    Update credential description
    """
    controller = request.state.app_container.credential_controller
    credential: CredentialRead = await controller.update_description(
        document_id=credential_id,
        description=data.description,
    )
    return credential
