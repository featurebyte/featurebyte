"""
Credential API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Optional

from bson import ObjectId
from fastapi import Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseApiRouter
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
from featurebyte.routes.credential.controller import CredentialController
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.credential import (
    CredentialCreate,
    CredentialList,
    CredentialRead,
    CredentialUpdate,
)
from featurebyte.schema.info import CredentialInfo


class CredentialRouter(
    BaseApiRouter[CredentialRead, CredentialList, CredentialCreate, CredentialController]
):
    """
    Credential API router
    """

    object_model = CredentialRead
    list_object_model = CredentialList
    create_object_schema = CredentialCreate
    controller = CredentialController

    def __init__(self) -> None:
        super().__init__("/credential")

        # update route
        self.router.add_api_route(
            "/{credential_id}",
            self.update_credential,
            methods=["PATCH"],
            response_model=CredentialRead,
            status_code=HTTPStatus.OK,
        )

        # info route
        self.router.add_api_route(
            "/{credential_id}/info",
            self.get_credential_info,
            methods=["GET"],
            response_model=CredentialInfo,
        )

    async def get_object(self, request: Request, credential_id: PydanticObjectId) -> CredentialRead:
        credential = await super().get_object(request, credential_id)
        return CredentialRead(**credential.model_dump(by_alias=True))

    async def delete_object(
        self, request: Request, credential_id: PydanticObjectId
    ) -> DeleteResponse:
        controller = self.get_controller_for_request(request)
        await controller.delete_credential(credential_id)
        return DeleteResponse()

    async def list_audit_logs(
        self,
        request: Request,
        credential_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        return await super().list_audit_logs(
            request,
            credential_id,
            page,
            page_size,
            sort_by,
            sort_dir,
            search,
        )

    async def update_description(
        self, request: Request, credential_id: PydanticObjectId, data: DescriptionUpdate
    ) -> CredentialRead:
        credentials = await super().update_description(request, credential_id, data)
        return CredentialRead(**credentials.model_dump(by_alias=True))

    async def list_objects(
        self,
        request: Request,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = SortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
        name: Optional[str] = NameQuery,
        feature_store_id: Optional[PydanticObjectId] = None,
    ) -> CredentialList:
        """
        List credentials
        """
        controller = self.get_controller_for_request(request)
        return await controller.list(
            page=page,
            page_size=page_size,
            sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
            search=search,
            name=name,
            query_filter={} if feature_store_id is None else {"feature_store_id": feature_store_id},
        )

    async def create_object(
        self,
        request: Request,
        data: CredentialCreate,
    ) -> CredentialRead:
        """
        Create credential
        """
        controller = self.get_controller_for_request(request)
        credentials = await controller.create_credential(request, data)
        return credentials

    async def update_credential(
        self,
        request: Request,
        credential_id: PydanticObjectId,
        data: CredentialUpdate,
    ) -> CredentialRead:
        """
        Update credential
        """
        controller = self.get_controller_for_request(request)
        credential = await controller.update_credential(credential_id=credential_id, data=data)
        return CredentialRead(**credential.model_dump(by_alias=True))

    async def get_credential_info(
        self,
        request: Request,
        credential_id: PydanticObjectId,
        verbose: bool = VerboseQuery,
    ) -> CredentialInfo:
        """
        Retrieve catalog info
        """
        controller = self.get_controller_for_request(request)
        return await controller.get_info(
            credential_id=ObjectId(credential_id),
            verbose=verbose,
        )
