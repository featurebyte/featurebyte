"""
Context API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Optional

from bson import ObjectId
from fastapi import Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.context import ContextModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseApiRouter
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortDirQuery,
)
from featurebyte.routes.context.controller import ContextController
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.context import ContextCreate, ContextList, ContextUpdate
from featurebyte.schema.info import ContextInfo
from featurebyte.schema.observation_table import ObservationTableList


class ContextRouter(BaseApiRouter[ContextModel, ContextList, ContextCreate, ContextController]):
    """
    Context API router
    """

    object_model = ContextModel
    list_object_model = ContextList
    create_object_schema = ContextCreate
    controller = ContextController

    def __init__(self) -> None:
        super().__init__("/context")

        # update route
        self.router.add_api_route(
            "/{context_id}",
            self.update_context,
            methods=["PATCH"],
            response_model=ContextModel,
            status_code=HTTPStatus.OK,
        )

        # list observation tables
        self.router.add_api_route(
            "/{context_id}/observation_tables",
            self.list_context_observation_tables,
            methods=["GET"],
            response_model=ObservationTableList,
        )

        # context info
        self.router.add_api_route(
            "/{context_id}/info",
            self.context_info,
            methods=["GET"],
            response_model=ContextInfo,
        )

    async def get_object(self, request: Request, context_id: PydanticObjectId) -> ContextModel:
        return await super().get_object(request, context_id)

    async def list_audit_logs(
        self,
        request: Request,
        context_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        return await super().list_audit_logs(
            request, context_id, page, page_size, sort_by, sort_dir, search
        )

    async def update_description(
        self, request: Request, context_id: PydanticObjectId, data: DescriptionUpdate
    ) -> ContextModel:
        return await super().update_description(request, context_id, data)

    async def create_object(
        self,
        request: Request,
        data: ContextCreate,
    ) -> ContextModel:
        """
        Create Context
        """
        controller = self.get_controller_for_request(request)
        result: ContextModel = await controller.create_context(data=data)
        return result

    async def delete_object(self, request: Request, context_id: PydanticObjectId) -> DeleteResponse:
        """
        Delete Context
        """
        controller = self.get_controller_for_request(request)
        await controller.delete(document_id=ObjectId(context_id))
        return DeleteResponse()

    async def update_context(
        self, request: Request, context_id: PydanticObjectId, data: ContextUpdate
    ) -> ContextModel:
        """
        Update Context
        """
        controller = self.get_controller_for_request(request)
        context: ContextModel = await controller.update_context(
            context_id=ObjectId(context_id), data=data
        )
        return context

    @staticmethod
    async def list_context_observation_tables(
        request: Request,
        context_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
    ) -> ObservationTableList:
        """
        List Observation Tables associated with the Use Case
        """
        observation_table_controller = request.state.app_container.observation_table_controller
        observation_table_list: ObservationTableList = await observation_table_controller.list(
            query_filter={"context_id": context_id},
            page=page,
            page_size=page_size,
        )
        return observation_table_list

    async def context_info(self, request: Request, context_id: PydanticObjectId) -> ContextInfo:
        """
        Get Context Info
        """
        controller = self.get_controller_for_request(request)
        return await controller.get_info(context_id=ObjectId(context_id))
