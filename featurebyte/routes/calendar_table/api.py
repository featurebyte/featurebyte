"""
CalendarTable API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.calendar_table import CalendarTableModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseApiRouter
from featurebyte.routes.calendar_table.controller import CalendarTableController
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortDirQuery,
    VerboseQuery,
)
from featurebyte.schema.calendar_table import (
    CalendarTableCreate,
    CalendarTableList,
    CalendarTableUpdate,
)
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.info import CalendarTableInfo
from featurebyte.schema.table import (
    ColumnCriticalDataInfoUpdate,
    ColumnDescriptionUpdate,
    ColumnEntityUpdate,
)

router = APIRouter(prefix="/calendar_table")


class CalendarTableRouter(
    BaseApiRouter[
        CalendarTableModel, CalendarTableList, CalendarTableCreate, CalendarTableController
    ]
):
    """
    Calendar table router
    """

    object_model = CalendarTableModel
    list_object_model = CalendarTableList
    create_object_schema = CalendarTableCreate
    controller = CalendarTableController

    def __init__(self) -> None:
        super().__init__("/calendar_table")

        # update route
        self.router.add_api_route(
            "/{calendar_table_id}",
            self.update_calendar_table,
            methods=["PATCH"],
            response_model=CalendarTableModel,
            status_code=HTTPStatus.OK,
        )

        # info route
        self.router.add_api_route(
            "/{calendar_table_id}/info",
            self.get_calendar_table_info,
            methods=["GET"],
            response_model=CalendarTableInfo,
        )

        # update column entity route
        self.router.add_api_route(
            "/{calendar_table_id}/column_entity",
            self.update_column_entity,
            methods=["PATCH"],
            response_model=CalendarTableModel,
            status_code=HTTPStatus.OK,
        )

        # update column critical data info route
        self.router.add_api_route(
            "/{calendar_table_id}/column_critical_data_info",
            self.update_column_critical_data_info,
            methods=["PATCH"],
            response_model=CalendarTableModel,
            status_code=HTTPStatus.OK,
        )

        # update column description
        self.router.add_api_route(
            "/{calendar_table_id}/column_description",
            self.update_column_description,
            methods=["PATCH"],
            response_model=CalendarTableModel,
            status_code=HTTPStatus.OK,
        )

        # delete route
        self.router.add_api_route(
            "/{calendar_table_id}",
            self.delete_object,
            methods=["DELETE"],
            response_model=DeleteResponse,
            status_code=HTTPStatus.OK,
        )

    async def get_object(
        self, request: Request, calendar_table_id: PydanticObjectId
    ) -> CalendarTableModel:
        return await super().get_object(request, calendar_table_id)

    async def list_audit_logs(
        self,
        request: Request,
        calendar_table_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        return await super().list_audit_logs(
            request,
            calendar_table_id,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            search=search,
        )

    async def update_description(
        self, request: Request, calendar_table_id: PydanticObjectId, data: DescriptionUpdate
    ) -> CalendarTableModel:
        return await super().update_description(request, calendar_table_id, data)

    async def create_object(
        self, request: Request, data: CalendarTableCreate
    ) -> CalendarTableModel:
        controller = self.get_controller_for_request(request)
        return await controller.create_table(data=data)

    async def get_calendar_table_info(
        self, request: Request, calendar_table_id: PydanticObjectId, verbose: bool = VerboseQuery
    ) -> CalendarTableInfo:
        """
        Retrieve calendar table info
        """
        controller = self.get_controller_for_request(request)
        info = await controller.get_info(
            document_id=ObjectId(calendar_table_id),
            verbose=verbose,
        )
        return info

    async def update_calendar_table(
        self, request: Request, calendar_table_id: PydanticObjectId, data: CalendarTableUpdate
    ) -> CalendarTableModel:
        """
        Update calendar table
        """
        controller = self.get_controller_for_request(request)
        calendar_table: CalendarTableModel = await controller.update_table(
            document_id=ObjectId(calendar_table_id),
            data=data,
        )
        return calendar_table

    async def update_column_entity(
        self, request: Request, calendar_table_id: PydanticObjectId, data: ColumnEntityUpdate
    ) -> CalendarTableModel:
        """
        Update column entity
        """
        controller = self.get_controller_for_request(request)
        calendar_table: CalendarTableModel = await controller.update_column_entity(
            document_id=ObjectId(calendar_table_id),
            column_name=data.column_name,
            entity_id=data.entity_id,
        )
        return calendar_table

    async def update_column_critical_data_info(
        self,
        request: Request,
        calendar_table_id: PydanticObjectId,
        data: ColumnCriticalDataInfoUpdate,
    ) -> CalendarTableModel:
        """
        Update column critical data info
        """
        controller = self.get_controller_for_request(request)
        calendar_table: CalendarTableModel = await controller.update_column_critical_data_info(
            document_id=ObjectId(calendar_table_id),
            column_name=data.column_name,
            critical_data_info=data.critical_data_info,  # type: ignore
        )
        return calendar_table

    async def update_column_description(
        self,
        request: Request,
        calendar_table_id: PydanticObjectId,
        data: ColumnDescriptionUpdate,
    ) -> CalendarTableModel:
        """
        Update column description
        """
        controller = self.get_controller_for_request(request)
        calendar_table: CalendarTableModel = await controller.update_column_description(
            document_id=ObjectId(calendar_table_id),
            column_name=data.column_name,
            description=data.description,
        )
        return calendar_table

    async def delete_object(
        self, request: Request, calendar_table_id: PydanticObjectId
    ) -> DeleteResponse:
        controller = self.get_controller_for_request(request)
        await controller.delete(document_id=ObjectId(calendar_table_id))
        return DeleteResponse()
