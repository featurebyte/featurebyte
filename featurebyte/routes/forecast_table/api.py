"""
ForecastTable API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.forecast_table import ForecastTableModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseApiRouter
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortDirQuery,
    VerboseQuery,
)
from featurebyte.routes.forecast_table.controller import ForecastTableController
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.forecast_table import (
    ForecastTableCreate,
    ForecastTableList,
    ForecastTableUpdate,
)
from featurebyte.schema.info import ForecastTableInfo
from featurebyte.schema.table import (
    ColumnCriticalDataInfoUpdate,
    ColumnDescriptionUpdate,
    ColumnEntityUpdate,
)

router = APIRouter(prefix="/forecast_table")


class ForecastTableRouter(
    BaseApiRouter[
        ForecastTableModel, ForecastTableList, ForecastTableCreate, ForecastTableController
    ]
):
    """
    Forecast table router
    """

    object_model = ForecastTableModel
    list_object_model = ForecastTableList
    create_object_schema = ForecastTableCreate
    controller = ForecastTableController

    def __init__(self) -> None:
        super().__init__("/forecast_table")

        # update route
        self.router.add_api_route(
            "/{forecast_table_id}",
            self.update_forecast_table,
            methods=["PATCH"],
            response_model=ForecastTableModel,
            status_code=HTTPStatus.OK,
        )

        # info route
        self.router.add_api_route(
            "/{forecast_table_id}/info",
            self.get_forecast_table_info,
            methods=["GET"],
            response_model=ForecastTableInfo,
        )

        # update column entity route
        self.router.add_api_route(
            "/{forecast_table_id}/column_entity",
            self.update_column_entity,
            methods=["PATCH"],
            response_model=ForecastTableModel,
            status_code=HTTPStatus.OK,
        )

        # update column critical data info route
        self.router.add_api_route(
            "/{forecast_table_id}/column_critical_data_info",
            self.update_column_critical_data_info,
            methods=["PATCH"],
            response_model=ForecastTableModel,
            status_code=HTTPStatus.OK,
        )

        # update column description
        self.router.add_api_route(
            "/{forecast_table_id}/column_description",
            self.update_column_description,
            methods=["PATCH"],
            response_model=ForecastTableModel,
            status_code=HTTPStatus.OK,
        )

        # delete route
        self.router.add_api_route(
            "/{forecast_table_id}",
            self.delete_object,
            methods=["DELETE"],
            response_model=DeleteResponse,
            status_code=HTTPStatus.OK,
        )

    async def get_object(
        self, request: Request, forecast_table_id: PydanticObjectId
    ) -> ForecastTableModel:
        return await super().get_object(request, forecast_table_id)

    async def list_audit_logs(
        self,
        request: Request,
        forecast_table_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        return await super().list_audit_logs(
            request,
            forecast_table_id,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            search=search,
        )

    async def update_description(
        self, request: Request, forecast_table_id: PydanticObjectId, data: DescriptionUpdate
    ) -> ForecastTableModel:
        return await super().update_description(request, forecast_table_id, data)

    async def create_object(
        self, request: Request, data: ForecastTableCreate
    ) -> ForecastTableModel:
        controller = self.get_controller_for_request(request)
        return await controller.create_table(data=data)

    async def get_forecast_table_info(
        self, request: Request, forecast_table_id: PydanticObjectId, verbose: bool = VerboseQuery
    ) -> ForecastTableInfo:
        """
        Retrieve forecast table info
        """
        controller = self.get_controller_for_request(request)
        info = await controller.get_info(
            document_id=ObjectId(forecast_table_id),
            verbose=verbose,
        )
        return info

    async def update_forecast_table(
        self, request: Request, forecast_table_id: PydanticObjectId, data: ForecastTableUpdate
    ) -> ForecastTableModel:
        """
        Update forecast table
        """
        controller = self.get_controller_for_request(request)
        forecast_table: ForecastTableModel = await controller.update_table(
            document_id=ObjectId(forecast_table_id),
            data=data,
        )
        return forecast_table

    async def update_column_entity(
        self, request: Request, forecast_table_id: PydanticObjectId, data: ColumnEntityUpdate
    ) -> ForecastTableModel:
        """
        Update column entity
        """
        controller = self.get_controller_for_request(request)
        forecast_table: ForecastTableModel = await controller.update_column_entity(
            document_id=ObjectId(forecast_table_id),
            column_name=data.column_name,
            entity_id=data.entity_id,
        )
        return forecast_table

    async def update_column_critical_data_info(
        self,
        request: Request,
        forecast_table_id: PydanticObjectId,
        data: ColumnCriticalDataInfoUpdate,
    ) -> ForecastTableModel:
        """
        Update column critical data info
        """
        controller = self.get_controller_for_request(request)
        forecast_table: ForecastTableModel = await controller.update_column_critical_data_info(
            document_id=ObjectId(forecast_table_id),
            column_name=data.column_name,
            critical_data_info=data.critical_data_info,  # type: ignore
        )
        return forecast_table

    async def update_column_description(
        self,
        request: Request,
        forecast_table_id: PydanticObjectId,
        data: ColumnDescriptionUpdate,
    ) -> ForecastTableModel:
        """
        Update column description
        """
        controller = self.get_controller_for_request(request)
        forecast_table: ForecastTableModel = await controller.update_column_description(
            document_id=ObjectId(forecast_table_id),
            column_name=data.column_name,
            description=data.description,
        )
        return forecast_table

    async def delete_object(
        self, request: Request, forecast_table_id: PydanticObjectId
    ) -> DeleteResponse:
        controller = self.get_controller_for_request(request)
        await controller.delete(document_id=ObjectId(forecast_table_id))
        return DeleteResponse()
