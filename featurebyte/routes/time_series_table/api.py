"""
TimeSeriesTable API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import List, Optional

from bson import ObjectId
from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.time_series_table import CronFeatureJobSettingHistoryEntry
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
from featurebyte.routes.time_series_table.controller import TimeSeriesTableController
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.info import TimeSeriesTableInfo
from featurebyte.schema.table import (
    ColumnCriticalDataInfoUpdate,
    ColumnDescriptionUpdate,
    ColumnEntityUpdate,
)
from featurebyte.schema.time_series_table import (
    TimeSeriesTableCreate,
    TimeSeriesTableList,
    TimeSeriesTableRead,
    TimeSeriesTableUpdate,
)

router = APIRouter(prefix="/time_series_table")


class TimeSeriesTableRouter(
    BaseApiRouter[
        TimeSeriesTableRead, TimeSeriesTableList, TimeSeriesTableCreate, TimeSeriesTableController
    ]
):
    """
    Time Series table router
    """

    object_model = TimeSeriesTableRead
    list_object_model = TimeSeriesTableList
    create_object_schema = TimeSeriesTableCreate
    controller = TimeSeriesTableController

    def __init__(self) -> None:
        super().__init__("/time_series_table")

        # update route
        self.router.add_api_route(
            "/{time_series_table_id}",
            self.update_time_series_table,
            methods=["PATCH"],
            response_model=TimeSeriesTableRead,
            status_code=HTTPStatus.OK,
        )

        # info route
        self.router.add_api_route(
            "/{time_series_table_id}/info",
            self.get_time_series_table_info,
            methods=["GET"],
            response_model=TimeSeriesTableInfo,
        )

        # update column entity route
        self.router.add_api_route(
            "/{time_series_table_id}/column_entity",
            self.update_column_entity,
            methods=["PATCH"],
            response_model=TimeSeriesTableRead,
            status_code=HTTPStatus.OK,
        )

        # update column critical data info route
        self.router.add_api_route(
            "/{time_series_table_id}/column_critical_data_info",
            self.update_column_critical_data_info,
            methods=["PATCH"],
            response_model=TimeSeriesTableRead,
            status_code=HTTPStatus.OK,
        )

        # update column description
        self.router.add_api_route(
            "/{time_series_table_id}/column_description",
            self.update_column_description,
            methods=["PATCH"],
            response_model=TimeSeriesTableRead,
            status_code=HTTPStatus.OK,
        )

        # list default feature job setting history
        self.router.add_api_route(
            "/history/default_feature_job_setting/{time_series_table_id}",
            self.list_default_feature_job_setting_history,
            methods=["GET"],
            response_model=List[CronFeatureJobSettingHistoryEntry],
        )

    async def get_object(
        self, request: Request, time_series_table_id: PydanticObjectId
    ) -> TimeSeriesTableRead:
        return await super().get_object(request, time_series_table_id)

    async def list_audit_logs(
        self,
        request: Request,
        time_series_table_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        return await super().list_audit_logs(
            request,
            time_series_table_id,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            search=search,
        )

    async def update_description(
        self, request: Request, time_series_table_id: PydanticObjectId, data: DescriptionUpdate
    ) -> TimeSeriesTableRead:
        return await super().update_description(request, time_series_table_id, data)

    async def create_object(
        self, request: Request, data: TimeSeriesTableCreate
    ) -> TimeSeriesTableRead:
        controller = self.get_controller_for_request(request)
        time_series_table = await controller.create_table(data=data)
        return TimeSeriesTableRead(**time_series_table.model_dump(by_alias=True))

    async def get_time_series_table_info(
        self, request: Request, time_series_table_id: PydanticObjectId, verbose: bool = VerboseQuery
    ) -> TimeSeriesTableInfo:
        """
        Retrieve time series table info
        """
        controller = self.get_controller_for_request(request)
        info = await controller.get_info(
            document_id=ObjectId(time_series_table_id),
            verbose=verbose,
        )
        return info

    async def update_time_series_table(
        self, request: Request, time_series_table_id: PydanticObjectId, data: TimeSeriesTableUpdate
    ) -> TimeSeriesTableRead:
        """
        Update time series table
        """
        controller = self.get_controller_for_request(request)
        time_series_table = await controller.update_table(
            document_id=ObjectId(time_series_table_id),
            data=data,
        )
        return TimeSeriesTableRead(**time_series_table.model_dump(by_alias=True))

    async def update_column_entity(
        self, request: Request, time_series_table_id: PydanticObjectId, data: ColumnEntityUpdate
    ) -> TimeSeriesTableRead:
        """
        Update column entity
        """
        controller = self.get_controller_for_request(request)
        time_series_table = await controller.update_column_entity(
            document_id=ObjectId(time_series_table_id),
            column_name=data.column_name,
            entity_id=data.entity_id,
        )
        return TimeSeriesTableRead(**time_series_table.model_dump(by_alias=True))

    async def update_column_critical_data_info(
        self,
        request: Request,
        time_series_table_id: PydanticObjectId,
        data: ColumnCriticalDataInfoUpdate,
    ) -> TimeSeriesTableRead:
        """
        Update column critical data info
        """
        controller = self.get_controller_for_request(request)
        time_series_table = await controller.update_column_critical_data_info(
            document_id=ObjectId(time_series_table_id),
            column_name=data.column_name,
            critical_data_info=data.critical_data_info,  # type: ignore
        )
        return TimeSeriesTableRead(**time_series_table.model_dump(by_alias=True))

    async def update_column_description(
        self,
        request: Request,
        time_series_table_id: PydanticObjectId,
        data: ColumnDescriptionUpdate,
    ) -> TimeSeriesTableRead:
        """
        Update column description
        """
        controller = self.get_controller_for_request(request)
        time_series_table = await controller.update_column_description(
            document_id=ObjectId(time_series_table_id),
            column_name=data.column_name,
            description=data.description,
        )
        return TimeSeriesTableRead(**time_series_table.model_dump(by_alias=True))

    async def list_default_feature_job_setting_history(
        self,
        request: Request,
        time_series_table_id: PydanticObjectId,
    ) -> List[CronFeatureJobSettingHistoryEntry]:
        """
        List TimeSeriesTable default feature job settings history
        """
        controller = self.get_controller_for_request(request)
        history_values = await controller.list_field_history(
            document_id=ObjectId(time_series_table_id),
            field="default_feature_job_setting",
        )

        return [
            CronFeatureJobSettingHistoryEntry(
                created_at=record.created_at,
                setting=record.value,
            )
            for record in history_values
        ]

    async def delete_object(
        self, request: Request, time_series_table_id: PydanticObjectId
    ) -> DeleteResponse:
        controller = self.get_controller_for_request(request)
        await controller.delete(document_id=ObjectId(time_series_table_id))
        return DeleteResponse()
