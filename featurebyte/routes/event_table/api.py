"""
EventTable API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import List, Optional

from bson import ObjectId
from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.event_table import FeatureJobSettingHistoryEntry
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
from featurebyte.routes.event_table.controller import EventTableController
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.event_table import (
    EventTableCreate,
    EventTableList,
    EventTableRead,
    EventTableUpdate,
)
from featurebyte.schema.info import EventTableInfo
from featurebyte.schema.table import (
    ColumnCriticalDataInfoUpdate,
    ColumnDescriptionUpdate,
    ColumnEntityUpdate,
)

router = APIRouter(prefix="/event_table")


class EventTableRouter(
    BaseApiRouter[EventTableRead, EventTableList, EventTableCreate, EventTableController]
):
    """
    Event table router
    """

    object_model = EventTableRead
    list_object_model = EventTableList
    create_object_schema = EventTableCreate
    controller = EventTableController

    def __init__(self) -> None:
        super().__init__("/event_table")

        # update route
        self.router.add_api_route(
            "/{event_table_id}",
            self.update_event_table,
            methods=["PATCH"],
            response_model=EventTableRead,
            status_code=HTTPStatus.OK,
        )

        # info route
        self.router.add_api_route(
            "/{event_table_id}/info",
            self.get_event_table_info,
            methods=["GET"],
            response_model=EventTableInfo,
        )

        # update column entity route
        self.router.add_api_route(
            "/{event_table_id}/column_entity",
            self.update_column_entity,
            methods=["PATCH"],
            response_model=EventTableRead,
            status_code=HTTPStatus.OK,
        )

        # update column critical data info route
        self.router.add_api_route(
            "/{event_table_id}/column_critical_data_info",
            self.update_column_critical_data_info,
            methods=["PATCH"],
            response_model=EventTableRead,
            status_code=HTTPStatus.OK,
        )

        # update column description
        self.router.add_api_route(
            "/{event_table_id}/column_description",
            self.update_column_description,
            methods=["PATCH"],
            response_model=EventTableRead,
            status_code=HTTPStatus.OK,
        )

        # list default feature job setting history
        self.router.add_api_route(
            "/history/default_feature_job_setting/{event_table_id}",
            self.list_default_feature_job_setting_history,
            methods=["GET"],
            response_model=List[FeatureJobSettingHistoryEntry],
        )

    async def get_object(
        self, request: Request, event_table_id: PydanticObjectId
    ) -> EventTableRead:
        return await super().get_object(request, event_table_id)

    async def list_audit_logs(
        self,
        request: Request,
        event_table_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        return await super().list_audit_logs(
            request,
            event_table_id,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            search=search,
        )

    async def update_description(
        self, request: Request, event_table_id: PydanticObjectId, data: DescriptionUpdate
    ) -> EventTableRead:
        return await super().update_description(request, event_table_id, data)

    async def create_object(self, request: Request, data: EventTableCreate) -> EventTableRead:
        controller = self.get_controller_for_request(request)
        event_table = await controller.create_table(data=data)
        return EventTableRead(**event_table.model_dump(by_alias=True))

    async def get_event_table_info(
        self, request: Request, event_table_id: PydanticObjectId, verbose: bool = VerboseQuery
    ) -> EventTableInfo:
        """
        Retrieve event table info
        """
        controller = self.get_controller_for_request(request)
        info = await controller.get_info(
            document_id=ObjectId(event_table_id),
            verbose=verbose,
        )
        return info

    async def update_event_table(
        self, request: Request, event_table_id: PydanticObjectId, data: EventTableUpdate
    ) -> EventTableRead:
        """
        Update event table
        """
        controller = self.get_controller_for_request(request)
        event_table = await controller.update_table(
            document_id=ObjectId(event_table_id),
            data=data,
        )
        return EventTableRead(**event_table.model_dump(by_alias=True))

    async def update_column_entity(
        self, request: Request, event_table_id: PydanticObjectId, data: ColumnEntityUpdate
    ) -> EventTableRead:
        """
        Update column entity
        """
        controller = self.get_controller_for_request(request)
        event_table = await controller.update_column_entity(
            document_id=ObjectId(event_table_id),
            column_name=data.column_name,
            entity_id=data.entity_id,
        )
        return EventTableRead(**event_table.model_dump(by_alias=True))

    async def update_column_critical_data_info(
        self,
        request: Request,
        event_table_id: PydanticObjectId,
        data: ColumnCriticalDataInfoUpdate,
    ) -> EventTableRead:
        """
        Update column critical data info
        """
        controller = self.get_controller_for_request(request)
        event_table = await controller.update_column_critical_data_info(
            document_id=ObjectId(event_table_id),
            column_name=data.column_name,
            critical_data_info=data.critical_data_info,  # type: ignore
        )
        return EventTableRead(**event_table.model_dump(by_alias=True))

    async def update_column_description(
        self,
        request: Request,
        event_table_id: PydanticObjectId,
        data: ColumnDescriptionUpdate,
    ) -> EventTableRead:
        """
        Update column description
        """
        controller = self.get_controller_for_request(request)
        event_table = await controller.update_column_description(
            document_id=ObjectId(event_table_id),
            column_name=data.column_name,
            description=data.description,
        )
        return EventTableRead(**event_table.model_dump(by_alias=True))

    async def list_default_feature_job_setting_history(
        self,
        request: Request,
        event_table_id: PydanticObjectId,
    ) -> List[FeatureJobSettingHistoryEntry]:
        """
        List EventTable default feature job settings history
        """
        controller = self.get_controller_for_request(request)
        history_values = await controller.list_field_history(
            document_id=ObjectId(event_table_id),
            field="default_feature_job_setting",
        )

        return [
            FeatureJobSettingHistoryEntry(
                created_at=record.created_at,
                setting=record.value,
            )
            for record in history_values
        ]

    async def delete_object(
        self, request: Request, event_table_id: PydanticObjectId
    ) -> DeleteResponse:
        controller = self.get_controller_for_request(request)
        await controller.delete(document_id=ObjectId(event_table_id))
        return DeleteResponse()
