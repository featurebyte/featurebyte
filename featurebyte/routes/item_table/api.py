"""
ItemTable API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.item_table import ItemTableModel
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
from featurebyte.routes.item_table.controller import ItemTableController
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.info import ItemTableInfo
from featurebyte.schema.item_table import ItemTableCreate, ItemTableList, ItemTableUpdate
from featurebyte.schema.table import (
    ColumnCriticalDataInfoUpdate,
    ColumnDescriptionUpdate,
    ColumnEntityUpdate,
)

router = APIRouter(prefix="/item_table")


class ItemTableRouter(
    BaseApiRouter[ItemTableModel, ItemTableList, ItemTableCreate, ItemTableController]
):
    """
    Item table router
    """

    object_model = ItemTableModel
    list_object_model = ItemTableList
    create_object_schema = ItemTableCreate
    controller = ItemTableController

    def __init__(self) -> None:
        super().__init__("/item_table")

        # update route
        self.router.add_api_route(
            "/{item_table_id}",
            self.update_item_table,
            methods=["PATCH"],
            response_model=ItemTableModel,
            status_code=HTTPStatus.OK,
        )

        # info route
        self.router.add_api_route(
            "/{item_table_id}/info",
            self.get_item_table_info,
            methods=["GET"],
            response_model=ItemTableInfo,
        )

        # update column entity route
        self.router.add_api_route(
            "/{item_table_id}/column_entity",
            self.update_column_entity,
            methods=["PATCH"],
            response_model=ItemTableModel,
            status_code=HTTPStatus.OK,
        )

        # update column critical data info route
        self.router.add_api_route(
            "/{item_table_id}/column_critical_data_info",
            self.update_column_critical_data_info,
            methods=["PATCH"],
            response_model=ItemTableModel,
            status_code=HTTPStatus.OK,
        )

        # update column description
        self.router.add_api_route(
            "/{item_table_id}/column_description",
            self.update_column_description,
            methods=["PATCH"],
            response_model=ItemTableModel,
            status_code=HTTPStatus.OK,
        )

    async def get_object(self, request: Request, item_table_id: PydanticObjectId) -> ItemTableModel:
        return await super().get_object(request, item_table_id)

    async def list_audit_logs(
        self,
        request: Request,
        item_table_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        return await super().list_audit_logs(
            request,
            item_table_id,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            search=search,
        )

    async def update_description(
        self, request: Request, item_table_id: PydanticObjectId, data: DescriptionUpdate
    ) -> ItemTableModel:
        return await super().update_description(request, item_table_id, data)

    async def create_object(self, request: Request, data: ItemTableCreate) -> ItemTableModel:
        controller = self.get_controller_for_request(request)
        return await controller.create_table(data=data)

    async def get_item_table_info(
        self, request: Request, item_table_id: PydanticObjectId, verbose: bool = VerboseQuery
    ) -> ItemTableInfo:
        """
        Retrieve item table info
        """
        controller = self.get_controller_for_request(request)
        info = await controller.get_info(
            document_id=ObjectId(item_table_id),
            verbose=verbose,
        )
        return info

    async def update_item_table(
        self, request: Request, item_table_id: PydanticObjectId, data: ItemTableUpdate
    ) -> ItemTableModel:
        """
        Update item table
        """
        controller = self.get_controller_for_request(request)
        item_table: ItemTableModel = await controller.update_table(
            document_id=ObjectId(item_table_id),
            data=data,
        )
        return item_table

    async def update_column_entity(
        self, request: Request, item_table_id: PydanticObjectId, data: ColumnEntityUpdate
    ) -> ItemTableModel:
        """
        Update column entity
        """
        controller = self.get_controller_for_request(request)
        item_table: ItemTableModel = await controller.update_column_entity(
            document_id=ObjectId(item_table_id),
            column_name=data.column_name,
            entity_id=data.entity_id,
        )
        return item_table

    async def update_column_critical_data_info(
        self,
        request: Request,
        item_table_id: PydanticObjectId,
        data: ColumnCriticalDataInfoUpdate,
    ) -> ItemTableModel:
        """
        Update column critical data info
        """
        controller = self.get_controller_for_request(request)
        item_table: ItemTableModel = await controller.update_column_critical_data_info(
            document_id=ObjectId(item_table_id),
            column_name=data.column_name,
            critical_data_info=data.critical_data_info,  # type: ignore
        )
        return item_table

    async def update_column_description(
        self,
        request: Request,
        item_table_id: PydanticObjectId,
        data: ColumnDescriptionUpdate,
    ) -> ItemTableModel:
        """
        Update column description
        """
        controller = self.get_controller_for_request(request)
        item_table: ItemTableModel = await controller.update_column_description(
            document_id=ObjectId(item_table_id),
            column_name=data.column_name,
            description=data.description,
        )
        return item_table

    async def delete_object(
        self, request: Request, item_table_id: PydanticObjectId
    ) -> DeleteResponse:
        controller = self.get_controller_for_request(request)
        await controller.delete(document_id=ObjectId(item_table_id))
        return DeleteResponse()
