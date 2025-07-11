"""
DimensionTable API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.dimension_table import DimensionTableModel
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
from featurebyte.routes.dimension_table.controller import DimensionTableController
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.dimension_table import (
    DimensionTableCreate,
    DimensionTableList,
    DimensionTableUpdate,
)
from featurebyte.schema.info import DimensionTableInfo
from featurebyte.schema.table import (
    ColumnCriticalDataInfoUpdate,
    ColumnDescriptionUpdate,
    ColumnEntityUpdate,
)

router = APIRouter(prefix="/dimension_table")


class DimensionTableRouter(
    BaseApiRouter[
        DimensionTableModel, DimensionTableList, DimensionTableCreate, DimensionTableController
    ]
):
    """
    Dimension table router
    """

    object_model = DimensionTableModel
    list_object_model = DimensionTableList
    create_object_schema = DimensionTableCreate
    controller = DimensionTableController

    def __init__(self) -> None:
        super().__init__("/dimension_table")

        # update route
        self.router.add_api_route(
            "/{dimension_table_id}",
            self.update_dimension_table,
            methods=["PATCH"],
            response_model=DimensionTableModel,
            status_code=HTTPStatus.OK,
        )

        # info route
        self.router.add_api_route(
            "/{dimension_table_id}/info",
            self.get_dimension_table_info,
            methods=["GET"],
            response_model=DimensionTableInfo,
        )

        # update column entity route
        self.router.add_api_route(
            "/{dimension_table_id}/column_entity",
            self.update_column_entity,
            methods=["PATCH"],
            response_model=DimensionTableModel,
            status_code=HTTPStatus.OK,
        )

        # update column critical data info route
        self.router.add_api_route(
            "/{dimension_table_id}/column_critical_data_info",
            self.update_column_critical_data_info,
            methods=["PATCH"],
            response_model=DimensionTableModel,
            status_code=HTTPStatus.OK,
        )

        # update column description
        self.router.add_api_route(
            "/{dimension_table_id}/column_description",
            self.update_column_description,
            methods=["PATCH"],
            response_model=DimensionTableModel,
            status_code=HTTPStatus.OK,
        )

    async def get_object(
        self, request: Request, dimension_table_id: PydanticObjectId
    ) -> DimensionTableModel:
        return await super().get_object(request, dimension_table_id)

    async def list_audit_logs(
        self,
        request: Request,
        dimension_table_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        return await super().list_audit_logs(
            request,
            dimension_table_id,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            search=search,
        )

    async def update_description(
        self, request: Request, dimension_table_id: PydanticObjectId, data: DescriptionUpdate
    ) -> DimensionTableModel:
        return await super().update_description(request, dimension_table_id, data)

    async def create_object(
        self, request: Request, data: DimensionTableCreate
    ) -> DimensionTableModel:
        controller = self.get_controller_for_request(request)
        return await controller.create_table(data=data)

    async def get_dimension_table_info(
        self, request: Request, dimension_table_id: PydanticObjectId, verbose: bool = VerboseQuery
    ) -> DimensionTableInfo:
        """
        Retrieve dimension table info
        """
        controller = self.get_controller_for_request(request)
        info = await controller.get_info(
            document_id=ObjectId(dimension_table_id),
            verbose=verbose,
        )
        return info

    async def update_dimension_table(
        self, request: Request, dimension_table_id: PydanticObjectId, data: DimensionTableUpdate
    ) -> DimensionTableModel:
        """
        Update dimension table
        """
        controller = self.get_controller_for_request(request)
        dimension_table: DimensionTableModel = await controller.update_table(
            document_id=ObjectId(dimension_table_id),
            data=data,
        )
        return dimension_table

    async def update_column_entity(
        self, request: Request, dimension_table_id: PydanticObjectId, data: ColumnEntityUpdate
    ) -> DimensionTableModel:
        """
        Update column entity
        """
        controller = self.get_controller_for_request(request)
        dimension_table: DimensionTableModel = await controller.update_column_entity(
            document_id=ObjectId(dimension_table_id),
            column_name=data.column_name,
            entity_id=data.entity_id,
        )
        return dimension_table

    async def update_column_critical_data_info(
        self,
        request: Request,
        dimension_table_id: PydanticObjectId,
        data: ColumnCriticalDataInfoUpdate,
    ) -> DimensionTableModel:
        """
        Update column critical data info
        """
        controller = self.get_controller_for_request(request)
        dimension_table: DimensionTableModel = await controller.update_column_critical_data_info(
            document_id=ObjectId(dimension_table_id),
            column_name=data.column_name,
            critical_data_info=data.critical_data_info,  # type: ignore
        )
        return dimension_table

    async def update_column_description(
        self,
        request: Request,
        dimension_table_id: PydanticObjectId,
        data: ColumnDescriptionUpdate,
    ) -> DimensionTableModel:
        """
        Update column description
        """
        controller = self.get_controller_for_request(request)
        dimension_table: DimensionTableModel = await controller.update_column_description(
            document_id=ObjectId(dimension_table_id),
            column_name=data.column_name,
            description=data.description,
        )
        return dimension_table

    async def delete_object(
        self, request: Request, dimension_table_id: PydanticObjectId
    ) -> DeleteResponse:
        controller = self.get_controller_for_request(request)
        await controller.delete(document_id=ObjectId(dimension_table_id))
        return DeleteResponse()
