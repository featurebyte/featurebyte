"""
TargetTable API routes
"""

from __future__ import annotations

import json
from http import HTTPStatus
from typing import Any, Dict, Optional, cast

from fastapi import Form, Query, Request, UploadFile
from starlette.responses import StreamingResponse

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.target_table import TargetTableModel
from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_materialized_table_router import BaseMaterializedTableRouter
from featurebyte.routes.common.schema import (
    PREVIEW_DEFAULT,
    PREVIEW_LIMIT,
    AuditLogSortByQuery,
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
    VerboseQuery,
)
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.info import TargetTableInfo
from featurebyte.schema.target_table import TargetTableCreate, TargetTableList
from featurebyte.schema.task import Task


class TargetTableRouter(BaseMaterializedTableRouter[TargetTableModel]):
    """
    Target table router
    """

    table_model = TargetTableModel
    controller = "target_table_controller"

    def __init__(self, prefix: str):
        super().__init__(prefix=prefix)
        self.router.add_api_route(
            "",
            self.create_target_table,
            methods=["POST"],
            response_model=Task,
            status_code=HTTPStatus.CREATED,
        )
        self.router.add_api_route(
            "/{target_table_id}",
            self.delete_target_table,
            methods=["DELETE"],
            response_model=Task,
            status_code=HTTPStatus.ACCEPTED,
        )
        self.router.add_api_route(
            "",
            self.list_target_tables,
            methods=["GET"],
            response_model=TargetTableList,
        )
        self.router.add_api_route(
            "/audit/{target_table_id}",
            self.list_target_table_audit_logs,
            methods=["GET"],
            response_model=AuditDocumentList,
        )
        self.router.add_api_route(
            "/{target_table_id}/info",
            self.get_target_table_info,
            methods=["GET"],
            response_model=TargetTableInfo,
        )
        self.router.add_api_route(
            "/pyarrow_table/{target_table_id}",
            self.download_table_as_pyarrow_table,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/parquet/{target_table_id}",
            self.download_table_as_parquet,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/{target_table_id}/description",
            self.update_target_table_description,
            methods=["PATCH"],
            response_model=TargetTableModel,
        )
        self.router.add_api_route(
            "/{target_table_id}/preview",
            self.preview_target_table,
            methods=["POST"],
        )

    async def get_table(
        self, request: Request, target_table_id: PydanticObjectId
    ) -> TargetTableModel:
        """
        Test random string
        """
        return await super().get_table(request, target_table_id)

    @staticmethod
    async def create_target_table(
        request: Request,
        payload: str = Form(),
        observation_set: Optional[UploadFile] = None,
    ) -> Task:
        """
        Create TargetTable by submitting a materialization task
        """
        payload_dict = json.loads(payload)
        # Note that only target_id, or graph should be passed in.
        # This cleanup is done here on the API layer due to backwards compatibility, where older clients may be passing
        # in payloads that have both. This is ok to change as the target_id was not being used for anything.
        if payload_dict["target_id"] is not None and payload_dict["graph"] is not None:
            payload_dict["target_id"] = None
        data = TargetTableCreate(**payload_dict)
        controller = request.state.app_container.target_table_controller
        task_submit: Task = await controller.create_table(
            data=data,
            observation_set=observation_set,
        )
        return task_submit

    @staticmethod
    async def delete_target_table(request: Request, target_table_id: PydanticObjectId) -> Task:
        """
        Delete TargetTable
        """
        controller = request.state.app_container.target_table_controller
        task: Task = await controller.delete_materialized_table(document_id=target_table_id)
        return task

    @staticmethod
    async def list_target_tables(
        request: Request,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = SortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
        name: Optional[str] = NameQuery,
    ) -> TargetTableList:
        """
        List TargetTables
        """
        controller = request.state.app_container.target_table_controller
        target_table_list: TargetTableList = await controller.list(
            page=page,
            page_size=page_size,
            sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
            search=search,
            name=name,
        )
        return target_table_list

    @staticmethod
    async def list_target_table_audit_logs(
        request: Request,
        target_table_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        """
        List TargetTable audit logs
        """
        controller = request.state.app_container.target_table_controller
        audit_doc_list: AuditDocumentList = await controller.list_audit(
            document_id=target_table_id,
            page=page,
            page_size=page_size,
            sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
            search=search,
        )
        return audit_doc_list

    @staticmethod
    async def get_target_table_info(
        request: Request, target_table_id: PydanticObjectId, verbose: bool = VerboseQuery
    ) -> TargetTableInfo:
        """
        Get TargetTable info
        """
        controller = request.state.app_container.target_table_controller
        info = await controller.get_info(document_id=target_table_id, verbose=verbose)
        return cast(TargetTableInfo, info)

    @staticmethod
    async def download_table_as_pyarrow_table(
        request: Request, target_table_id: PydanticObjectId
    ) -> StreamingResponse:
        """
        Download TargetTable as pyarrow table
        """
        controller = request.state.app_container.target_table_controller
        result: StreamingResponse = await controller.download_materialized_table(
            document_id=target_table_id,
        )
        return result

    @staticmethod
    async def download_table_as_parquet(
        request: Request, target_table_id: PydanticObjectId
    ) -> StreamingResponse:
        """
        Download TargetTable as parquet file
        """
        controller = request.state.app_container.target_table_controller
        result: StreamingResponse = await controller.download_materialized_table_as_parquet(
            document_id=target_table_id,
        )
        return result

    @staticmethod
    async def update_target_table_description(
        request: Request,
        target_table_id: PydanticObjectId,
        data: DescriptionUpdate,
    ) -> TargetTableModel:
        """
        Update target_table description
        """
        controller = request.state.app_container.target_table_controller
        target_table: TargetTableModel = await controller.update_description(
            document_id=target_table_id,
            description=data.description,
        )
        return target_table

    @staticmethod
    async def preview_target_table(
        request: Request,
        target_table_id: PydanticObjectId,
        limit: int = Query(default=PREVIEW_DEFAULT, gt=0, le=PREVIEW_LIMIT),
    ) -> Dict[str, Any]:
        """
        Preview TargetTable
        """
        controller = request.state.app_container.target_table_controller
        result: dict[str, Any] = await controller.preview_materialized_table(
            document_id=target_table_id,
            limit=limit,
        )
        return result
