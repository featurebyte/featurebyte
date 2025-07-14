"""
BatchFeatureTable API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any, Dict, Optional, cast

from fastapi import APIRouter, Query, Request
from starlette.responses import StreamingResponse

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseRouter
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
from featurebyte.schema.batch_feature_table import (
    BatchExternalFeatureTableCreate,
    BatchFeatureTableCreate,
    BatchFeatureTableList,
)
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.info import BatchFeatureTableInfo
from featurebyte.schema.task import Task

router = APIRouter(prefix="/batch_feature_table")


class BatchFeatureTableRouter(BaseRouter):
    """
    Batch feature table router
    """

    def __init__(self) -> None:
        super().__init__(router=router)

        self.router.add_api_route(
            path="",
            endpoint=self.create_batch_feature_table,
            methods=["POST"],
            response_model=Task,
            status_code=HTTPStatus.CREATED,
        )

        self.router.add_api_route(
            path="/feature_table",
            endpoint=self.append_batch_features_to_feature_table,
            methods=["POST"],
            response_model=Task,
            status_code=HTTPStatus.CREATED,
        )

        self.router.add_api_route(
            path="/{batch_feature_table_id}",
            endpoint=self.get_batch_feature_table,
            methods=["GET"],
            response_model=BatchFeatureTableModel,
        )

        self.router.add_api_route(
            path="/{batch_feature_table_id}",
            endpoint=self.delete_batch_feature_table,
            methods=["DELETE"],
            response_model=Task,
            status_code=HTTPStatus.ACCEPTED,
        )

        self.router.add_api_route(
            path="",
            endpoint=self.list_batch_feature_tables,
            methods=["GET"],
            response_model=BatchFeatureTableList,
        )

        self.router.add_api_route(
            path="/audit/{batch_feature_table_id}",
            endpoint=self.list_batch_feature_table_audit_logs,
            methods=["GET"],
            response_model=AuditDocumentList,
        )

        self.router.add_api_route(
            path="/{batch_feature_table_id}/info",
            endpoint=self.get_batch_feature_table_info,
            methods=["GET"],
            response_model=BatchFeatureTableInfo,
        )

        self.router.add_api_route(
            path="/pyarrow_table/{batch_feature_table_id}",
            endpoint=self.download_table_as_pyarrow_table,
            methods=["GET"],
            response_class=StreamingResponse,
        )

        self.router.add_api_route(
            path="/parquet/{batch_feature_table_id}",
            endpoint=self.download_table_as_parquet,
            methods=["GET"],
            response_class=StreamingResponse,
        )

        self.router.add_api_route(
            path="/{batch_feature_table_id}/description",
            endpoint=self.update_batch_feature_table_description,
            methods=["PATCH"],
            response_model=BatchFeatureTableModel,
        )

        self.router.add_api_route(
            path="/{batch_feature_table_id}/preview",
            endpoint=self.preview_batch_feature_table,
            methods=["POST"],
            response_model=Dict[str, Any],
        )

        self.router.add_api_route(
            path="/{batch_feature_table_id}",
            endpoint=self.recreate_batch_feature_table,
            methods=["POST"],
            response_model=Task,
            status_code=HTTPStatus.CREATED,
        )

    async def create_batch_feature_table(
        self,
        request: Request,
        data: BatchFeatureTableCreate,
    ) -> Task:
        """
        Create BatchFeatureTable by submitting a materialization task
        """
        controller = request.state.app_container.batch_feature_table_controller
        task_submit: Task = await controller.create_batch_feature_table(
            data=data,
        )
        return task_submit

    async def append_batch_features_to_feature_table(
        self,
        request: Request,
        data: BatchExternalFeatureTableCreate,
    ) -> Task:
        """
        Append batch features to an unmanaged feature table by submitting a materialization task
        """
        controller = request.state.app_container.batch_feature_table_controller
        task_submit: Task = await controller.create_batch_feature_table(
            data=data,
        )
        return task_submit

    async def get_batch_feature_table(
        self, request: Request, batch_feature_table_id: PydanticObjectId
    ) -> BatchFeatureTableModel:
        """
        Get BatchFeatureTable
        """
        controller = request.state.app_container.batch_feature_table_controller
        batch_feature_table: BatchFeatureTableModel = await controller.get(
            document_id=batch_feature_table_id
        )
        return batch_feature_table

    async def delete_batch_feature_table(
        self, request: Request, batch_feature_table_id: PydanticObjectId
    ) -> Task:
        """
        Delete BatchFeatureTable
        """
        controller = request.state.app_container.batch_feature_table_controller
        task: Task = await controller.delete_materialized_table(document_id=batch_feature_table_id)
        return task

    async def list_batch_feature_tables(
        self,
        request: Request,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = SortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
        name: Optional[str] = NameQuery,
    ) -> BatchFeatureTableList:
        """
        List BatchFeatureTables
        """
        controller = request.state.app_container.batch_feature_table_controller
        batch_feature_table_list: BatchFeatureTableList = await controller.list(
            page=page,
            page_size=page_size,
            sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
            search=search,
            name=name,
        )
        return batch_feature_table_list

    async def list_batch_feature_table_audit_logs(
        self,
        request: Request,
        batch_feature_table_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        """
        List BatchFeatureTable audit logs
        """
        controller = request.state.app_container.batch_feature_table_controller
        audit_doc_list: AuditDocumentList = await controller.list_audit(
            document_id=batch_feature_table_id,
            page=page,
            page_size=page_size,
            sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
            search=search,
        )
        return audit_doc_list

    async def get_batch_feature_table_info(
        self,
        request: Request,
        batch_feature_table_id: PydanticObjectId,
        verbose: bool = VerboseQuery,
    ) -> BatchFeatureTableInfo:
        """
        Get BatchFeatureTable info
        """
        controller = request.state.app_container.batch_feature_table_controller
        info = await controller.get_info(document_id=batch_feature_table_id, verbose=verbose)
        return cast(BatchFeatureTableInfo, info)

    async def download_table_as_pyarrow_table(
        self, request: Request, batch_feature_table_id: PydanticObjectId
    ) -> StreamingResponse:
        """
        Download BatchFeatureTable as pyarrow table
        """
        controller = request.state.app_container.batch_feature_table_controller
        result: StreamingResponse = await controller.download_materialized_table(
            document_id=batch_feature_table_id,
        )
        return result

    async def download_table_as_parquet(
        self, request: Request, batch_feature_table_id: PydanticObjectId
    ) -> StreamingResponse:
        """
        Download BatchFeatureTable as parquet file
        """
        controller = request.state.app_container.batch_feature_table_controller
        result: StreamingResponse = await controller.download_materialized_table_as_parquet(
            document_id=batch_feature_table_id,
        )
        return result

    async def update_batch_feature_table_description(
        self,
        request: Request,
        batch_feature_table_id: PydanticObjectId,
        data: DescriptionUpdate,
    ) -> BatchFeatureTableModel:
        """
        Update batch_feature_table description
        """
        controller = request.state.app_container.batch_feature_table_controller
        batch_feature_table: BatchFeatureTableModel = await controller.update_description(
            document_id=batch_feature_table_id,
            description=data.description,
        )
        return batch_feature_table

    async def preview_batch_feature_table(
        self,
        request: Request,
        batch_feature_table_id: PydanticObjectId,
        limit: int = Query(default=PREVIEW_DEFAULT, gt=0, le=PREVIEW_LIMIT),
    ) -> Dict[str, Any]:
        """
        Preview batch feature table
        """
        controller = request.state.app_container.batch_feature_table_controller
        preview: Dict[str, Any] = await controller.preview_materialized_table(
            document_id=batch_feature_table_id,
            limit=limit,
        )
        return preview

    async def recreate_batch_feature_table(
        self,
        request: Request,
        batch_feature_table_id: PydanticObjectId,
    ) -> Task:
        """
        Recreate a BatchFeatureTable from an existing one
        """
        controller = request.state.app_container.batch_feature_table_controller
        task_submit: Task = await controller.recreate_batch_feature_table(
            batch_feature_table_id=batch_feature_table_id
        )
        return task_submit
