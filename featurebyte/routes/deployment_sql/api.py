"""
DeploymentSql API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Optional

from bson import ObjectId
from fastapi import Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.deployment_sql import DeploymentSqlModel
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
from featurebyte.routes.deployment_sql.controller import DeploymentSqlController
from featurebyte.schema.common.base import DeleteResponse
from featurebyte.schema.deployment_sql import (
    DeploymentSqlCreate,
    DeploymentSqlList,
    DeploymentSqlModelResponse,
)
from featurebyte.schema.task import Task


class DeploymentSqlRouter(
    BaseApiRouter[
        DeploymentSqlModel, DeploymentSqlList, DeploymentSqlCreate, DeploymentSqlController
    ]
):
    """
    DeploymentSql API router
    """

    object_model = DeploymentSqlModel
    list_object_model = DeploymentSqlList
    create_object_schema = DeploymentSqlCreate
    controller = DeploymentSqlController

    def __init__(self) -> None:
        super().__init__("/deployment_sql")

        self.remove_routes({"/deployment_sql": ["POST"]})

        # Async creation route that returns a Task
        self.router.add_api_route(
            "",
            self.create_deployment_sql_async,
            methods=["POST"],
            response_model=Task,
            status_code=HTTPStatus.CREATED,
        )

        # # Get by deployment ID
        # self.router.add_api_route(
        #     "/deployment/{deployment_id}",
        #     self.get_deployment_sql_by_deployment_id,
        #     methods=["GET"],
        #     response_model=DeploymentSqlModelResponse,
        # )

    async def get_object(
        self, request: Request, deployment_sql_id: PydanticObjectId
    ) -> DeploymentSqlModel:
        return await super().get_object(request, deployment_sql_id)

    async def list_audit_logs(
        self,
        request: Request,
        deployment_sql_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        return await super().list_audit_logs(
            request, deployment_sql_id, page, page_size, sort_by, sort_dir, search
        )

    async def delete_object(
        self, request: Request, deployment_sql_id: PydanticObjectId
    ) -> DeleteResponse:
        return await super().delete_object(request, deployment_sql_id)

    async def create_deployment_sql_async(
        self,
        request: Request,
        data: DeploymentSqlCreate,
    ) -> Task:
        """
        Create DeploymentSql by submitting an async generation task
        """
        controller = self.get_controller_for_request(request)
        task = await controller.generate_deployment_sql(str(data.deployment_id))
        return task

    async def get_deployment_sql_by_deployment_id(
        self, request: Request, deployment_id: PydanticObjectId
    ) -> DeploymentSqlModelResponse:
        """
        Get DeploymentSql by deployment ID
        """
        controller = self.get_controller_for_request(request)
        # Query by deployment_id field
        query_filter = {"deployment_id": ObjectId(deployment_id)}
        documents = await controller.service.list_documents_as_dict(
            query_filter=query_filter, page=0, page_size=1
        )
        if not documents["data"]:
            from featurebyte.exception import DocumentNotFoundError

            raise DocumentNotFoundError("DeploymentSql not found for deployment")

        document = documents["data"][0]
        return DeploymentSqlModelResponse(**document)
