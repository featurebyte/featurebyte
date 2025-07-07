"""
DevelopmentDataset API routes
"""

from __future__ import annotations

from typing import Optional, cast

from bson import ObjectId
from fastapi import Request

from featurebyte.models.base import PyObjectId
from featurebyte.models.development_dataset import DevelopmentDatasetModel
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
from featurebyte.routes.development_dataset.controller import DevelopmentDatasetController
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.development_dataset import (
    DevelopmentDatasetCreate,
    DevelopmentDatasetInfo,
    DevelopmentDatasetList,
    DevelopmentDatasetUpdate,
)


class DevelopmentDatasetRouter(
    BaseApiRouter[
        DevelopmentDatasetModel,
        DevelopmentDatasetList,
        DevelopmentDatasetCreate,
        DevelopmentDatasetController,
    ]
):
    """
    Feature Store API router
    """

    object_model = DevelopmentDatasetModel
    list_object_model = DevelopmentDatasetList
    create_object_schema = DevelopmentDatasetCreate
    controller = DevelopmentDatasetController

    def __init__(self) -> None:
        super().__init__("/development_dataset")

        self.router.add_api_route(
            "/{development_dataset_id}",
            self.update_development_dataset,
            methods=["PATCH"],
            response_model=self.object_model,
        )
        self.router.add_api_route(
            "/{development_dataset_id}/info",
            self.get_development_dataset_info,
            methods=["GET"],
            response_model=DevelopmentDatasetInfo,
        )

    async def create_object(
        self,
        request: Request,
        data: DevelopmentDatasetCreate,
    ) -> DevelopmentDatasetModel:
        """
        Create Feature Store
        """
        controller = self.get_controller_for_request(request)
        result: DevelopmentDatasetModel = await controller.create_development_dataset(data=data)
        return result

    async def get_object(
        self, request: Request, development_dataset_id: PyObjectId
    ) -> DevelopmentDatasetModel:
        return await super().get_object(request, development_dataset_id)

    async def list_audit_logs(
        self,
        request: Request,
        development_dataset_id: PyObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        return await super().list_audit_logs(
            request,
            development_dataset_id,
            page,
            page_size,
            sort_by,
            sort_dir,
            search,
        )

    @staticmethod
    async def update_development_dataset(
        request: Request, development_dataset_id: PyObjectId, data: DevelopmentDatasetUpdate
    ) -> DevelopmentDatasetModel:
        """
        Update online store
        """
        controller = request.state.app_container.development_dataset_controller
        result: DevelopmentDatasetModel = await controller.update_development_dataset(
            development_dataset_id, data
        )
        return result

    async def update_description(
        self, request: Request, development_dataset_id: PyObjectId, data: DescriptionUpdate
    ) -> DevelopmentDatasetModel:
        return await super().update_description(request, development_dataset_id, data)

    @staticmethod
    async def get_development_dataset_info(
        request: Request,
        development_dataset_id: PyObjectId,
        verbose: bool = VerboseQuery,
    ) -> DevelopmentDatasetInfo:
        """
        Retrieve DevelopmentDataset info
        """
        controller = request.state.app_container.development_dataset_controller
        info = await controller.get_info(
            document_id=ObjectId(development_dataset_id),
            verbose=verbose,
        )
        return cast(DevelopmentDatasetInfo, info)

    async def delete_object(
        self, request: Request, development_dataset_id: PyObjectId
    ) -> DeleteResponse:
        controller = self.get_controller_for_request(request)
        await controller.delete(document_id=ObjectId(development_dataset_id))
        return DeleteResponse()
