"""
Semantic API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Optional

from fastapi import Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.relationship import Parent
from featurebyte.models.semantic import SemanticModel
from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseApiRouter
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortDirQuery,
)
from featurebyte.routes.semantic.controller import SemanticController
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.semantic import SemanticCreate, SemanticList


class SemanticRouter(
    BaseApiRouter[SemanticModel, SemanticList, SemanticCreate, SemanticController]
):
    """
    Semantic API router
    """

    object_model = SemanticModel
    list_object_model = SemanticList
    create_object_schema = SemanticCreate
    controller = SemanticController

    def __init__(self) -> None:
        super().__init__("/semantic")
        self.router.add_api_route(
            "/{semantic_id}/parent",
            self.add_parent,
            methods=["POST"],
            response_model=SemanticModel,
            status_code=HTTPStatus.CREATED,
        )
        self.router.add_api_route(
            "/{semantic_id}/parent/{parent_semantic_id}",
            self.remove_parent,
            methods=["DELETE"],
            response_model=SemanticModel,
        )

    @staticmethod
    async def add_parent(
        request: Request, semantic_id: PydanticObjectId, data: Parent
    ) -> SemanticModel:
        """
        Create semantic relationship
        """
        controller = request.state.app_container.semantic_controller
        semantic: SemanticModel = await controller.create_relationship(
            data=data, child_id=semantic_id
        )
        return semantic

    @staticmethod
    async def remove_parent(
        request: Request, semantic_id: PydanticObjectId, parent_semantic_id: PydanticObjectId
    ) -> SemanticModel:
        """
        Remove semantic relationship
        """
        controller = request.state.app_container.semantic_controller
        semantic: SemanticModel = await controller.remove_relationship(
            parent_id=parent_semantic_id, child_id=semantic_id
        )
        return semantic

    async def create_object(self, request: Request, data: SemanticCreate) -> SemanticModel:
        return await super().create_object(request=request, data=data)

    async def get_object(self, request: Request, semantic_id: PydanticObjectId) -> SemanticModel:
        return await super().get_object(request=request, object_id=semantic_id)

    async def list_audit_logs(
        self,
        request: Request,
        semantic_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        return await super().list_audit_logs(
            request,
            semantic_id,
            page,
            page_size,
            sort_by,
            sort_dir,
            search,
        )

    async def update_description(
        self, request: Request, semantic_id: PydanticObjectId, data: DescriptionUpdate
    ) -> SemanticModel:
        return await super().update_description(request, semantic_id, data)

    async def delete_object(
        self, request: Request, semantic_id: PydanticObjectId
    ) -> DeleteResponse:
        return await super().delete_object(request, semantic_id)
