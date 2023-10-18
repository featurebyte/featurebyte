"""
Catalog API routes
"""
from __future__ import annotations

from typing import List, Optional

from http import HTTPStatus

from fastapi import Query, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.catalog import CatalogModel, CatalogNameHistoryEntry
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.routes.base_router import BaseApiRouter
from featurebyte.routes.catalog.controller import CatalogController
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortDirQuery,
    VerboseQuery,
)
from featurebyte.schema.catalog import CatalogCreate, CatalogList, CatalogUpdate
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.info import CatalogInfo


class CatalogRouter(BaseApiRouter[CatalogModel, CatalogList, CatalogCreate, CatalogController]):
    """
    Catalog API router
    """

    # pylint: disable=arguments-renamed

    object_model = CatalogModel
    list_object_model = CatalogList
    create_object_schema = CatalogCreate
    controller = CatalogController

    def __init__(self) -> None:
        super().__init__("/catalog")
        self.remove_routes({"/catalog/{catalog_id}": ["DELETE"]})

        # update route
        self.router.add_api_route(
            "/{catalog_id}",
            self.update_catalog,
            methods=["PATCH"],
            response_model=CatalogModel,
            status_code=HTTPStatus.OK,
        )

        # delete route
        self.router.add_api_route(
            "/{catalog_id}",
            self.delete_catalog,
            methods=["DELETE"],
            status_code=HTTPStatus.OK,
        )

        # info route
        self.router.add_api_route(
            "/{catalog_id}/info",
            self.get_catalog_info,
            methods=["GET"],
            response_model=CatalogInfo,
        )

        # history name route
        self.router.add_api_route(
            "/history/name/{catalog_id}",
            self.list_name_history,
            methods=["GET"],
            response_model=List[CatalogNameHistoryEntry],
        )

    async def get_object(self, request: Request, catalog_id: PydanticObjectId) -> CatalogModel:
        return await super().get_object(request, catalog_id)

    async def list_audit_logs(
        self,
        request: Request,
        catalog_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[str] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        return await super().list_audit_logs(
            request, catalog_id, page, page_size, sort_by, sort_dir, search
        )

    async def update_description(
        self, request: Request, catalog_id: PydanticObjectId, data: DescriptionUpdate
    ) -> CatalogModel:
        return await super().update_description(request, catalog_id, data)

    async def create_object(
        self,
        request: Request,
        data: CatalogCreate,
    ) -> CatalogModel:
        """
        Create catalog
        """
        return await super().create_object(request, data)

    async def get_catalog_info(
        self,
        request: Request,
        catalog_id: PydanticObjectId,
        verbose: bool = VerboseQuery,
    ) -> CatalogInfo:
        """
        Retrieve catalog info
        """
        controller = self.get_controller_for_request(request)
        info = await controller.get_info(
            document_id=catalog_id,
            verbose=verbose,
        )
        return info

    async def update_catalog(
        self,
        request: Request,
        catalog_id: PydanticObjectId,
        data: CatalogUpdate,
    ) -> CatalogModel:
        """
        Update catalog
        """
        controller = self.get_controller_for_request(request)
        catalog: CatalogModel = await controller.update_catalog(
            catalog_id=catalog_id,
            data=data,
        )
        return catalog

    async def delete_catalog(
        self,
        request: Request,
        catalog_id: PydanticObjectId,
        soft_delete: bool = Query(default=True),
    ) -> None:
        """
        Delete catalog

        Parameters
        ----------
        request: Request
            Request
        catalog_id: PydanticObjectId
            Catalog ID
        soft_delete: Optional[str]
            Soft delete
        """
        controller = self.get_controller_for_request(request)
        await controller.delete_catalog(
            catalog_id=catalog_id,
            soft_delete=soft_delete,
        )

    async def list_name_history(
        self,
        request: Request,
        catalog_id: PydanticObjectId,
    ) -> List[CatalogNameHistoryEntry]:
        """
        List catalog name history
        """
        controller = self.get_controller_for_request(request)
        history_values = await controller.list_field_history(
            document_id=catalog_id,
            field="name",
        )

        return [
            CatalogNameHistoryEntry(
                created_at=record.created_at,
                name=record.value,
            )
            for record in history_values
        ]
