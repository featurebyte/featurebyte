"""
Base router
"""

from http import HTTPStatus
from typing import Dict, Generic, List, Optional, Type, TypeVar, Union, cast

from bson import ObjectId
from fastapi import APIRouter, Request
from fastapi.routing import APIRoute
from starlette.routing import BaseRoute

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.persistent.base import SortDir
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
)
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate

ObjectModelT = TypeVar("ObjectModelT")
ListObjectModelT = TypeVar("ListObjectModelT")
CreateObjectSchemaT = TypeVar("CreateObjectSchemaT")
ControllerT = TypeVar("ControllerT", bound=BaseDocumentController)  # type: ignore[type-arg]


class BaseRouter:
    """
    BaseRouter class that just encapsulates the APIRouter object.
    """

    def __init__(self, router: APIRouter) -> None:
        self.router = router

    def add_router(self, router: APIRouter) -> None:
        """
        add_router will add all the routes from another router, into the current router.

        Parameters
        ----------
        router: APIRouter
            The router to add to the current router.
        """
        self.router.routes.extend(router.routes)

    def remove_routes(self, unwanted_path_to_method: Dict[str, List[str]]) -> None:
        """
        Remove routes from router

        Parameters
        ----------
        unwanted_path_to_method: Dict[str, List[str]]
            Dictionary of paths to methods to be removed from the router
        """
        routes_to_keep: List[BaseRoute] = []
        for route in self.router.routes:
            route = cast(APIRoute, route)
            if set(route.methods).issubset(unwanted_path_to_method.get(route.path, [])):
                continue
            routes_to_keep.append(route)
        self.router.routes = routes_to_keep


class BaseApiRouter(
    BaseRouter, Generic[ObjectModelT, ListObjectModelT, CreateObjectSchemaT, ControllerT]
):
    """
    Base API router.

    This class contains basic CRUD operations for a given model.
    """

    object_model: Type[ObjectModelT]
    list_object_model: Type[ListObjectModelT]
    create_object_schema: Type[CreateObjectSchemaT]
    controller: Union[Type[ControllerT], str]

    def __init__(self, prefix: str, skip_id_prefix: bool = False):
        super().__init__(router=APIRouter(prefix=prefix))
        base_name = prefix.lstrip("/")
        api_id = f"{{{base_name}_id}}" if not skip_id_prefix else "{object_id}"

        # Get object
        self.router.add_api_route(
            f"/{api_id}",
            self.get_object,
            methods=["GET"],
            response_model=self.object_model,
            status_code=HTTPStatus.OK,
        )

        # Create an object
        self.router.add_api_route(
            "",
            self.create_object,
            methods=["POST"],
            response_model=self.object_model,
            status_code=HTTPStatus.CREATED,
        )

        # List objects
        self.router.add_api_route(
            "",
            self.list_objects,
            methods=["GET"],
            response_model=self.list_object_model,
            status_code=HTTPStatus.OK,
        )

        # Delete objects
        self.router.add_api_route(
            f"/{api_id}",
            self.delete_object,
            methods=["DELETE"],
            status_code=HTTPStatus.OK,
        )

        # List audit logs for object
        self.router.add_api_route(
            f"/audit/{api_id}",
            self.list_audit_logs,
            methods=["GET"],
            response_model=AuditDocumentList,
        )

        # Update description for object
        self.router.add_api_route(
            f"/{api_id}/description",
            self.update_description,
            methods=["PATCH"],
            response_model=self.object_model,
        )

    @classmethod
    def get_controller_for_request(cls, request: Request) -> ControllerT:
        """
        Get controller for request
        """
        app_container: LazyAppContainer = request.state.app_container
        return cast(ControllerT, app_container.get(cls.controller))

    async def get_object(self, request: Request, object_id: PydanticObjectId) -> ObjectModelT:
        """
        Get table
        """
        controller = self.get_controller_for_request(request)
        object_model: ObjectModelT = await controller.get(document_id=ObjectId(object_id))
        return object_model

    async def create_object(self, request: Request, data: CreateObjectSchemaT) -> ObjectModelT:
        """
        Create object
        """
        controller = self.get_controller_for_request(request)
        result: ObjectModelT = await controller.service.create_document(data=data)
        return result

    async def list_objects(
        self,
        request: Request,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = SortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
        name: Optional[str] = NameQuery,
    ) -> ListObjectModelT:
        """
        List objects
        """
        controller = self.get_controller_for_request(request)
        return cast(
            ListObjectModelT,
            await controller.list(
                page=page,
                page_size=page_size,
                sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
                search=search,
                name=name,
            ),
        )

    async def delete_object(self, request: Request, object_id: PydanticObjectId) -> DeleteResponse:
        """
        Delete object
        """
        controller = self.get_controller_for_request(request)
        await controller.delete(document_id=ObjectId(object_id))
        return DeleteResponse()

    async def list_audit_logs(
        self,
        request: Request,
        object_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        """
        List audit logs
        """
        controller = self.get_controller_for_request(request)
        audit_doc_list: AuditDocumentList = await controller.list_audit(
            document_id=ObjectId(object_id),
            page=page,
            page_size=page_size,
            sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
            search=search,
        )
        return audit_doc_list

    async def update_description(
        self, request: Request, object_id: PydanticObjectId, data: DescriptionUpdate
    ) -> ObjectModelT:
        """
        Update description
        """
        controller = self.get_controller_for_request(request)
        object_model: ObjectModelT = await controller.update_description(
            document_id=ObjectId(object_id),
            description=data.description,
        )
        return object_model
