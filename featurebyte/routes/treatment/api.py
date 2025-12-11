"""
Treatment API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Optional

from fastapi import Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.treatment import TreatmentModel
from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseApiRouter
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
    VerboseQuery,
)
from featurebyte.routes.treatment.controller import TreatmentController
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.info import TreatmentInfo
from featurebyte.schema.task import Task
from featurebyte.schema.treatment import (
    TreatmentCreate,
    TreatmentLabelsValidate,
    TreatmentList,
    TreatmentUpdate,
)


class TreatmentRouter(
    BaseApiRouter[TreatmentModel, TreatmentList, TreatmentCreate, TreatmentController]
):
    """
    Treatment router
    """

    object_model = TreatmentModel
    list_object_model = TreatmentList
    create_object_schema = TreatmentCreate
    controller = TreatmentController

    def __init__(self) -> None:
        super().__init__("/treatment")

        self.router.add_api_route(
            "/{treatment_id}/treatment_labels",
            self.validate_treatment_labels,
            methods=["PATCH"],
            response_model=Task,
            status_code=HTTPStatus.ACCEPTED,
        )
        self.router.add_api_route(
            "/{treatment_id}",
            self.update_treatment,
            methods=["PATCH"],
            response_model=TreatmentModel,
        )
        self.router.add_api_route(
            "/{treatment_id}/info",
            self.get_treatment_info,
            methods=["GET"],
            response_model=TreatmentInfo,
        )

    async def create_object(self, request: Request, data: TreatmentCreate) -> TreatmentModel:
        """
        Create treatment
        """
        controller = self.get_controller_for_request(request)
        treatment: TreatmentModel = await controller.create_treatment(data=data)
        return treatment

    async def get_object(self, request: Request, treatment_id: PydanticObjectId) -> TreatmentModel:
        """
        Retrieve Treatment
        """
        controller = self.get_controller_for_request(request)
        treatment: TreatmentModel = await controller.get(
            document_id=treatment_id,
            exception_detail=(
                f'Treatment (id: "{treatment_id}") not found. Please save the Treatment object first.'
            ),
        )
        return treatment

    async def list_objects(
        self,
        request: Request,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = SortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
        name: Optional[str] = NameQuery,
    ) -> TreatmentList:
        """
        List Treatment
        """
        controller = self.get_controller_for_request(request)
        treatment_list: TreatmentList = await controller.list(
            page=page,
            page_size=page_size,
            sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
            search=search,
            name=name,
        )
        return treatment_list

    async def delete_object(
        self, request: Request, treatment_id: PydanticObjectId
    ) -> DeleteResponse:
        """
        Delete Treatment
        """
        return await super().delete_object(request, treatment_id)

    async def list_audit_logs(
        self,
        request: Request,
        treatment_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        """
        List Treatment audit logs
        """
        return await super().list_audit_logs(
            request, treatment_id, page, page_size, sort_by, sort_dir, search
        )

    async def update_description(
        self, request: Request, treatment_id: PydanticObjectId, data: DescriptionUpdate
    ) -> TreatmentModel:
        """
        Update treatment description
        """
        return await super().update_description(request, treatment_id, data)

    async def validate_treatment_labels(
        self,
        request: Request,
        treatment_id: PydanticObjectId,
        data: TreatmentLabelsValidate,
    ) -> Task:
        """
        Validate Treatment Labels
        """
        controller = self.get_controller_for_request(request)
        task: Task = await controller.create_treatment_labels_validate_task(
            treatment_id=treatment_id, data=data
        )
        return task

    async def update_treatment(
        self, request: Request, treatment_id: PydanticObjectId, data: TreatmentUpdate
    ) -> TreatmentModel:
        """
        Update Treatment
        """
        controller = self.get_controller_for_request(request)
        treatment: TreatmentModel = await controller.update_treatment(treatment_id, data)
        return treatment

    async def get_treatment_info(
        self,
        request: Request,
        treatment_id: PydanticObjectId,
        verbose: bool = VerboseQuery,
    ) -> TreatmentInfo:
        """
        Retrieve Treatment info
        """
        controller = self.get_controller_for_request(request)
        info = await controller.get_info(
            document_id=treatment_id,
            verbose=verbose,
        )
        return info
