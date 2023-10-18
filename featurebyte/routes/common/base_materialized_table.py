"""
Base class for materialized table routes
"""
from typing import Any, TypeVar

from bson import ObjectId
from starlette.responses import StreamingResponse

from featurebyte.exception import DocumentDeletionError
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.static_source_table import StaticSourceTableModel
from featurebyte.models.target_table import TargetTableModel
from featurebyte.routes.common.base import BaseDocumentController, PaginatedDocument
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.task import Task
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.preview import PreviewService
from featurebyte.service.static_source_table import StaticSourceTableService
from featurebyte.service.target_table import TargetTableService

MaterializedTableDocumentT = TypeVar(
    "MaterializedTableDocumentT",
    ObservationTableModel,
    HistoricalFeatureTableModel,
    BatchRequestTableModel,
    BatchFeatureTableModel,
    StaticSourceTableModel,
    TargetTableModel,
)
MaterializedTableDocumentServiceT = TypeVar(
    "MaterializedTableDocumentServiceT",
    ObservationTableService,
    HistoricalFeatureTableService,
    BatchRequestTableService,
    BatchFeatureTableService,
    StaticSourceTableService,
    TargetTableService,
)


class BaseMaterializedTableController(
    BaseDocumentController[
        MaterializedTableDocumentT, MaterializedTableDocumentServiceT, PaginatedDocument
    ]
):
    """
    Base class for materialized table routes
    """

    task_controller: TaskController

    def __init__(self, service: Any, preview_service: PreviewService) -> None:
        super().__init__(service)
        self.preview_service = preview_service

    async def delete_materialized_table(self, document_id: ObjectId) -> Task:
        """
        Delete materialized table

        Parameters
        ----------
        document_id: ObjectId
            ID of materialized table to delete
        """
        # check if document exists
        _ = await self.service.get_document(document_id=document_id)

        # check if document is used by any other documents
        await self.verify_operation_by_checking_reference(
            document_id=document_id, exception_class=DocumentDeletionError
        )

        # create task payload & submit task
        payload = await self.service.get_materialized_table_delete_task_payload(
            document_id=document_id
        )
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def download_materialized_table(
        self,
        document_id: ObjectId,
    ) -> StreamingResponse:
        """
        Download materialized table as pyarrow table

        Parameters
        ----------
        document_id: ObjectId
            ID of materialized table to download

        Returns
        -------
        StreamingResponse
            StreamingResponse object
        """
        table = await self.service.get_document(document_id=document_id)
        bytestream = await self.preview_service.download_table(
            location=table.location,
        )
        assert bytestream is not None

        return StreamingResponse(
            bytestream,
            media_type="application/octet-stream",
        )
