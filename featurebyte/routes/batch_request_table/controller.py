"""
BatchRequestTable API route controller
"""
from __future__ import annotations

from bson import ObjectId

from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.routes.common.base_materialized_table import BaseMaterializedTableController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.batch_request_table import BatchRequestTableCreate, BatchRequestTableList
from featurebyte.schema.info import BatchRequestTableInfo
from featurebyte.schema.task import Task
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.info import InfoService
from featurebyte.service.preview import PreviewService
from featurebyte.service.validator.materialized_table_delete import check_delete_batch_request_table


class BatchRequestTableController(
    BaseMaterializedTableController[
        BatchRequestTableModel, BatchRequestTableService, BatchRequestTableList
    ],
):
    """
    BatchRequestTable Controller
    """

    paginated_document_class = BatchRequestTableList

    def __init__(
        self,
        service: BatchRequestTableService,
        preview_service: PreviewService,
        batch_feature_table_service: BatchFeatureTableService,
        info_service: InfoService,
        task_controller: TaskController,
    ):
        super().__init__(service=service, preview_service=preview_service)
        self.batch_feature_table_service = batch_feature_table_service
        self.info_service = info_service
        self.task_controller = task_controller

    async def create_batch_request_table(
        self,
        data: BatchRequestTableCreate,
    ) -> Task:
        """
        Create BatchRequestTable by submitting a materialization task

        Parameters
        ----------
        data: BatchRequestTableCreate
            BatchRequestTable creation payload

        Returns
        -------
        Task
        """
        payload = await self.service.get_batch_request_table_task_payload(data=data)
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def _verify_delete_operation(self, document_id: ObjectId) -> None:
        await check_delete_batch_request_table(
            batch_request_table_service=self.service,
            batch_feature_table_service=self.batch_feature_table_service,
            document_id=document_id,
        )

    async def get_info(self, document_id: ObjectId, verbose: bool) -> BatchRequestTableInfo:
        """
        Get BatchRequestTable info given document_id

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        BatchRequestTableInfo
        """
        info_document = await self.info_service.get_batch_request_table_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
