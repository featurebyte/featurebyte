"""
BatchRequestTable API route controller
"""
from __future__ import annotations

from bson import ObjectId

from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.routes.common.base_materialized_table import BaseMaterializedTableController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.batch_request_table import (
    BatchRequestTableCreate,
    BatchRequestTableInfo,
    BatchRequestTableList,
)
from featurebyte.schema.task import Task
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.info import InfoService


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
        info_service: InfoService,
        task_controller: TaskController,
    ):
        super().__init__(service)
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
