"""
BatchRequestTable API route controller
"""
from __future__ import annotations

from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.batch_request_table import BatchRequestTableCreate, BatchRequestTableList
from featurebyte.schema.task import Task
from featurebyte.service.batch_request_table import BatchRequestTableService


class BatchRequestTableController(
    BaseDocumentController[BatchRequestTableModel, BatchRequestTableService, BatchRequestTableList],
):
    """
    BatchRequestTable Controller
    """

    paginated_document_class = BatchRequestTableList

    def __init__(self, service: BatchRequestTableService, task_controller: TaskController):
        super().__init__(service)
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
