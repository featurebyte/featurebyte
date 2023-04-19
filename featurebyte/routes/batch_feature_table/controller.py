"""
BatchFeatureTable API route controller
"""
from __future__ import annotations

from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.batch_feature_table import BatchFeatureTableCreate, BatchFeatureTableList
from featurebyte.schema.task import Task
from featurebyte.service.batch_feature_table import BatchFeatureTableService


class BatchFeatureTableController(
    BaseDocumentController[BatchFeatureTableModel, BatchFeatureTableService, BatchFeatureTableList],
):
    """
    BatchFeatureTable Controller
    """

    paginated_document_class = BatchFeatureTableList

    def __init__(self, service: BatchFeatureTableService, task_controller: TaskController):
        super().__init__(service)
        self.task_controller = task_controller

    async def create_batch_feature_table(
        self,
        data: BatchFeatureTableCreate,
    ) -> Task:
        """
        Create BatchFeatureTable by submitting an async prediction request task

        Parameters
        ----------
        data: BatchFeatureTableCreate
            BatchFeatureTable creation request parameters

        Returns
        -------
        Task
        """
        payload = await self.service.get_batch_feature_table_task_payload(data=data)
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))
