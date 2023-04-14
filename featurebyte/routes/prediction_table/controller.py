"""
PredictionTable API route controller
"""
from __future__ import annotations

from featurebyte.models.prediction_table import PredictionTableModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.prediction_table import PredictionTableCreate, PredictionTableList
from featurebyte.schema.task import Task
from featurebyte.service.prediction_table import PredictionTableService


class PredictionTableController(
    BaseDocumentController[PredictionTableModel, PredictionTableService, PredictionTableList],
):
    """
    PredictionTable Controller
    """

    paginated_document_class = PredictionTableList

    def __init__(self, service: PredictionTableService, task_controller: TaskController):
        super().__init__(service)
        self.task_controller = task_controller

    async def create_prediction_table(
        self,
        data: PredictionTableCreate,
    ) -> Task:
        """
        Create PredictionTable by submitting an async prediction request task

        Returns
        -------
        Task
        """
        payload = await self.service.get_prediction_table_task_payload(data=data)
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))
