"""
ModelingTable API route controller
"""
from __future__ import annotations

from featurebyte.models.modeling_table import ModelingTableModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.modeling_table import ModelingTableCreate, ModelingTableList
from featurebyte.schema.task import Task
from featurebyte.service.modeling_table import ModelingTableService


class ModelingTableController(
    BaseDocumentController[ModelingTableModel, ModelingTableService, ModelingTableList],
):
    """
    ObservationTable Controller
    """

    paginated_document_class = ModelingTableList

    def __init__(self, service: ModelingTableService, task_controller: TaskController):
        super().__init__(service)
        self.task_controller = task_controller

    async def create_modeling_table(
        self,
        data: ModelingTableCreate,
    ) -> Task:
        """
        Create ModelingTable by submitting an async historical feature request task

        Parameters
        ----------
        data: ModelingTableCreate
            ModelingTable creation payload

        Returns
        -------
        Task
        """
        payload = await self.service.get_modeling_table_task_payload(data=data)
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))
