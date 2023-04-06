"""
ObservationTable API route controller
"""
from __future__ import annotations

from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.observation_table import ObservationTableCreate, ObservationTableList
from featurebyte.schema.task import Task
from featurebyte.service.observation_table import ObservationTableService


class ObservationTableController(
    BaseDocumentController[ObservationTableModel, ObservationTableService, ObservationTableList],
):
    """
    ObservationTable Controller
    """

    paginated_document_class = ObservationTableList

    def __init__(self, service: ObservationTableService, task_controller: TaskController):
        super().__init__(service)
        self.task_controller = task_controller

    async def create_observation_table(
        self,
        data: ObservationTableCreate,
    ) -> Task:
        """
        Create ObservationTable by submitting a materialization task

        Parameters
        ----------
        data: ObservationTableCreate
            ObservationTable creation payload

        Returns
        -------
        Task
        """
        payload = await self.service.get_observation_table_task_payload(data=data)
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))
