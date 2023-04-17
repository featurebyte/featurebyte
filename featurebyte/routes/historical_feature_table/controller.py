"""
ModelingTable API route controller
"""
from __future__ import annotations

from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.historical_feature_table import HistoricalFeatureTableCreate, HistoricalFeatureTableList
from featurebyte.schema.task import Task
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService


class ModelingTableController(
    BaseDocumentController[HistoricalFeatureTableModel, HistoricalFeatureTableService, HistoricalFeatureTableList],
):
    """
    ObservationTable Controller
    """

    paginated_document_class = HistoricalFeatureTableList

    def __init__(self, service: HistoricalFeatureTableService, task_controller: TaskController):
        super().__init__(service)
        self.task_controller = task_controller

    async def create_modeling_table(
        self,
        data: HistoricalFeatureTableCreate,
    ) -> Task:
        """
        Create ModelingTable by submitting an async historical feature request task

        Parameters
        ----------
        data: HistoricalFeatureTableCreate
            HistoricalFeatureTable creation payload

        Returns
        -------
        Task
        """
        payload = await self.service.get_modeling_table_task_payload(data=data)
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))
