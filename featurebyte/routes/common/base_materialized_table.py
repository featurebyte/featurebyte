"""
Base class for materialized table routes
"""
from typing import TypeVar

from bson import ObjectId

from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.routes.common.base import BaseDocumentController, PaginatedDocument
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.task import Task
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.observation_table import ObservationTableService

MaterializedTableDocumentT = TypeVar(
    "MaterializedTableDocumentT",
    ObservationTableModel,
    HistoricalFeatureTableModel,
    BatchRequestTableModel,
    BatchFeatureTableModel,
)
MaterializedTableDocumentServiceT = TypeVar(
    "MaterializedTableDocumentServiceT",
    ObservationTableService,
    HistoricalFeatureTableService,
    BatchRequestTableService,
    BatchFeatureTableService,
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

    async def delete_materialized_table(self, document_id: ObjectId) -> Task:
        """
        Delete materialized table

        Parameters
        ----------
        document_id: ObjectId
            ID of materialized table to delete
        """
        payload = await self.service.get_materialized_table_delete_task_payload(
            document_id=document_id
        )
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))
