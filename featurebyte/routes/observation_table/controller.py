"""
ObservationTable API route controller
"""
from __future__ import annotations

from bson import ObjectId

from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.routes.common.base_materialized_table import BaseMaterializedTableController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.info import ObservationTableInfo
from featurebyte.schema.observation_table import ObservationTableCreate, ObservationTableList
from featurebyte.schema.task import Task
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.preview import PreviewService
from featurebyte.service.validator.materialized_table_delete import ObservationTableDeleteValidator


class ObservationTableController(
    BaseMaterializedTableController[
        ObservationTableModel, ObservationTableService, ObservationTableList
    ],
):
    """
    ObservationTable Controller
    """

    paginated_document_class = ObservationTableList

    def __init__(
        self,
        observation_table_service: ObservationTableService,
        preview_service: PreviewService,
        task_controller: TaskController,
        feature_store_service: FeatureStoreService,
        observation_table_delete_validator: ObservationTableDeleteValidator,
    ):
        super().__init__(service=observation_table_service, preview_service=preview_service)
        self.task_controller = task_controller
        self.feature_store_service = feature_store_service
        self.observation_table_delete_validator = observation_table_delete_validator

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

    async def _verify_delete_operation(self, document_id: ObjectId) -> None:
        await self.observation_table_delete_validator.check_delete_observation_table(
            observation_table_id=document_id,
        )

    async def get_info(self, document_id: ObjectId, verbose: bool) -> ObservationTableInfo:
        """
        Get ObservationTable info given document_id

        Parameters
        ----------
        document_id: ObjectId
            ObservationTable document_id
        verbose: bool
            Whether to return verbose info

        Returns
        -------
        ObservationTableInfo
        """
        _ = verbose
        observation_table = await self.service.get_document(document_id=document_id)
        feature_store = await self.feature_store_service.get_document(
            document_id=observation_table.location.feature_store_id
        )
        return ObservationTableInfo(
            name=observation_table.name,
            type=observation_table.request_input.type,
            feature_store_name=feature_store.name,
            table_details=observation_table.location.table_details,
            created_at=observation_table.created_at,
            updated_at=observation_table.updated_at,
            description=observation_table.description,
        )
