"""
StaticSourceTable API route controller
"""
from __future__ import annotations

from bson import ObjectId

from featurebyte.models.static_source_table import StaticSourceTableModel
from featurebyte.routes.common.base_materialized_table import BaseMaterializedTableController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.info import StaticSourceTableInfo
from featurebyte.schema.static_source_table import StaticSourceTableCreate, StaticSourceTableList
from featurebyte.schema.task import Task
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.preview import PreviewService
from featurebyte.service.static_source_table import StaticSourceTableService
from featurebyte.service.table import TableService
from featurebyte.service.validator.materialized_table_delete import check_delete_static_source_table


class StaticSourceTableController(
    BaseMaterializedTableController[
        StaticSourceTableModel, StaticSourceTableService, StaticSourceTableList
    ],
):
    """
    StaticSourceTable Controller
    """

    paginated_document_class = StaticSourceTableList

    def __init__(
        self,
        static_source_table_service: StaticSourceTableService,
        preview_service: PreviewService,
        table_service: TableService,
        task_controller: TaskController,
        feature_store_service: FeatureStoreService,
    ):
        super().__init__(service=static_source_table_service, preview_service=preview_service)
        self.table_service = table_service
        self.task_controller = task_controller
        self.feature_store_service = feature_store_service

    async def create_static_source_table(
        self,
        data: StaticSourceTableCreate,
    ) -> Task:
        """
        Create StaticSourceTable by submitting a materialization task

        Parameters
        ----------
        data: StaticSourceTableCreate
            StaticSourceTable creation payload

        Returns
        -------
        Task
        """
        payload = await self.service.get_static_source_table_task_payload(data=data)
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def _verify_delete_operation(self, document_id: ObjectId) -> None:
        await check_delete_static_source_table(
            static_source_table_service=self.service,
            table_service=self.table_service,
            document_id=document_id,
        )

    async def get_info(self, document_id: ObjectId, verbose: bool) -> StaticSourceTableInfo:
        """
        Get StaticSourceTable info given document_id

        Parameters
        ----------
        document_id: ObjectId
            StaticSourceTable document_id
        verbose: bool
            Whether to return verbose info

        Returns
        -------
        StaticSourceTableInfo
        """
        _ = verbose
        static_source_table = await self.service.get_document(document_id=document_id)
        feature_store = await self.feature_store_service.get_document(
            document_id=static_source_table.location.feature_store_id
        )
        return StaticSourceTableInfo(
            name=static_source_table.name,
            type=static_source_table.request_input.type,
            feature_store_name=feature_store.name,
            table_details=static_source_table.location.table_details,
            created_at=static_source_table.created_at,
            updated_at=static_source_table.updated_at,
            description=static_source_table.description,
        )
