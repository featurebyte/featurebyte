"""
BatchRequestTable API route controller
"""

from __future__ import annotations

from typing import Any, List, Tuple

from bson import ObjectId

from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.routes.common.base_materialized_table import BaseMaterializedTableController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.batch_request_table import BatchRequestTableCreate, BatchRequestTableList
from featurebyte.schema.info import BatchRequestTableInfo
from featurebyte.schema.task import Task
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService


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
        batch_request_table_service: BatchRequestTableService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        batch_feature_table_service: BatchFeatureTableService,
        task_controller: TaskController,
        feature_store_service: FeatureStoreService,
    ):
        super().__init__(
            service=batch_request_table_service,
            feature_store_warehouse_service=feature_store_warehouse_service,
        )
        self.batch_feature_table_service = batch_feature_table_service
        self.task_controller = task_controller
        self.feature_store_service = feature_store_service

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

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return [
            (self.batch_feature_table_service, {"batch_request_table_id": document_id}),
        ]

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
        _ = verbose
        batch_request_table = await self.service.get_document(document_id=document_id)
        feature_store = await self.feature_store_service.get_document(
            document_id=batch_request_table.location.feature_store_id
        )
        return BatchRequestTableInfo(
            name=batch_request_table.name,
            type=batch_request_table.request_input.type,
            feature_store_name=feature_store.name,
            table_details=batch_request_table.location.table_details,
            created_at=batch_request_table.created_at,
            updated_at=batch_request_table.updated_at,
            description=batch_request_table.description,
        )
