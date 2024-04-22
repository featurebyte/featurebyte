"""
BatchFeatureTable API route controller
"""

from __future__ import annotations

from bson import ObjectId

from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.routes.common.base_materialized_table import BaseMaterializedTableController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.batch_feature_table import BatchFeatureTableCreate, BatchFeatureTableList
from featurebyte.schema.info import BatchFeatureTableInfo
from featurebyte.schema.task import Task
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService


class BatchFeatureTableController(
    BaseMaterializedTableController[
        BatchFeatureTableModel, BatchFeatureTableService, BatchFeatureTableList
    ],
):
    """
    BatchFeatureTable Controller
    """

    paginated_document_class = BatchFeatureTableList

    def __init__(
        self,
        batch_feature_table_service: BatchFeatureTableService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        feature_store_service: FeatureStoreService,
        feature_list_service: FeatureListService,
        batch_request_table_service: BatchRequestTableService,
        deployment_service: DeploymentService,
        entity_validation_service: EntityValidationService,
        task_controller: TaskController,
    ):
        super().__init__(
            service=batch_feature_table_service,
            feature_store_warehouse_service=feature_store_warehouse_service,
        )
        self.feature_store_service = feature_store_service
        self.feature_list_service = feature_list_service
        self.batch_request_table_service = batch_request_table_service
        self.deployment_service = deployment_service
        self.entity_validation_service = entity_validation_service
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
        # Validate the batch_request_table_id & deployment_id
        batch_request_table = await self.batch_request_table_service.get_document(
            document_id=data.batch_request_table_id
        )
        deployment = await self.deployment_service.get_document(document_id=data.deployment_id)
        feature_list = await self.feature_list_service.get_document(
            document_id=deployment.feature_list_id
        )
        feature_store = await self.feature_store_service.get_document(
            document_id=data.feature_store_id
        )
        await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
            feature_list_model=feature_list,
            request_column_names={col.name for col in batch_request_table.columns_info},
            feature_store=feature_store,
        )

        # prepare task payload and submit task
        payload = await self.service.get_batch_feature_table_task_payload(data=data)
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def get_info(self, document_id: ObjectId, verbose: bool) -> BatchFeatureTableInfo:
        """
        Get BatchFeatureTable info

        Parameters
        ----------
        document_id: ObjectId
            BatchFeatureTable ID
        verbose: bool
            Whether to return verbose info

        Returns
        -------
        BatchFeatureTableInfo
        """
        _ = verbose
        batch_feature_table = await self.service.get_document(document_id=document_id)
        batch_request_table = await self.batch_request_table_service.get_document(
            document_id=batch_feature_table.batch_request_table_id
        )
        deployment = await self.deployment_service.get_document(
            document_id=batch_feature_table.deployment_id
        )
        return BatchFeatureTableInfo(
            name=batch_feature_table.name,
            deployment_name=deployment.name,
            batch_request_table_name=batch_request_table.name,
            table_details=batch_feature_table.location.table_details,
            created_at=batch_feature_table.created_at,
            updated_at=batch_feature_table.updated_at,
            description=batch_feature_table.description,
        )
