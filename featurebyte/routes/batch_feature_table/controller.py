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
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService


class BatchFeatureTableController(
    BaseDocumentController[BatchFeatureTableModel, BatchFeatureTableService, BatchFeatureTableList],
):
    """
    BatchFeatureTable Controller
    """

    paginated_document_class = BatchFeatureTableList

    def __init__(
        self,
        service: BatchFeatureTableService,
        feature_store_service: FeatureStoreService,
        feature_list_service: FeatureListService,
        batch_request_table_service: BatchRequestTableService,
        deployment_service: DeploymentService,
        entity_validation_service: EntityValidationService,
        task_controller: TaskController,
    ):
        super().__init__(service)
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

        # feature cluster group feature graph by feature store ID, only single feature store is supported
        assert feature_list.feature_clusters is not None
        feature_cluster = feature_list.feature_clusters[0]
        feature_store = await self.feature_store_service.get_document(
            document_id=feature_cluster.feature_store_id
        )
        await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
            graph=feature_cluster.graph,
            nodes=feature_cluster.nodes,
            request_column_names={col.name for col in batch_request_table.columns_info},
            feature_store=feature_store,
        )

        # prepare task payload and submit task
        payload = await self.service.get_batch_feature_table_task_payload(data=data)
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))
