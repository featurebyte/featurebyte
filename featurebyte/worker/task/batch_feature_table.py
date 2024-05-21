"""
BatchFeatureTable creation task
"""

from __future__ import annotations

from typing import Any

from featurebyte.logging import get_logger
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.schema.worker.task.batch_feature_table import BatchFeatureTableTaskPayload
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.online_serving import OnlineServingService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class BatchFeatureTableTask(DataWarehouseMixin, BaseTask[BatchFeatureTableTaskPayload]):
    """
    BatchFeatureTableTask creates a batch feature table by computing online predictions
    """

    payload_class = BatchFeatureTableTaskPayload

    def __init__(  # pylint: disable=too-many-arguments
        self,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        batch_request_table_service: BatchRequestTableService,
        batch_feature_table_service: BatchFeatureTableService,
        deployment_service: DeploymentService,
        feature_list_service: FeatureListService,
        online_serving_service: OnlineServingService,
    ):
        super().__init__()
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.batch_request_table_service = batch_request_table_service
        self.batch_feature_table_service = batch_feature_table_service
        self.deployment_service = deployment_service
        self.feature_list_service = feature_list_service
        self.online_serving_service = online_serving_service

    async def get_task_description(self, payload: BatchFeatureTableTaskPayload) -> str:
        return f'Save batch feature table "{payload.name}"'

    async def execute(self, payload: BatchFeatureTableTaskPayload) -> Any:
        feature_store = await self.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)

        batch_request_table_model = await self.batch_request_table_service.get_document(
            payload.batch_request_table_id
        )

        location = await self.batch_feature_table_service.generate_materialized_table_location(
            payload.feature_store_id
        )

        # retrieve feature list from deployment
        deployment: DeploymentModel = await self.deployment_service.get_document(
            document_id=payload.deployment_id
        )
        feature_list: FeatureListModel = await self.feature_list_service.get_document(
            document_id=deployment.feature_list_id
        )

        async with self.drop_table_on_error(
            db_session=db_session, table_details=location.table_details, payload=payload
        ):
            await self.online_serving_service.get_online_features_from_feature_list(
                feature_list=feature_list,
                request_data=batch_request_table_model,
                output_table_details=location.table_details,
            )
            (
                columns_info,
                num_rows,
            ) = await self.batch_request_table_service.get_columns_info_and_num_rows(
                db_session, location.table_details
            )
            logger.debug("Creating a new BatchFeatureTable", extra=location.table_details.dict())
            batch_feature_table_model = BatchFeatureTableModel(
                _id=payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                batch_request_table_id=payload.batch_request_table_id,
                deployment_id=payload.deployment_id,
                columns_info=columns_info,
                num_rows=num_rows,
            )
            await self.batch_feature_table_service.create_document(batch_feature_table_model)
