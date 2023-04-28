"""
BatchFeatureTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.logging import get_logger
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.schema.worker.task.batch_feature_table import BatchFeatureTableTaskPayload
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.online_serving import OnlineServingService
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class BatchFeatureTableTask(DataWarehouseMixin, BaseTask):
    """
    BatchFeatureTableTask creates a batch feature table by computing online predictions
    """

    payload_class = BatchFeatureTableTaskPayload

    async def execute(self) -> Any:
        """
        Execute BatchFeatureTableTask
        """
        payload = cast(BatchFeatureTableTaskPayload, self.payload)
        feature_store = await self.app_container.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.get_db_session(feature_store)

        app_container = self.app_container

        batch_request_table_service: BatchRequestTableService = (
            app_container.batch_request_table_service
        )
        batch_request_table_model = await batch_request_table_service.get_document(
            payload.batch_request_table_id
        )

        batch_feature_table_service: BatchFeatureTableService = (
            app_container.batch_feature_table_service
        )
        location = await batch_feature_table_service.generate_materialized_table_location(
            self.get_credential, payload.feature_store_id
        )

        # retrieve feature list from deployment
        deployment: DeploymentModel = await app_container.deployment_service.get_document(
            document_id=payload.deployment_id
        )
        feature_list: FeatureListModel = await app_container.feature_list_service.get_document(
            document_id=deployment.feature_list_id
        )

        async with self.drop_table_on_error(
            db_session=db_session, table_details=location.table_details
        ):
            online_serving_service: OnlineServingService = app_container.online_serving_service
            await online_serving_service.get_online_features_from_feature_list(
                feature_list=feature_list,
                request_data=batch_request_table_model,
                get_credential=self.get_credential,
                output_table_details=location.table_details,
            )
            (
                columns_info,
                num_rows,
            ) = await batch_request_table_service.get_columns_info_and_num_rows(
                db_session, location.table_details
            )
            logger.debug("Creating a new BatchFeatureTable", extra=location.table_details.dict())
            batch_feature_table_model = BatchFeatureTableModel(
                _id=payload.output_document_id,
                user_id=self.payload.user_id,
                name=payload.name,
                location=location,
                batch_request_table_id=payload.batch_request_table_id,
                deployment_id=payload.deployment_id,
                columns_info=columns_info,
                num_rows=num_rows,
            )
            await batch_feature_table_service.create_document(batch_feature_table_model)
