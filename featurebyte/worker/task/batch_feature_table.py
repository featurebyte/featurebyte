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
from featurebyte.schema.worker.task.batch_request_table import BatchRequestTableTaskPayload
from featurebyte.service.batch_external_feature_table import BatchExternalFeatureTableService
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.online_serving import OnlineServingService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.batch_request_table import BatchRequestTableTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class BatchFeatureTableTask(DataWarehouseMixin, BaseTask[BatchFeatureTableTaskPayload]):
    """
    BatchFeatureTableTask creates a batch feature table by computing online predictions
    """

    payload_class = BatchFeatureTableTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        batch_request_table_service: BatchRequestTableService,
        batch_feature_table_service: BatchFeatureTableService,
        batch_external_feature_table_service: BatchExternalFeatureTableService,
        deployment_service: DeploymentService,
        feature_list_service: FeatureListService,
        online_serving_service: OnlineServingService,
        entity_validation_service: EntityValidationService,
        batch_request_table_task: BatchRequestTableTask,
    ):
        super().__init__(task_manager=task_manager)
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.batch_request_table_service = batch_request_table_service
        self.batch_feature_table_service = batch_feature_table_service
        self.deployment_service = deployment_service
        self.feature_list_service = feature_list_service
        self.online_serving_service = online_serving_service
        self.entity_validation_service = entity_validation_service
        self.batch_request_table_task = batch_request_table_task
        self.batch_external_feature_table_service = batch_external_feature_table_service

    async def get_task_description(self, payload: BatchFeatureTableTaskPayload) -> str:
        if payload.output_table_info is not None:
            return f"Compute and append batch features to table {payload.output_table_info.name}"
        return f'Save batch feature table "{payload.name}"'

    async def execute(self, payload: BatchFeatureTableTaskPayload) -> Any:
        feature_store = await self.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)

        if payload.batch_request_table_id:
            batch_request_table_model = await self.batch_request_table_service.get_document(
                payload.batch_request_table_id
            )
        else:
            # Create a temporary batch request table
            batch_request_table_payload = BatchRequestTableTaskPayload(
                name="temporary_batch_request_table",
                feature_store_id=feature_store.id,
                request_input=payload.request_input,
                user_id=payload.user_id,
                catalog_id=payload.catalog_id,
                output_document_id=payload.output_document_id,
            )
            batch_request_table_model = (
                await self.batch_request_table_task.create_batch_request_table(
                    db_session, batch_request_table_payload, create_document=False
                )
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

        try:
            if payload.batch_request_table_id is None:
                # Validate entities of the temporary batch request table
                await (
                    self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
                        feature_list_model=feature_list,
                        request_column_names={
                            col.name for col in batch_request_table_model.columns_info
                        },
                        feature_store=feature_store,
                    )
                )

            async with self.drop_table_on_error(
                db_session=db_session,
                list_of_table_details=[location.table_details],
                payload=payload,
            ):
                await self.online_serving_service.get_online_features_from_feature_list(
                    feature_list=feature_list,
                    request_data=batch_request_table_model,
                    output_table_details=location.table_details,
                    batch_feature_table_id=payload.output_document_id,
                    point_in_time=payload.point_in_time,
                )
                (
                    columns_info,
                    num_rows,
                ) = await self.batch_feature_table_service.get_columns_info_and_num_rows(
                    db_session, location.table_details
                )
                batch_feature_table_model = BatchFeatureTableModel(
                    _id=payload.output_document_id,
                    user_id=payload.user_id,
                    name=payload.name,
                    location=location,
                    batch_request_table_id=payload.batch_request_table_id,
                    request_input=batch_request_table_model.request_input,
                    deployment_id=payload.deployment_id,
                    columns_info=columns_info,
                    num_rows=num_rows,
                    parent_batch_feature_table_name=payload.parent_batch_feature_table_name,
                )

                if payload.output_table_info is not None:
                    # Append feature values from temporary batch request table to output table
                    await self.batch_external_feature_table_service.write_batch_features_to_table(
                        batch_feature_table=batch_feature_table_model,
                        output_table_info=payload.output_table_info,
                        point_in_time=payload.point_in_time,
                    )

                    # Drop the temporary batch feature table
                    await db_session.drop_table(
                        table_name=location.table_details.table_name,
                        schema_name=location.table_details.schema_name,  # type: ignore
                        database_name=location.table_details.database_name,  # type: ignore
                    )
                else:
                    logger.debug(
                        "Creating a new BatchFeatureTable",
                        extra=location.table_details.model_dump(),
                    )
                    await self.batch_feature_table_service.create_document(
                        batch_feature_table_model
                    )
        finally:
            if payload.batch_request_table_id is None:
                # clean up temporary batch request table
                await db_session.drop_table(
                    table_name=batch_request_table_model.location.table_details.table_name,
                    schema_name=batch_request_table_model.location.table_details.schema_name,  # type: ignore
                    database_name=batch_request_table_model.location.table_details.database_name,  # type: ignore
                )
