"""
ModelingTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.models.modeling_table import ModelingTableModel
from featurebyte.routes.app_container import AppContainer
from featurebyte.schema.worker.task.modeling_table import ModelingTableTaskPayload
from featurebyte.service.modeling_table import ModelingTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.preview import PreviewService
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseTask


class ModelingTableTask(BaseTask):
    """
    ModelingTableTask creates a ModelingTable by computing historical features
    """

    payload_class = ModelingTableTaskPayload

    async def execute(self) -> Any:
        """
        Execute ModelingTableTask
        """
        payload = cast(ModelingTableTaskPayload, self.payload)

        # TODO: move this to BaseTask?
        app_container = AppContainer.get_instance(
            user=self.user,
            persistent=self.get_persistent(),
            temp_storage=self.get_temp_storage(),
            task_manager=TaskManager(
                user=self.user,
                persistent=self.get_persistent(),
                catalog_id=payload.catalog_id,
            ),
            storage=self.get_storage(),
            container_id=payload.catalog_id,
        )

        observation_table_service: ObservationTableService = app_container.observation_table_service
        observation_table_model = await observation_table_service.get_document(
            payload.observation_table_id
        )

        modeling_table_service: ModelingTableService = app_container.modeling_table_service
        location = await modeling_table_service.generate_materialized_table_location(
            self.get_credential, payload.feature_store_id
        )

        preview_service: PreviewService = app_container.preview_service

        await preview_service.get_historical_features(
            observation_set=observation_table_model,
            featurelist_get_historical_features=payload.featurelist_get_historical_features,
            get_credential=self.get_credential,
            output_table_details=location.table_details,
        )

        modeling_table_model = ModelingTableModel(
            _id=payload.output_document_id,
            user_id=self.payload.user_id,
            name=payload.name,
            location=location,
            observation_table_id=payload.observation_table_id,
            feature_list_id=payload.featurelist_get_historical_features.feature_list_id,
        )
        created_doc = await modeling_table_service.create_document(modeling_table_model)
        assert created_doc.id == payload.output_document_id
