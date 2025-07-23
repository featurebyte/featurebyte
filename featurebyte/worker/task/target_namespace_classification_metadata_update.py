"""
TargetNamespaceClassificationMetadataUpdateTask module
"""

from __future__ import annotations

from typing import Any

from featurebyte.schema.worker.task.target_namespace_classification_metadata_update import (
    TargetNamespaceClassificationMetadataUpdateTaskPayload,
)
from featurebyte.service.catalog import CatalogService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.target_namespace import TargetNamespaceService
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseTask


class TargetNamespaceClassificationMetadataUpdateTask(
    BaseTask[TargetNamespaceClassificationMetadataUpdateTaskPayload]
):
    """
    Task to update classification metadata for a target namespace.
    """

    payload_class = TargetNamespaceClassificationMetadataUpdateTaskPayload

    def __init__(
        self,
        target_namespace_service: TargetNamespaceService,
        observation_table_service: ObservationTableService,
        catalog_service: CatalogService,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        task_manager: TaskManager,
    ):
        super().__init__(task_manager=task_manager)
        self.target_namespace_service = target_namespace_service
        self.observation_table_service = observation_table_service
        self.catalog_service = catalog_service
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service

    async def get_task_description(
        self, payload: TargetNamespaceClassificationMetadataUpdateTaskPayload
    ) -> str:
        target_namespace = await self.target_namespace_service.get_document(
            document_id=payload.target_namespace_id
        )
        return f"Update classification metadata for target namespace [{target_namespace.name}]"

    async def execute(self, payload: TargetNamespaceClassificationMetadataUpdateTaskPayload) -> Any:
        catalog = await self.catalog_service.get_document(document_id=payload.catalog_id)
        feature_store = await self.feature_store_service.get_document(
            document_id=catalog.default_feature_store_ids[0]
        )
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store
        )

        observation_table = await self.observation_table_service.get_document(
            document_id=payload.observation_table_id
        )
        await self.target_namespace_service.update_target_namespace_classification_metadata(
            target_namespace_id=payload.target_namespace_id,
            observation_table=observation_table,
            db_session=db_session,
        )
