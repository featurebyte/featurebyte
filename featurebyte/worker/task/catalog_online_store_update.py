"""
Online store initialization task
"""

from __future__ import annotations

from typing import Any

from featurebyte.feast.service.feature_store import FeastFeatureStoreService
from featurebyte.feast.service.registry import FeastRegistryService
from featurebyte.logging import get_logger
from featurebyte.schema.catalog import CatalogOnlineStoreUpdate
from featurebyte.schema.worker.task.online_store_initialize import (
    CatalogOnlineStoreInitializeTaskPayload,
)
from featurebyte.service.catalog import CatalogService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.feature_materialize import FeatureMaterializeService
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

logger = get_logger(__name__)


class CatalogOnlineStoreUpdateTask(BaseTask[CatalogOnlineStoreInitializeTaskPayload]):
    """
    CatalogOnlineStoreUpdateTask class

    This task is triggered after a request to update a catalog to use a new online store.

    This task updates the online store so that it contains the feature values for the currently
    deployed features in the catalog. Once that is done, this completes the catalog update by
    pointing the catalog to the new online store.
    """

    payload_class = CatalogOnlineStoreInitializeTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        feature_materialize_service: FeatureMaterializeService,
        offline_store_feature_table_service: OfflineStoreFeatureTableService,
        feast_registry_service: FeastRegistryService,
        feast_feature_store_service: FeastFeatureStoreService,
        catalog_service: CatalogService,
        deployment_service: DeploymentService,
        task_progress_updater: TaskProgressUpdater,
    ):
        super().__init__(task_manager=task_manager)
        self.feature_materialize_service = feature_materialize_service
        self.offline_store_feature_table_service = offline_store_feature_table_service
        self.feast_registry_service = feast_registry_service
        self.feast_feature_store_service = feast_feature_store_service
        self.catalog_service = catalog_service
        self.deployment_service = deployment_service
        self.task_progress_updater = task_progress_updater

    async def get_task_description(self, payload: CatalogOnlineStoreInitializeTaskPayload) -> str:
        if payload.online_store_id is not None:
            return f'Updating online store "{payload.online_store_id}" for catalog {payload.catalog_id}'
        return f"Disabling online store for catalog {payload.catalog_id}"

    async def execute(self, payload: CatalogOnlineStoreInitializeTaskPayload) -> Any:
        logger.info(f"Starting task: {await self.get_task_description(payload)}")

        # Check if we need to populate offline store tables based on the new desired states
        need_offline_store_tables = (
            payload.populate_offline_feature_tables is True or payload.online_store_id is not None
        )

        # Need to populate offline store tables if they are not already populated
        catalog_model = await self.catalog_service.get_document(payload.catalog_id)
        if need_offline_store_tables and catalog_model.populate_offline_feature_tables is not True:
            await self._populate_offline_store_tables(payload)

        if payload.online_store_id is not None:
            await self._run_materialize(payload)

        logger.info(
            "Updating online store for catalog",
            extra={"online_store_id": payload.online_store_id, "catalog_id": payload.catalog_id},
        )
        await self.catalog_service.update_online_store(
            document_id=payload.catalog_id,
            data=CatalogOnlineStoreUpdate(
                online_store_id=payload.online_store_id,
                populate_offline_feature_tables=payload.populate_offline_feature_tables,
            ),
        )

    async def _populate_offline_store_tables(
        self,
        payload: CatalogOnlineStoreInitializeTaskPayload,
    ) -> None:
        logger.info(
            "Populating offline store feature tables for catalog",
            extra={"catalog_id": payload.catalog_id},
        )

        offline_store_tables = []
        async for (
            feature_table_model
        ) in self.offline_store_feature_table_service.list_source_feature_tables():
            offline_store_tables.append(feature_table_model)

        for current_table_index, feature_table_model in enumerate(offline_store_tables):
            logger.info(
                "Populating offline store feature table",
                extra={
                    "feature_table_name": feature_table_model.name,
                    "catalog_id": payload.catalog_id,
                },
            )
            await self.task_progress_updater.update_progress(
                int(50 * (current_table_index + 1) / len(offline_store_tables)),
                message=f"Populating offline store table {feature_table_model.name}",
            )
            await self.feature_materialize_service.scheduled_populate_offline_feature_table(
                feature_table_model=feature_table_model,
            )

    async def _run_materialize(self, payload: CatalogOnlineStoreInitializeTaskPayload) -> None:
        session = None
        total_count = (await self.offline_store_feature_table_service.list_documents_as_dict())[
            "total"
        ]
        current_table_index = 0
        async for (
            feature_table_model
        ) in self.offline_store_feature_table_service.list_documents_iterator({}):
            logger.info(
                "Updating online store for offline feature store table",
                extra={
                    "online_store_id": payload.online_store_id,
                    "feature_table_name": feature_table_model.name,
                    "catalog_id": payload.catalog_id,
                },
            )
            await self.task_progress_updater.update_progress(
                int(50 + 50 * (current_table_index + 1) / total_count),
                message=f"Updating online store for offline store_table {feature_table_model.name}",
            )
            current_table_index += 1

            if session is None:
                session = await self.feature_materialize_service._get_session(feature_table_model)

            if feature_table_model.deployment_ids:
                service = self.feast_feature_store_service
                feast_feature_store = (
                    await service.get_feast_feature_store_for_feature_materialization(
                        feature_table_model=feature_table_model,
                        online_store_id=payload.online_store_id,
                    )
                )
                if feast_feature_store:
                    await self.feature_materialize_service.update_online_store(
                        feature_store=feast_feature_store,
                        feature_table_model=feature_table_model,
                        session=session,
                    )
