"""
Online store initialization task
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from featurebyte.feast.model.registry import FeastRegistryModel
from featurebyte.feast.service.feature_store import FeastFeatureStoreService
from featurebyte.feast.service.registry import FeastRegistryService
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.schema.catalog import CatalogOnlineStoreUpdate
from featurebyte.schema.worker.task.online_store_initialize import (
    CatalogOnlineStoreInitializeTaskPayload,
)
from featurebyte.service.catalog import CatalogService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.feature_materialize import FeatureMaterializeService
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService
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
        feature_materialize_service: FeatureMaterializeService,
        offline_store_feature_table_service: OfflineStoreFeatureTableService,
        feast_registry_service: FeastRegistryService,
        feast_feature_store_service: FeastFeatureStoreService,
        catalog_service: CatalogService,
        deployment_service: DeploymentService,
        task_progress_updater: TaskProgressUpdater,
    ):
        super().__init__()
        self.feature_materialize_service = feature_materialize_service
        self.offline_store_feature_table_service = offline_store_feature_table_service
        self.feast_registry_service = feast_registry_service
        self.feast_feature_store_service = feast_feature_store_service
        self.catalog_service = catalog_service
        self.deployment_service = deployment_service
        self.task_progress_updater = task_progress_updater

        # Cache feast registry by deployment id
        self._deployment_id_to_feast_registry: Dict[
            PydanticObjectId, Optional[FeastRegistryModel]
        ] = {}

    async def get_task_description(self, payload: CatalogOnlineStoreInitializeTaskPayload) -> str:
        if payload.online_store_id is not None:
            return f'Updating online store "{payload.online_store_id}" for catalog {payload.catalog_id}'
        return f"Disabling online store for catalog {payload.catalog_id}"

    async def get_feast_registry(
        self, deployment_id: PydanticObjectId
    ) -> Optional[FeastRegistryModel]:
        """
        Get feast registry for the given deployment id

        Parameters
        ----------
        deployment_id: PydanticObjectId
            Deployment id

        Returns
        -------
        Optional[FeastRegistryModel]
            Feast registry model
        """
        if deployment_id in self._deployment_id_to_feast_registry:
            return self._deployment_id_to_feast_registry[deployment_id]

        deployment = await self.deployment_service.get_document(document_id=deployment_id)
        feast_registry = None
        if deployment.registry_info:
            feast_registry = await self.feast_registry_service.get_document(
                document_id=deployment.registry_info.registry_id
            )

        self._deployment_id_to_feast_registry[deployment_id] = feast_registry
        return feast_registry

    async def execute(self, payload: CatalogOnlineStoreInitializeTaskPayload) -> Any:
        logger.info(f"Starting task: {self.get_task_description(payload)}")

        if payload.online_store_id is not None:
            await self._run_materialize(payload)

        logger.info(
            "Updating online store for catalog",
            extra={"online_store_id": payload.online_store_id, "catalog_id": payload.catalog_id},
        )
        await self.catalog_service.update_online_store(
            document_id=payload.catalog_id,
            data=CatalogOnlineStoreUpdate(online_store_id=payload.online_store_id),
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
                int(100.0 * (current_table_index + 1) / total_count),
                message=f"Updating online store for offline store_table {feature_table_model.name}",
            )
            current_table_index += 1

            if session is None:
                session = await self.feature_materialize_service._get_session(  # pylint: disable=protected-access
                    feature_table_model
                )

            if feature_table_model.deployment_ids:
                feast_registry = await self.get_feast_registry(
                    deployment_id=feature_table_model.deployment_ids[0]
                )
                if feast_registry:
                    feast_feature_store = (
                        await self.feast_feature_store_service.get_feast_feature_store(
                            feast_registry=feast_registry,
                            online_store_id=payload.online_store_id,
                        )
                    )
                    if feast_feature_store:
                        await self.feature_materialize_service.update_online_store(
                            feature_store=feast_feature_store,
                            feature_table_model=feature_table_model,
                            session=session,
                        )
