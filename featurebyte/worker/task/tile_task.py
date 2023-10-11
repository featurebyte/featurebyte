"""
Test task
"""
from __future__ import annotations

from typing import Any, cast

from uuid import UUID

from featurebyte.logging import get_logger
from featurebyte.models.base import User
from featurebyte.persistent import Persistent
from featurebyte.schema.worker.task.tile import TileTaskPayload
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.tile.tile_task_executor import TileTaskExecutor
from featurebyte.storage import Storage
from featurebyte.worker.task.base import BaseTask

logger = get_logger(__name__)


class TileTask(BaseTask):
    """
    Test Task
    """

    payload_class = TileTaskPayload

    def __init__(  # pylint: disable=too-many-arguments
        self,
        task_id: UUID,
        payload: dict[str, Any],
        progress: Any,
        user: User,
        persistent: Persistent,
        storage: Storage,
        temp_storage: Storage,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        tile_task_executor: TileTaskExecutor,
    ):
        super().__init__(
            task_id=task_id,
            payload=payload,
            progress=progress,
            user=user,
            persistent=persistent,
            storage=storage,
            temp_storage=temp_storage,
        )
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.tile_task_executor = tile_task_executor

    async def get_task_description(self) -> str:
        payload = cast(TileTaskPayload, self.payload)
        return (
            f'Generate tile for "{payload.parameters.tile_id}:{payload.parameters.aggregation_id}"'
        )

    async def execute(self) -> Any:
        """
        Execute Tile task
        """
        logger.debug("Tile task started")

        payload = cast(TileTaskPayload, self.payload)

        # get feature store
        feature_store = await self.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )

        # establish database session
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)

        await self.tile_task_executor.execute(session=db_session, params=payload.parameters)

        logger.debug("Tile task ended")
