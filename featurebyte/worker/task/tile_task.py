"""
Test task
"""

from __future__ import annotations

from typing import Any

from featurebyte.logging import get_logger
from featurebyte.schema.worker.task.tile import TileTaskPayload
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.task_manager import TaskManager
from featurebyte.service.tile.tile_task_executor import TileTaskExecutor
from featurebyte.worker.task.base import BaseTask

logger = get_logger(__name__)


class TileTask(BaseTask[TileTaskPayload]):
    """
    Test Task
    """

    payload_class = TileTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        tile_task_executor: TileTaskExecutor,
    ):
        super().__init__(task_manager=task_manager)
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.tile_task_executor = tile_task_executor

    async def get_task_description(self, payload: TileTaskPayload) -> str:
        return (
            f'Generate tile for "{payload.parameters.tile_id}:{payload.parameters.aggregation_id}"'
        )

    async def execute(self, payload: TileTaskPayload) -> Any:
        logger.debug("Tile task started")
        # get feature store
        feature_store = await self.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )

        # establish database session
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)

        await self.tile_task_executor.execute(session=db_session, params=payload.parameters)

        logger.debug("Tile task ended")
