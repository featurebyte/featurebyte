"""
Test task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.logging import get_logger
from featurebyte.schema.worker.task.tile import TileTaskPayload
from featurebyte.worker.task.base import BaseTask

logger = get_logger(__name__)


class TileTask(BaseTask):
    """
    Test Task
    """

    payload_class = TileTaskPayload

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
        feature_store = await self.app_container.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )

        # establish database session
        db_session = await self.app_container.session_manager_service.get_feature_store_session(
            feature_store
        )

        await self.app_container.tile_task_executor.execute(
            session=db_session, params=payload.parameters
        )

        logger.debug("Tile task ended")
