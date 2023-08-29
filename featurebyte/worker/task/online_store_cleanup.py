"""
Test task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.logging import get_logger
from featurebyte.schema.worker.task.online_store_cleanup import OnlineStoreCleanupTaskPayload
from featurebyte.worker.task.base import BaseTask

logger = get_logger(__name__)


class OnlineStoreCleanupTask(BaseTask):
    """
    OnlineStoreCleanupTask class
    """

    payload_class = OnlineStoreCleanupTaskPayload

    async def get_task_description(self) -> str:
        payload = cast(OnlineStoreCleanupTaskPayload, self.payload)
        return f'Clean up online store table "{payload.online_store_table_name}"'

    async def execute(self) -> Any:
        """
        Execute task
        """
        payload = cast(OnlineStoreCleanupTaskPayload, self.payload)
        await self.app_container.online_store_cleanup_service.run_cleanup(
            feature_store_id=payload.feature_store_id,
            online_store_table_name=payload.online_store_table_name,
        )
