"""
Test task
"""
from __future__ import annotations

from typing import Any, cast

from uuid import UUID

from featurebyte.logging import get_logger
from featurebyte.models.base import User
from featurebyte.persistent import Persistent
from featurebyte.schema.worker.task.online_store_cleanup import OnlineStoreCleanupTaskPayload
from featurebyte.service.online_store_cleanup import OnlineStoreCleanupService
from featurebyte.worker.task.base import BaseTask

logger = get_logger(__name__)


class OnlineStoreCleanupTask(BaseTask):
    """
    OnlineStoreCleanupTask class
    """

    payload_class = OnlineStoreCleanupTaskPayload

    def __init__(  # pylint: disable=too-many-arguments
        self,
        task_id: UUID,
        payload: dict[str, Any],
        progress: Any,
        user: User,
        persistent: Persistent,
        online_store_cleanup_service: OnlineStoreCleanupService,
    ):
        super().__init__(
            task_id=task_id,
            payload=payload,
            progress=progress,
            user=user,
            persistent=persistent,
        )
        self.online_store_cleanup_service = online_store_cleanup_service

    async def get_task_description(self) -> str:
        payload = cast(OnlineStoreCleanupTaskPayload, self.payload)
        return f'Clean up online store table "{payload.online_store_table_name}"'

    async def execute(self) -> Any:
        """
        Execute task
        """
        payload = cast(OnlineStoreCleanupTaskPayload, self.payload)
        await self.online_store_cleanup_service.run_cleanup(
            feature_store_id=payload.feature_store_id,
            online_store_table_name=payload.online_store_table_name,
        )
