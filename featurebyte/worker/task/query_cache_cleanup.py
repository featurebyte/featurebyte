"""
Query cache cleanup task
"""

from __future__ import annotations

from typing import Any

from featurebyte.logging import get_logger
from featurebyte.schema.worker.task.query_cache_cleanup import QueryCacheCleanupTaskPayload
from featurebyte.service.query_cache_cleanup import QueryCacheCleanupService
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseTask

logger = get_logger(__name__)


class QueryCacheCleanupTask(BaseTask[QueryCacheCleanupTaskPayload]):
    """
    QueryCacheCleanupTask class
    """

    payload_class = QueryCacheCleanupTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        query_cache_cleanup_service: QueryCacheCleanupService,
    ):
        super().__init__(task_manager=task_manager)
        self.query_cache_cleanup_service = query_cache_cleanup_service

    async def get_task_description(self, payload: QueryCacheCleanupTaskPayload) -> str:
        return f'Clean up query cache for feature store id "{payload.feature_store_id}"'

    async def execute(self, payload: QueryCacheCleanupTaskPayload) -> Any:
        await self.query_cache_cleanup_service.run_cleanup(
            feature_store_id=payload.feature_store_id
        )
