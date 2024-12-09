"""
Catalog Cleanup Task
"""

from featurebyte.enum import WorkerCommand
from featurebyte.schema.worker.task.base import BaseTaskPayload

DELETED_CATALOG_CLEANUP_THRESHOLD_IN_DAYS = 7


class CatalogCleanupTaskPayload(BaseTaskPayload):
    """
    Catalog Cleanup Task Payload
    """

    command = WorkerCommand.CATALOG_CLEANUP
    cleanup_threshold_in_days: int = DELETED_CATALOG_CLEANUP_THRESHOLD_IN_DAYS
