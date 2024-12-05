"""
Catalog Cleanup Task
"""

from featurebyte.enum import WorkerCommand
from featurebyte.schema.worker.task.base import BaseTaskPayload


class CatalogCleanupTaskPayload(BaseTaskPayload):
    """
    Catalog Cleanup Task Payload
    """

    command = WorkerCommand.CATALOG_CLEANUP
