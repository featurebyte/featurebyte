"""
PeriodicTaskService class
"""

from __future__ import annotations

from featurebyte.models.periodic_task import PeriodicTask
from featurebyte.schema.periodic_task import PeriodicTaskUpdate
from featurebyte.service.base_document import BaseDocumentService


class PeriodicTaskService(BaseDocumentService[PeriodicTask, PeriodicTask, PeriodicTaskUpdate]):
    """
    PeriodicTaskService class
    """

    document_class = PeriodicTask
