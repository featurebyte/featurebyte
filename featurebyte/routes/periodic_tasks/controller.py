"""
Periodic Task API route controller
"""

from __future__ import annotations

from featurebyte.models.periodic_task import PeriodicTask
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.periodic_task import PeriodicTaskList
from featurebyte.service.periodic_task import PeriodicTaskService


class PeriodicTaskController(
    BaseDocumentController[PeriodicTask, PeriodicTaskService, PeriodicTaskList],
):
    """
    PeriodicTask Controller
    """

    paginated_document_class = PeriodicTaskList
