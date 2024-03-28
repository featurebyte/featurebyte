"""
Periodic task schema
"""

from typing import List

from featurebyte.models.periodic_task import PeriodicTask
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class PeriodicTaskUpdate(BaseDocumentServiceUpdateSchema):
    """
    Schema for periodic task update
    """

    enabled: bool


class PeriodicTaskList(PaginationMixin):
    """
    Schema for periodic task list
    """

    data: List[PeriodicTask]
