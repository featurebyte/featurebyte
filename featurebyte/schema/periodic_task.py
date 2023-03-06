"""
Periodic task schema
"""
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema


class PeriodicTaskUpdate(BaseDocumentServiceUpdateSchema):
    """
    Schema for periodic task update
    """

    enabled: bool
