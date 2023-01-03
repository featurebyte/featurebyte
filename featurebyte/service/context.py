"""
ContextService class
"""
from __future__ import annotations

from featurebyte.models.context import ContextModel
from featurebyte.schema.context import ContextCreate, ContextUpdate
from featurebyte.service.base_document import BaseDocumentService


class ContextService(BaseDocumentService[ContextModel, ContextCreate, ContextUpdate]):
    """
    ContextService class
    """

    document_class = ContextModel
