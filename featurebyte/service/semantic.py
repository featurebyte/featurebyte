"""
SemanticService class
"""
from __future__ import annotations

from featurebyte.models.semantic import SemanticModel
from featurebyte.schema.semantic import SemanticCreate, SemanticServiceUpdate
from featurebyte.service.base_document import BaseDocumentService


class SemanticService(BaseDocumentService[SemanticModel, SemanticCreate, SemanticServiceUpdate]):
    """
    SemanticService class
    """

    document_class = SemanticModel
