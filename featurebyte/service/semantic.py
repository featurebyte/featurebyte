"""
SemanticService class
"""

from __future__ import annotations

from featurebyte.models.semantic import SemanticModel
from featurebyte.schema.semantic import SemanticCreate, SemanticServiceUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.mixin import GetOrCreateMixin


class SemanticService(
    BaseDocumentService[SemanticModel, SemanticCreate, SemanticServiceUpdate],
    GetOrCreateMixin[SemanticModel, SemanticCreate],
):
    """
    SemanticService class
    """

    document_class = SemanticModel
    document_create_class = SemanticCreate
