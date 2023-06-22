"""
Target class
"""
from __future__ import annotations

from featurebyte.models.target import TargetModel
from featurebyte.schema.target import TargetCreate, TargetServiceUpdate
from featurebyte.service.base_document import BaseDocumentService


class TargetService(BaseDocumentService[TargetModel, TargetCreate, TargetServiceUpdate]):
    """
    TargetService class
    """

    document_class = TargetModel
