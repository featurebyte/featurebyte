"""
TargetNamespaceService class
"""

from __future__ import annotations

from featurebyte.models.target_namespace import TargetNamespaceModel
from featurebyte.schema.target_namespace import TargetNamespaceCreate, TargetNamespaceServiceUpdate
from featurebyte.service.base_document import BaseDocumentService


class TargetNamespaceService(
    BaseDocumentService[TargetNamespaceModel, TargetNamespaceCreate, TargetNamespaceServiceUpdate],
):
    """
    TargetNamespaceService class
    """

    document_class = TargetNamespaceModel
