"""
TargetNamespaceService class
"""
from __future__ import annotations

from featurebyte.models.target_namespace import TargetNamespaceModel
from featurebyte.schema.target_namespace import TargetNamespaceCreate, TargetNamespaceUpdate
from featurebyte.service.base_document import BaseDocumentService


class TargetNamespaceService(
    BaseDocumentService[TargetNamespaceModel, TargetNamespaceCreate, TargetNamespaceUpdate],
):
    """
    TargetNamespaceService class
    """

    document_class = TargetNamespaceModel
