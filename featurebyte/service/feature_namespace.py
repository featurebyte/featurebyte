"""
FeatureNamespaceService class
"""

from __future__ import annotations

from featurebyte.models.feature_namespace import FeatureNamespaceModel
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceCreate,
    FeatureNamespaceServiceUpdate,
)
from featurebyte.service.base_document import BaseDocumentService


class FeatureNamespaceService(
    BaseDocumentService[
        FeatureNamespaceModel, FeatureNamespaceCreate, FeatureNamespaceServiceUpdate
    ],
):
    """
    FeatureNamespaceService class
    """

    document_class = FeatureNamespaceModel
