"""
FeatureListNamespaceService class
"""
from __future__ import annotations

from featurebyte.models.feature_list import FeatureListNamespaceModel
from featurebyte.routes.app_container import register_service_constructor
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.service.base_document import BaseDocumentService


class FeatureListNamespaceService(
    BaseDocumentService[
        FeatureListNamespaceModel, FeatureListNamespaceModel, FeatureListNamespaceServiceUpdate
    ],
):
    """
    FeatureListNamespaceService class
    """

    document_class = FeatureListNamespaceModel


register_service_constructor(FeatureListNamespaceService)
