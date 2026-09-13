"""
ExposureNamespaceService class
"""

from __future__ import annotations

from featurebyte.models.exposure_namespace import ExposureNamespaceModel
from featurebyte.schema.exposure_namespace import (
    ExposureNamespaceCreate,
    ExposureNamespaceServiceUpdate,
)
from featurebyte.service.base_document import BaseDocumentService


class ExposureNamespaceService(
    BaseDocumentService[
        ExposureNamespaceModel, ExposureNamespaceCreate, ExposureNamespaceServiceUpdate
    ],
):
    """
    ExposureNamespaceService class
    """

    document_class = ExposureNamespaceModel
