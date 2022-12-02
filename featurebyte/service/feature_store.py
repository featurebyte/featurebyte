"""
FeatureStoreService class
"""
from __future__ import annotations

from typing import Type

from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.routes.app_container import register_service_constructor
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.schema.feature_store import FeatureStoreCreate
from featurebyte.service.base_document import BaseDocumentService


class FeatureStoreService(
    BaseDocumentService[FeatureStoreModel, FeatureStoreCreate, BaseDocumentServiceUpdateSchema],
):
    """
    FeatureStoreService class
    """

    document_class: Type[FeatureStoreModel] = FeatureStoreModel


register_service_constructor(FeatureStoreService)
