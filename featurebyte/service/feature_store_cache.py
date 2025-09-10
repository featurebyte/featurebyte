"""
Feature store cache service
"""

from typing import Type

from featurebyte.models.feature_store_cache import FeatureStoreCacheModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.schema.feature_store_cache import FeatureStoreCacheCreate
from featurebyte.service.base_document import BaseDocumentService


class FeatureStoreCacheService(
    BaseDocumentService[
        FeatureStoreCacheModel, FeatureStoreCacheCreate, BaseDocumentServiceUpdateSchema
    ],
):
    """
    ManagedViewService class
    """

    document_class: Type[FeatureStoreCacheModel] = FeatureStoreCacheModel
