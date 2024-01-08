"""
Feature Table Cache schema classes
"""
from typing import List

from featurebyte.models.feature_table_cache import CachedFeatureDefinition
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema


class FeatureTableCacheUpdate(BaseDocumentServiceUpdateSchema):
    """
    Feature Table Cache Update Schema
    """

    feature_definitions: List[CachedFeatureDefinition]
