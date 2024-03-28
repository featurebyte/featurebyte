"""
Feature Table Cache schema classes
"""

from typing import List

from featurebyte.models.feature_table_cache_metadata import CachedFeatureDefinition
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema


class FeatureTableCacheMetadataUpdate(BaseDocumentServiceUpdateSchema):
    """
    Feature Table Cache Metadata Update Schema
    """

    feature_definitions: List[CachedFeatureDefinition]
