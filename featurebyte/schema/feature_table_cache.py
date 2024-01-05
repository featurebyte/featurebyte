"""
Feature Table Cache schema classes
"""
from typing import List, Optional

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema


class FeatureTableCacheUpdate(BaseDocumentServiceUpdateSchema):
    """
    Feature Table Cache Update Schema
    """

    features: List[str]


class FeatureTableCacheInfo(FeatureByteBaseModel):
    """
    Feature Table Cache info object
    """

    cache_table_name: str
    cached_feature_names: List[str]


class CachedFeatureDefinition(FeatureByteBaseModel):
    """
    Defintion of the feature which is used in Feature Table Cache
    """

    name: str
    definition_hash: Optional[str]
