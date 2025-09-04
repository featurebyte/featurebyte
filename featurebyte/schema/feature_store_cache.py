"""
FeatureStoreCacheCreate schema
"""

from typing import List

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature_store_cache import FeatureStoreCachedValue


class FeatureStoreCacheCreate(FeatureByteBaseModel):
    """
    FeatureStoreCacheCreate class
    """

    feature_store_id: PydanticObjectId
    keys: List[str]
    value: FeatureStoreCachedValue
