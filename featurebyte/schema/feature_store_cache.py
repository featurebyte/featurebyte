"""
FeatureStoreCacheCreate schema
"""

from typing import Optional

from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature_store_cache import FeatureStoreCachedValue


class FeatureStoreCacheCreate(FeatureByteBaseModel):
    """
    FeatureStoreCacheCreate class
    """

    feature_store_id: PydanticObjectId
    key: str
    database_name: Optional[str] = Field(None)
    schema_name: Optional[str] = Field(None)
    table_name: Optional[str] = Field(None)
    value: FeatureStoreCachedValue
