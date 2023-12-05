"""
Feast registry related schemas
"""
from typing import List, Optional

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema


class FeastRegistryCreate(FeatureByteBaseModel):
    """
    Feast registry create schema
    """

    project_name: Optional[str]
    feature_lists: List[FeatureListModel]


class FeastRegistryUpdate(BaseDocumentServiceUpdateSchema):
    """
    Feast registry update schema
    """

    feature_lists: Optional[List[FeatureListModel]]
    registry: Optional[bytes]
