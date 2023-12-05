"""
Feast registry related schemas
"""
from typing import List, Optional

from featurebyte.models import FeatureListModel
from featurebyte.models.base import FeatureByteBaseModel


class FeastRegistryCreate(FeatureByteBaseModel):
    """
    Feast registry create schema
    """

    project_name: Optional[str]
    feature_lists: List[FeatureListModel]
