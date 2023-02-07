"""
Models related to serving parent features
"""
from __future__ import annotations

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.tabular_data import TabularDataModel


class JoinStep(FeatureByteBaseModel):

    data: TabularDataModel
    parent_key: str
    parent_serving_name: str
    child_key: str
    child_serving_name: str
