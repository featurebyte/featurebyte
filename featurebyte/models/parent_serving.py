"""
Models related to serving parent features
"""
from __future__ import annotations

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.tabular_data import TabularDataModel


class JoinStep(FeatureByteBaseModel):

    # TODO: serving names?
    data: TabularDataModel
    parent_key: str
    child_key: str
