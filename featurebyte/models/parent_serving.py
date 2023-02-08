"""
Models related to serving parent features
"""
from __future__ import annotations

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.tabular_data import TabularDataModel


class JoinStep(FeatureByteBaseModel):
    """
    JoinStep contains all information required to perform a join between two related entities for
    the purpose of serving parent features

    data: TabularDataModel
        The data encoding the relationship between the two entities
    parent_key: str
        Column name in the data that is tagged as the parent entity
    parent_serving_name: str
        Serving name of the parent entity
    child_key: str
        Column name in the data that is tagged as the child entity
    child_serving_name: str
        Serving name of the child entity
    """

    data: TabularDataModel
    parent_key: str
    parent_serving_name: str
    child_key: str
    child_serving_name: str
