"""
Models related to serving parent features
"""
from __future__ import annotations

from typing import List

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.proxy_table import ProxyTableModel
from featurebyte.query_graph.node.schema import FeatureStoreDetails


class JoinStep(FeatureByteBaseModel):
    """
    JoinStep contains all information required to perform a join between two related entities for
    the purpose of serving parent features

    table: ProxyTableModel
        The table encoding the relationship between the two entities
    parent_key: str
        Column name in the table that is tagged as the parent entity
    parent_serving_name: str
        Serving name of the parent entity
    child_key: str
        Column name in the table that is tagged as the child entity
    child_serving_name: str
        Serving name of the child entity
    """

    table: ProxyTableModel
    parent_key: str
    parent_serving_name: str
    child_key: str
    child_serving_name: str


class ParentServingPreparation(FeatureByteBaseModel):
    """
    Operations required to serve parent features from children entities

    join_steps: List[JoinStep]
        List of JoinSteps identified based on the provided entities
    feature_store_details: FeatureStoreDetails
        Feature store information
    """

    join_steps: List[JoinStep]
    feature_store_details: FeatureStoreDetails
