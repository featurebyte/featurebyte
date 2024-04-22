"""
Preview schema
"""

from typing import Optional

from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.common.feature_or_target import ComputeRequest
from featurebyte.schema.feature_list import PreviewObservationSet


class FeatureOrTargetPreview(ComputeRequest, PreviewObservationSet):
    """
    FeatureOrTargetPreview Preview schema
    """

    graph: QueryGraph
    node_name: str
    feature_store_id: Optional[PydanticObjectId]


class FeaturePreview(ComputeRequest, PreviewObservationSet):
    """
    Feature Preview schema
    """

    graph: Optional[QueryGraph]
    node_name: Optional[str]
    feature_id: Optional[PydanticObjectId]
    feature_store_id: Optional[PydanticObjectId]


class TargetPreview(ComputeRequest, PreviewObservationSet):
    """
    Target Preview schema
    """

    graph: Optional[QueryGraph]
    node_name: Optional[str]
    target_id: Optional[PydanticObjectId]
    feature_store_id: Optional[PydanticObjectId]
