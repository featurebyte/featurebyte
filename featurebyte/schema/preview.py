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
    feature_store_id: Optional[PydanticObjectId] = None


class FeaturePreview(ComputeRequest, PreviewObservationSet):
    """
    Feature Preview schema
    """

    graph: Optional[QueryGraph] = None
    node_name: Optional[str] = None
    feature_id: Optional[PydanticObjectId] = None
    feature_store_id: Optional[PydanticObjectId] = None


class TargetPreview(ComputeRequest, PreviewObservationSet):
    """
    Target Preview schema
    """

    graph: Optional[QueryGraph] = None
    node_name: Optional[str] = None
    target_id: Optional[PydanticObjectId] = None
    feature_store_id: Optional[PydanticObjectId] = None
