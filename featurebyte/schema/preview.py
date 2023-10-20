"""
Preview schema
"""
from typing import Optional

from pydantic import StrictStr

from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.common.feature_or_target import ComputeRequest
from featurebyte.schema.feature_list import PreviewObservationSet


class FeatureOrTargetPreview(ComputeRequest, PreviewObservationSet):
    """
    FeatureOrTargetPreview Preview schema
    """

    feature_store_name: StrictStr
    graph: QueryGraph
    node_name: str


class FeaturePreview(ComputeRequest, PreviewObservationSet):
    """
    Feature Preview schema
    """

    feature_store_name: StrictStr
    graph: Optional[QueryGraph]
    node_name: Optional[str]
    feature_id: Optional[PydanticObjectId]


class TargetPreview(ComputeRequest, PreviewObservationSet):
    """
    Target Preview schema
    """

    feature_store_name: StrictStr
    graph: Optional[QueryGraph]
    node_name: Optional[str]
    target_id: Optional[PydanticObjectId]
