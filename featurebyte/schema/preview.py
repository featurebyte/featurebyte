"""
Preview schema
"""
from typing import Any, Dict, List

from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.graph import QueryGraph


class FeatureOrTargetPreview(FeatureByteBaseModel):
    """
    Feature Preview schema
    """

    graph: QueryGraph
    node_name: str
    feature_store_name: StrictStr
    point_in_time_and_serving_name_list: List[Dict[str, Any]] = Field(min_items=1, max_items=50)
