"""
Preview schema
"""
from typing import Any, Dict, Optional

from pydantic import StrictStr, root_validator

from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.common.feature_or_target import ComputeRequest
from featurebyte.schema.feature_list import PreviewObservationSet


class FeatureOrTargetPreview(ComputeRequest, PreviewObservationSet):
    """
    Feature Preview schema
    """

    feature_store_name: StrictStr
    graph: Optional[QueryGraph]
    node_name: Optional[str]
    object_id: Optional[PydanticObjectId]

    @root_validator
    @classmethod
    def _validate_graph_node_name(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        graph = values.get("graph", None)
        node_name = values.get("node_name", None)
        object_id = values.get("object_id", None)
        if not (graph and node_name) and not object_id:
            raise ValueError("Either graph and node_name, or object_id must be set")

        return values
