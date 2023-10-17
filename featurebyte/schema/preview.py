"""
Preview schema
"""
from typing import Optional

from pydantic import StrictStr

from featurebyte.enum import FeatureOrTargetType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.common.feature_or_target import ComputeRequest
from featurebyte.schema.feature_list import PreviewObservationSet


class FeatureOrTarget(FeatureByteBaseModel):
    """
    FeatureOrTarget schema
    """

    type: FeatureOrTargetType
    id: PydanticObjectId


class FeatureOrTargetPreview(ComputeRequest, PreviewObservationSet):
    """
    Feature Preview schema
    """

    feature_store_name: StrictStr
    graph: Optional[QueryGraph]
    node_name: Optional[str]
    feature_or_target: Optional[FeatureOrTarget]
