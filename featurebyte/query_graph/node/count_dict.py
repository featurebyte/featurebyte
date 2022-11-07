"""
This module contains datetime operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Literal, Optional, Union
from typing_extensions import Annotated

from pydantic import BaseModel, Field

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import BaseSeriesOutputNode


class CountDictTransformNode(BaseSeriesOutputNode):
    """CountDictTransformNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        transform_type: Literal["entropy", "most_frequent"]

    class UniqueCountParameters(BaseModel):
        """UniqueCountParameters"""

        transform_type: Literal["unique_count"]
        include_missing: Optional[bool]

    type: Literal[NodeType.COUNT_DICT_TRANSFORM] = Field(NodeType.COUNT_DICT_TRANSFORM, const=True)
    parameters: Annotated[
        Union[Parameters, UniqueCountParameters], Field(discriminator="transform_type")
    ]


class CosineSimilarityNode(BaseSeriesOutputNode):
    """CosineSimilarityNode class"""

    type: Literal[NodeType.COSINE_SIMILARITY] = Field(NodeType.COSINE_SIMILARITY, const=True)
