"""
This module contains datetime operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal, Optional, Union
from typing_extensions import Annotated

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import BaseSeriesOutputNode
from featurebyte.query_graph.node.metadata.operation import OperationStructure


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

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        if self.parameters.transform_type == "most_frequent":
            return DBVarType.VARCHAR
        return DBVarType.FLOAT


class CosineSimilarityNode(BaseSeriesOutputNode):
    """CosineSimilarityNode class"""

    type: Literal[NodeType.COSINE_SIMILARITY] = Field(NodeType.COSINE_SIMILARITY, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT


class DictionaryKeysNode(BaseSeriesOutputNode):
    """Dictionary keys node class"""

    type: Literal[NodeType.DICTIONARY_KEYS] = Field(NodeType.DICTIONARY_KEYS, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.ARRAY
