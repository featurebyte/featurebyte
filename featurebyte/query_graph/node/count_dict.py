"""
This module contains datetime operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal, Optional, Union
from typing_extensions import Annotated

from pydantic import BaseModel, Field

from featurebyte.common.typing import Scalar
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.agg_func import construct_agg_func
from featurebyte.query_graph.node.base import BaseSeriesOutputNode
from featurebyte.query_graph.node.metadata.operation import AggregationColumn, OperationStructure


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


class GetValueFromDictionaryNode(BaseSeriesOutputNode):
    """Get value from dictionary node class"""

    class Parameters(BaseModel):
        """Parameters"""

        value: Optional[Scalar]

    type: Literal[NodeType.GET_VALUE] = Field(NodeType.GET_VALUE, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        aggregations = inputs[0].aggregations
        agg_column = aggregations[0]
        assert isinstance(agg_column, AggregationColumn)
        method = agg_column.method
        assert method is not None
        agg_func = construct_agg_func(method)
        return agg_func.derive_output_var_type(agg_column.dtype)
