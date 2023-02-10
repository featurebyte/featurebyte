"""
This module contains datetime operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal, Optional, Tuple, Union
from typing_extensions import Annotated

from abc import ABC, abstractmethod

from pydantic import BaseModel, Field

from featurebyte.common.typing import Scalar
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.agg_func import construct_agg_func
from featurebyte.query_graph.node.base import BaseSeriesOutputNode
from featurebyte.query_graph.node.metadata.operation import AggregationColumn, OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    ExpressionStr,
    StatementT,
    StyleConfig,
    VariableNameGenerator,
    VarNameExpression,
)


class BaseCountDictOpNode(BaseSeriesOutputNode, ABC):
    """BaseCountDictOpNode class"""

    @abstractmethod
    def _generate_expression(self, operand: str, other_operands: List[str]) -> str:
        """
        Generate expression for the unary operation

        Parameters
        ----------
        operand: str
            Operand

        Returns
        -------
        str
        """

    def _derive_sdk_codes(
        self,
        input_var_name_expressions: List[VarNameExpression],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        style_config: StyleConfig,
    ) -> Tuple[List[StatementT], VarNameExpression]:
        var_name_expression = input_var_name_expressions[0].as_input()
        other_operands = [val.as_input() for val in input_var_name_expressions[1:]]
        expression = ExpressionStr(
            self._generate_expression(operand=var_name_expression, other_operands=other_operands)
        )
        return [], expression


class CountDictTransformNode(BaseCountDictOpNode):
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

    def _generate_expression(self, operand: str, other_operands: List[str]) -> str:
        params = ""
        if self.parameters.transform_type == "unique_count":
            params = f"include_missing={self.parameters.include_missing}"
        return f"{operand}.cd.{self.parameters.transform_type}({params})"


class CosineSimilarityNode(BaseCountDictOpNode):
    """CosineSimilarityNode class"""

    type: Literal[NodeType.COSINE_SIMILARITY] = Field(NodeType.COSINE_SIMILARITY, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT

    def _generate_expression(self, operand: str, other_operands: List[str]) -> str:
        return f"{operand}.cd.cosine_similarity({other_operands[0]})"


class DictionaryKeysNode(BaseSeriesOutputNode):
    """Dictionary keys node class"""

    type: Literal[NodeType.DICTIONARY_KEYS] = Field(NodeType.DICTIONARY_KEYS, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.ARRAY

    def _derive_sdk_codes(
        self,
        input_var_name_expressions: List[VarNameExpression],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        style_config: StyleConfig,
    ) -> Tuple[List[StatementT], VarNameExpression]:
        return [], input_var_name_expressions[0]


class GetValueFromDictionaryNode(BaseCountDictOpNode):
    """Get value from dictionary node class"""

    class Parameters(BaseModel):
        """Parameters"""

        value: Optional[Scalar]

    type: Literal[NodeType.GET_VALUE] = Field(NodeType.GET_VALUE, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        aggregations = inputs[0].aggregations
        agg_column = aggregations[0]
        # This assumes that dictionary features are never post-processed.
        assert isinstance(agg_column, AggregationColumn)
        method = agg_column.method
        assert method is not None
        agg_func = construct_agg_func(method)
        # derive the output_var_type using aggregation's parent column without passing category parameter
        # as count method doesn't have any parent column, take the first input column as parent column
        parent_column = agg_column.column
        if parent_column is None:
            parent_column = inputs[0].columns[0]
        return agg_func.derive_output_var_type(parent_column.dtype, category=None)

    def _generate_expression(self, operand: str, other_operands: List[str]) -> str:
        param = other_operands[0] if other_operands else self.parameters.value
        return f"{operand}.cd.get_value(key={param})"


class GetRankFromDictionaryNode(BaseCountDictOpNode):
    """Get rank from dictionary node class"""

    class Parameters(BaseModel):
        """Parameters"""

        value: Optional[Scalar]
        descending: bool = False

    type: Literal[NodeType.GET_RANK] = Field(NodeType.GET_RANK, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT

    def _generate_expression(self, operand: str, other_operands: List[str]) -> str:
        key = other_operands[0] if other_operands else self.parameters.value
        descending = self.parameters.descending
        params = f"key={key}, descending={descending}"
        return f"{operand}.cd.get_value(key={params})"


class GetRelativeFrequencyFromDictionaryNode(BaseCountDictOpNode):
    """Get relative frequency from dictionary node class"""

    class Parameters(BaseModel):
        """Parameters"""

        value: Optional[Scalar]

    type: Literal[NodeType.GET_RELATIVE_FREQUENCY] = Field(
        NodeType.GET_RELATIVE_FREQUENCY, const=True
    )
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT

    def _generate_expression(self, operand: str, other_operands: List[str]) -> str:
        param = other_operands[0] if other_operands else self.parameters.value
        return f"{operand}.cd.get_relative_frequency(key={param})"
