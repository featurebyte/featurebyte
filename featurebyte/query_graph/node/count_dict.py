"""
This module contains datetime operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal, Optional, Sequence, Tuple, Union
from typing_extensions import Annotated

from abc import ABC, abstractmethod  # pylint: disable=wrong-import-order

from pydantic import BaseModel, Field

from featurebyte.common.typing import Scalar
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.agg_func import construct_agg_func
from featurebyte.query_graph.node.base import BaseSeriesOutputNode
from featurebyte.query_graph.node.metadata.operation import AggregationColumn, OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerationConfig,
    CodeGenerationContext,
    ExpressionStr,
    StatementT,
    ValueStr,
    VariableNameGenerator,
    VarNameExpressionInfo,
)


class BaseCountDictOpNode(BaseSeriesOutputNode, ABC):
    """BaseCountDictOpNode class"""

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name_expression = var_name_expressions[0].as_input()
        other_operands = [val.as_input() for val in var_name_expressions[1:]]
        expression = ExpressionStr(
            self.generate_expression(operand=var_name_expression, other_operands=other_operands)
        )
        return [], expression

    @abstractmethod
    def generate_expression(self, operand: str, other_operands: List[str]) -> str:
        """
        Generate expression for the unary operation

        Parameters
        ----------
        operand: str
            First operand
        other_operands: List[str]
            Other operands

        Returns
        -------
        str
        """


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

    def generate_expression(self, operand: str, other_operands: List[str]) -> str:
        params = ""
        if isinstance(self.parameters, self.UniqueCountParameters):
            params = f"include_missing={ValueStr.create(self.parameters.include_missing)}"
        return f"{operand}.cd.{self.parameters.transform_type}({params})"


class CosineSimilarityNode(BaseCountDictOpNode):
    """CosineSimilarityNode class"""

    type: Literal[NodeType.COSINE_SIMILARITY] = Field(NodeType.COSINE_SIMILARITY, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT

    def generate_expression(self, operand: str, other_operands: List[str]) -> str:
        return f"{operand}.cd.cosine_similarity(other={other_operands[0]})"


class DictionaryKeysNode(BaseSeriesOutputNode):
    """Dictionary keys node class"""

    type: Literal[NodeType.DICTIONARY_KEYS] = Field(NodeType.DICTIONARY_KEYS, const=True)

    @property
    def max_input_count(self) -> int:
        return 1

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.ARRAY

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        return [], node_inputs[0]


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

    def generate_expression(self, operand: str, other_operands: List[str]) -> str:
        param = other_operands[0] if other_operands else ValueStr.create(self.parameters.value)
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

    def generate_expression(self, operand: str, other_operands: List[str]) -> str:
        key = other_operands[0] if other_operands else self.parameters.value
        descending = ValueStr.create(self.parameters.descending)
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

    def generate_expression(self, operand: str, other_operands: List[str]) -> str:
        param = other_operands[0] if other_operands else ValueStr.create(self.parameters.value)
        return f"{operand}.cd.get_relative_frequency(key={param})"
