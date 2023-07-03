"""
This module contains generic function related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, Union
from typing_extensions import Annotated

from abc import abstractmethod  # pylint: disable=wrong-import-order

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType, FunctionParameterInputForm
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.base import BaseSeriesOutputNode
from featurebyte.query_graph.node.metadata.operation import (
    DerivedDataColumn,
    NodeOutputCategory,
    OperationStructure,
    OperationStructureBranchState,
    OperationStructureInfo,
    PostAggregationColumn,
)
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerationConfig,
    CodeGenerationContext,
    ExpressionStr,
    InfoDict,
    RightHandSide,
    StatementT,
    ValueStr,
    VariableNameGenerator,
    VarNameExpressionInfo,
)
from featurebyte.query_graph.node.scalar import ValueParameterType


class BaseFunctionParameterInput(BaseModel):
    """BaseFunctionParameterInput class"""

    dtype: DBVarType

    @abstractmethod
    def get_column_args(self) -> List[Optional[str]]:
        """
        Get column arguments (column name or None) for the function parameter input.
        If the input is a value, return empty list.

        Returns
        _______
        List[Optional[str]]
        """

    @abstractmethod
    def get_sdk_function_argument(
        self, node_inputs: List[VarNameExpressionInfo], node_input_index: int
    ) -> Tuple[RightHandSide, int]:
        """
        Get SDK function argument for the function parameter input.

        Parameters
        ----------
        node_inputs : List[VarNameExpressionInfo]
            List of node inputs.
        node_input_index : int
            The index of the node input that the function parameter input is associated with.

        Returns
        -------
        Tuple[RightHandSide, int]
            Tuple of SDK function argument and the updated node input index.
        """


class ValueFunctionParameterInput(BaseFunctionParameterInput):
    """ValueFunctionParameterInput class"""

    value: Optional[ValueParameterType]
    input_form: Literal[FunctionParameterInputForm.VALUE] = Field(
        FunctionParameterInputForm.VALUE, const=True
    )

    def get_column_args(self) -> List[Optional[str]]:
        return []

    def get_sdk_function_argument(
        self, node_inputs: List[VarNameExpressionInfo], node_input_index: int
    ) -> Tuple[RightHandSide, int]:
        # do not increment node_input_index as this is a value input, it should not increment the node_input_index
        return ValueStr.create(self.value), node_input_index


class ColumnFunctionParameterInput(BaseFunctionParameterInput):
    """ColumnFunctionParameterInput class"""

    column_name: Optional[str]
    input_form: Literal[FunctionParameterInputForm.COLUMN] = Field(
        FunctionParameterInputForm.COLUMN, const=True
    )

    def get_column_args(self) -> List[Optional[str]]:
        return [self.column_name]

    def get_sdk_function_argument(
        self, node_inputs: List[VarNameExpressionInfo], node_input_index: int
    ) -> Tuple[RightHandSide, int]:
        # InfoDict is not expected here as it should be used only in (ConditionNode - AssignNode) structure
        value = node_inputs[node_input_index]
        assert not isinstance(value, InfoDict), "Unexpected InfoDict type"
        # increment node_input_index as this is a column input, it should increment the node_input_index
        return value, node_input_index + 1


FunctionParameterInput = Annotated[
    Union[ValueFunctionParameterInput, ColumnFunctionParameterInput],
    Field(discriminator="input_form"),
]


class GenericFunctionNodeParameters(BaseModel):
    """GenericFunctionNodeParameters class"""

    function_name: str
    function_parameters: List[FunctionParameterInput]
    output_dtype: DBVarType
    function_id: PydanticObjectId


class GenericFunctionNode(BaseSeriesOutputNode):
    """GenericFunctionNode class"""

    type: Literal[NodeType.GENERIC_FUNCTION] = Field(NodeType.GENERIC_FUNCTION, const=True)
    parameters: GenericFunctionNodeParameters

    def _get_column_function_args(self) -> List[Optional[str]]:
        column_input_args = []
        for func_arg in self.parameters.function_parameters:
            column_input_args.extend(func_arg.get_column_args())
        return column_input_args

    @property
    def max_input_count(self) -> int:
        return len(self._get_column_function_args())

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        column_input_args = self._get_column_function_args()
        if column_input_args[input_index] is None:
            return []
        return [column_input_args[input_index]]  # type: ignore

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return self.parameters.output_dtype

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        input_category = inputs[0].output_category
        row_index_lineage = inputs[0].row_index_lineage
        for input_operation_structure in inputs:
            # check input category type is homogeneous
            if input_operation_structure.output_category != input_category:
                raise ValueError("Input category type is not homogeneous")
            if input_operation_structure.output_type != NodeOutputType.SERIES:
                raise ValueError("Input type is not series")
            if input_operation_structure.row_index_lineage != row_index_lineage:
                raise ValueError("Input row index is not matched")

        # prepare node parameters
        columns = []
        aggregations = []
        for input_operation_structure in inputs:
            columns.extend(input_operation_structure.columns)
            aggregations.extend(input_operation_structure.aggregations)

        node_kwargs: Dict[str, Any] = {"columns": []}
        if input_category == NodeOutputCategory.VIEW:
            node_kwargs["columns"] = [
                DerivedDataColumn.create(
                    name=None,
                    columns=columns,
                    transform=self.parameters.function_name,
                    node_name=self.name,
                    dtype=self.parameters.output_dtype,
                )
            ]
        else:
            node_kwargs["columns"] = columns
            node_kwargs["aggregations"] = [
                PostAggregationColumn.create(
                    name=None,
                    columns=aggregations,
                    transform=self.parameters.function_name,
                    node_name=self.name,
                    dtype=self.parameters.output_dtype,
                )
            ]

        return OperationStructure(
            **node_kwargs,
            output_type=NodeOutputType.SERIES,
            output_category=input_category,
            row_index_lineage=row_index_lineage,
        )

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        function_parameters: List[Any] = []
        node_input_index = 0
        for func_param in self.parameters.function_parameters:
            func_param_val, node_input_index = func_param.get_sdk_function_argument(
                node_inputs, node_input_index
            )
            function_parameters.append(func_param_val)

        function_parameters = [str(arg) for arg in function_parameters]
        expression = f"{self.parameters.function_name}({', '.join(function_parameters)})"
        return [], ExpressionStr(expression)
