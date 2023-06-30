"""
This module contains generic function related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple

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
    StatementT,
    ValueStr,
    VariableNameGenerator,
    VarNameExpressionInfo,
)


class FunctionParameterInput(BaseModel):
    """FunctionParameterInput class"""

    value: Optional[Any]
    dtype: DBVarType
    column_name: Optional[str]
    input_form: FunctionParameterInputForm


class GenericFunctionNodeParameters(BaseModel):
    """GenericFunctionNodeParameters class"""

    function_name: str
    function_parameters: List[FunctionParameterInput]
    output_dtype: DBVarType
    function_id: Optional[PydanticObjectId]


class GenericFunctionNode(BaseSeriesOutputNode):
    """GenericFunctionNode class"""

    type: Literal[NodeType.GENERIC_FUNCTION] = Field(NodeType.GENERIC_FUNCTION, const=True)
    parameters: GenericFunctionNodeParameters

    def _get_column_function_args(self) -> List[Optional[str]]:
        column_input_args = []
        for func_arg in self.parameters.function_parameters:
            if func_arg.input_form == FunctionParameterInputForm.COLUMN:
                column_input_args.append(func_arg.column_name)
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
        node_input_count = 0
        for func_param in self.parameters.function_parameters:
            if func_param.input_form == FunctionParameterInputForm.COLUMN:
                function_parameters.append(node_inputs[node_input_count])
                node_input_count += 1
            else:
                function_parameters.append(ValueStr.create(func_param.value))

        function_parameters = [str(arg) for arg in function_parameters]
        expression = f"{self.parameters.function_name}({', '.join(function_parameters)})"
        return [], ExpressionStr(expression)
