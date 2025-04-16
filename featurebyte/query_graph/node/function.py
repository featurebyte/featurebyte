"""
This module contains generic function related node classes
"""

# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from abc import abstractmethod
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union, cast

from pydantic import Field
from typing_extensions import Annotated, Literal

from featurebyte.enum import DBVarType, FunctionParameterInputForm
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
from featurebyte.query_graph.node.base import BaseSeriesOutputNode
from featurebyte.query_graph.node.metadata.config import (
    OnDemandFunctionCodeGenConfig,
    SDKCodeGenConfig,
)
from featurebyte.query_graph.node.metadata.operation import (
    DerivedDataColumn,
    NodeOutputCategory,
    OperationStructure,
    OperationStructureInfo,
    PostAggregationColumn,
)
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    CodeGenerationContext,
    CommentStr,
    ExpressionStr,
    InfoDict,
    NodeCodeGenOutput,
    ObjectClass,
    StatementT,
    VariableNameGenerator,
    VarNameExpressionInfo,
    VarNameExpressionStr,
    get_object_class_from_function_call,
)
from featurebyte.query_graph.node.scalar import TimestampValue, ValueParameterType
from featurebyte.typing import Scalar

SDKFunctionArgument = Union[VarNameExpressionInfo, Scalar, ObjectClass]


class BaseFunctionParameterInput(FeatureByteBaseModel):
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
        self, node_inputs: List[VarNameExpressionStr], node_input_index: int
    ) -> Tuple[SDKFunctionArgument, int]:
        """
        Get SDK function argument for the function parameter input.

        Parameters
        ----------
        node_inputs : List[VarNameExpressionStr]
            List of node inputs.
        node_input_index : int
            The index of the node input that the function parameter input is associated with.

        Returns
        -------
        Tuple[SDKFunctionArgument, int]
            Tuple of SDK function argument and the updated node input index.
        """


class ValueFunctionParameterInput(BaseFunctionParameterInput):
    """ValueFunctionParameterInput class"""

    value: Optional[ValueParameterType] = Field(default=None)
    input_form: Literal[FunctionParameterInputForm.VALUE] = FunctionParameterInputForm.VALUE

    def get_column_args(self) -> List[Optional[str]]:
        return []

    def get_sdk_function_argument(
        self, node_inputs: List[VarNameExpressionStr], node_input_index: int
    ) -> Tuple[SDKFunctionArgument, int]:
        # do not increment node_input_index as this is a value input, it should not increment the node_input_index
        if isinstance(self.value, TimestampValue):
            return ClassEnum.PD_TIMESTAMP(self.value.iso_format_str), node_input_index
        if isinstance(self.value, (list, tuple)):
            raise NotImplementedError("List value is not supported yet")
        return cast(Scalar, self.value), node_input_index


class ColumnFunctionParameterInput(BaseFunctionParameterInput):
    """ColumnFunctionParameterInput class"""

    column_name: Optional[str] = Field(default=None)
    input_form: Literal[FunctionParameterInputForm.COLUMN] = FunctionParameterInputForm.COLUMN

    def get_column_args(self) -> List[Optional[str]]:
        return [self.column_name]

    def get_sdk_function_argument(
        self, node_inputs: List[VarNameExpressionStr], node_input_index: int
    ) -> Tuple[SDKFunctionArgument, int]:
        # InfoDict is not expected here as it should be used only in (ConditionNode - AssignNode) structure
        value = node_inputs[node_input_index]
        assert not isinstance(value, InfoDict), "Unexpected InfoDict type"
        # increment node_input_index as this is a column input, it should increment the node_input_index
        return value, node_input_index + 1


FunctionParameterInput = Annotated[
    Union[ValueFunctionParameterInput, ColumnFunctionParameterInput],
    Field(discriminator="input_form"),
]


class GenericFunctionNodeParameters(FeatureByteBaseModel):
    """GenericFunctionNodeParameters class"""

    name: str
    sql_function_name: str
    function_parameters: List[FunctionParameterInput]
    output_dtype: DBVarType
    function_id: PydanticObjectId


class GenericFunctionNode(BaseSeriesOutputNode):
    """GenericFunctionNode class"""

    type: Literal[NodeType.GENERIC_FUNCTION] = NodeType.GENERIC_FUNCTION
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

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=self.parameters.output_dtype)

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
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
            if (
                input_category == NodeOutputCategory.VIEW
                and input_operation_structure.row_index_lineage != row_index_lineage
            ):
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
                    transform=self.parameters.name,
                    node_name=self.name,
                    dtype_info=DBVarTypeInfo(dtype=self.parameters.output_dtype),
                )
            ]
        else:
            node_kwargs["columns"] = columns
            node_kwargs["aggregations"] = [
                PostAggregationColumn.create(
                    name=None,
                    columns=aggregations,
                    transform=self.parameters.name,
                    node_name=self.name,
                    dtype_info=DBVarTypeInfo(dtype=self.parameters.output_dtype),
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
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        node_inps = self._assert_no_info_dict(node_inputs)
        function_parameters: List[Any] = []
        node_input_index = 0
        for func_param in self.parameters.function_parameters:
            func_param_val, node_input_index = func_param.get_sdk_function_argument(
                node_inps, node_input_index
            )
            function_parameters.append(func_param_val)

        # if function_id in var_name_generator.func_id_to_var_name,
        # it means there is a udf variable name is corresponding to the function_id.
        statements: List[StatementT] = []
        to_retrieve_udf = self.parameters.function_id not in var_name_generator.func_id_to_var_name
        udf_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix=f"udf_{self.parameters.name}",
            node_name=None,
            function_id=self.parameters.function_id,
        )
        if to_retrieve_udf:
            # to retrieve udf if the udf variable name is not in the scope
            comment = CommentStr(
                f"udf_name: {self.parameters.name}, sql_function_name: {self.parameters.sql_function_name}"
            )
            statements.append(comment)
            function_id = ClassEnum.OBJECT_ID(self.parameters.function_id)
            udf = ClassEnum.USER_DEFINED_FUNCTION(function_id, _method_name="get_by_id")
            statements.append((udf_var_name, udf))

        # construct output of current node
        out_var_name = var_name_generator.generate_variable_name(
            node_output_category=operation_structure.output_category,
            node_output_type=operation_structure.output_type,
            node_name=self.name,
        )
        expression = get_object_class_from_function_call(udf_var_name, *function_parameters)
        statements.append((out_var_name, expression))
        return statements, out_var_name

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        if config.to_generate_null_filling_function:
            # NOTE: We make an assumption that all UDFs does not have null filling capability,
            # any null input(s) will result in null output.
            if self.parameters.output_dtype in DBVarType.supported_timestamp_types():
                return [], ExpressionStr("pd.NaT")
            return [], ExpressionStr("np.nan")
        raise NotImplementedError("User defined function is not supported for GenericFunctionNode")
