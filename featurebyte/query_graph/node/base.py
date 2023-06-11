"""
Base classes required for constructing query graph nodes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Any, ClassVar, Dict, List, Optional, Sequence, Tuple, Type, TypeVar, Union

from abc import ABC, abstractmethod

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.metadata.column import InColumnStr, OutColumnStr
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
    StatementT,
    ValueStr,
    VariableNameGenerator,
    VariableNameStr,
    VarNameExpressionInfo,
    VarNameExpressionStr,
)
from featurebyte.query_graph.node.scalar import ValueParameterType

NODE_TYPES = []
NodeT = TypeVar("NodeT", bound="BaseNode")


class BaseNodeParameters(BaseModel):
    """
    BaseNodeParameters class
    """

    class Config:
        """Model configuration"""

        # cause validation to fail if extra attributes are included (https://docs.pydantic.dev/usage/model_config/)
        extra = "forbid"


class BaseNode(BaseModel):
    """
    BaseNode class
    """

    name: str
    type: NodeType
    output_type: NodeOutputType
    parameters: BaseModel

    # class variables
    # _auto_convert_expression_to_variable: when the expression is long, it will convert to a new
    # variable to limit the line width of the generated SDK code.
    _auto_convert_expression_to_variable: ClassVar[bool] = True

    class Config:
        """Model configuration"""

        extra = "forbid"

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

        # make sure subclass set certain properties correctly
        assert self.__fields__["type"].field_info.const is True
        assert repr(self.__fields__["type"].type_).startswith("typing.Literal")
        assert self.__fields__["output_type"].type_ is NodeOutputType

    def __init_subclass__(cls, **kwargs: Any):
        if repr(cls.__fields__["type"].type_).startswith("typing.Literal"):
            # only add node type class to NODE_TYPES if the type variable is a literal (to filter out base classes)
            NODE_TYPES.append(cls)

    @property
    def transform_info(self) -> str:
        """
        Construct from node transform object from this node

        Returns
        -------
        str
        """
        parameters = sorted(
            f"{key}='{value}'" if isinstance(value, str) else f"{key}={value}"
            for key, value in self.parameters.dict().items()
            if value
        )
        if parameters and len(parameters) < 4:
            # Note: 4 is chosen here so that the info is more readable, with too many
            # parameters presented here, it is hard to read. This value currently is only
            # used for the signal type tagging (for feature theme).
            return f"{str(self.type).lower()}({', '.join(parameters)})"
        return str(self.type).lower()

    @property
    def is_inplace_operation_in_sdk_code(self) -> bool:
        """
        Check if this node is an inplace operation in SDK code. For example, if the SDK code generated
        for this node is `view['new_col'] = 1`, then this node is an inplace operation as it will modify
        the input view object inplace. If the SDK code generated for this node is something like
        `joined_view = view.join_event_table_attributes(["col_float"])`, then this node is not an inplace
        operation as it will not modify the input view object inplace.

        Returns
        -------
        bool
        """
        return False

    @staticmethod
    def _assert_no_info_dict(inputs: List[VarNameExpressionInfo]) -> List[VarNameExpressionStr]:
        """
        Assert there is no info dict in the given inputs & convert the type to VarNameExpressionStr

        Parameters
        ----------
        inputs: List[VarNameExpressionInfoStr]
            List of inputs

        Returns
        -------
        List[VarNameExpressionStr]
        """
        out = []
        for input_ in inputs:
            assert not isinstance(input_, InfoDict)
            out.append(input_)
        return out

    @classmethod
    def detect_var_type_from_value(cls, value: Any) -> DBVarType:
        """
        Detect variable type of the given scalar value

        Parameters
        ----------
        value: Any
            Input value

        Returns
        -------
        DBVarType
        """
        if isinstance(value, bool):
            return DBVarType.BOOL
        if isinstance(value, int):
            return DBVarType.INT
        if isinstance(value, float):
            return DBVarType.FLOAT
        if isinstance(value, str):
            return DBVarType.VARCHAR
        return DBVarType.UNKNOWN

    @classmethod
    def _extract_column_str_values(
        cls,
        values: Any,
        column_str_type: Union[Type[InColumnStr], Type[OutColumnStr]],
    ) -> List[str]:
        out = set()
        if isinstance(values, dict):
            for val in values.values():
                if isinstance(val, column_str_type):
                    out.add(str(val))
                if isinstance(val, (dict, list)):
                    out.update(cls._extract_column_str_values(val, column_str_type))
        if isinstance(values, list):
            for val in values:
                if isinstance(val, column_str_type):
                    out.add(str(val))
                if isinstance(val, (dict, list)):
                    out.update(cls._extract_column_str_values(val, column_str_type))
        return list(out)

    def get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        """
        Get the required input column names for the given input based on this node parameters.
        For example, a JoinNode will consume two input node and inside the JoinNode parameters,
        some columns are referenced from the first input node and some are referenced from the
        second input node. When the input_order is 0, this method will return the column names
        from the first input node. When the input_order is 1, this method will return the column
        names from the second input node.

        Parameters
        ----------
        input_index: int
            This parameter is used to specify which input to get the required columns
        available_column_names: List[str]
            List of available column names

        Returns
        -------
        Sequence[str]
            When the output is empty, it means this node does not have any column name requirement
            for the given input index.
        """
        self._validate_get_required_input_columns_input_index(input_index)
        return self._get_required_input_columns(input_index, available_column_names)

    @abstractmethod
    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        """
        Helper method for get_required_input_columns

        Parameters
        ----------
        input_index: int
            This parameter is used to specify which input to get the required columns
        available_column_names: List[str]
            List of input available columns

        Returns
        -------
        Sequence[str]
        """

    def derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        """
        Derive node operation info

        Parameters
        ----------
        inputs: List[OperationStructure]
            List of input nodes' operation info
        branch_state: OperationStructureBranchState
            State captures the graph branching state info
        global_state: OperationStructureInfo
            State captures the global graph info (used during operation structure derivation)

        Returns
        -------
        OperationStructure
        """
        operation_info = self._derive_node_operation_info(
            inputs=inputs, branch_state=branch_state, global_state=global_state
        )
        if operation_info.columns or operation_info.aggregations:
            # make sure node name should be included in the node operation info
            assert self.name in operation_info.all_node_names

        # update is_time_based based on the inputs, or if the derive_node_operation_info returns true
        operation_info.is_time_based = (
            any(input_.is_time_based for input_ in inputs) or operation_info.is_time_based
        )
        return operation_info

    def derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        """
        Derive SDK codes based on the graph traversal from starting node(s) to this node

        Parameters
        ----------
        node_inputs: List[VarNameExpressionStr]
            Node inputs to derive SDK code
        var_name_generator: VariableNameGenerator
            Variable name generator
        operation_structure: OperationStructure
            Operation structure of current node
        config: CodeGenerationConfig
            Code generation configuration
        context: CodeGenerationContext
            Context for code generation

        Returns
        -------
        Tuple[List[StatementT], VarNameExpressionStr]
        """
        statements, var_name_expression_info = self._derive_sdk_code(
            node_inputs=node_inputs,
            var_name_generator=var_name_generator,
            operation_structure=operation_structure,
            config=config,
            context=context,
        )

        if (
            self._auto_convert_expression_to_variable
            and isinstance(var_name_expression_info, ExpressionStr)
            and len(var_name_expression_info) > config.max_expression_length
        ):
            # if the output of the var_name_expression is an expression and
            # the length of expression exceeds limit specified in code generation config,
            # then assign a new variable to reduce line width.
            var_name = var_name_generator.generate_variable_name(
                node_output_type=operation_structure.output_type,
                node_output_category=operation_structure.output_category,
                node_name=self.name,
            )
            statements.append((var_name, var_name_expression_info))
            return statements, var_name
        return statements, var_name_expression_info

    def clone(self: NodeT, **kwargs: Any) -> NodeT:
        """
        Clone an existing object with certain update

        Parameters
        ----------
        kwargs: Any
            Keyword parameters to overwrite existing object

        Returns
        -------
        NodeT
        """
        return type(self)(**{**self.dict(), **kwargs})

    def prune(
        self: NodeT,
        target_node_input_order_pairs: Sequence[Tuple[NodeT, int]],
        input_operation_structures: List[OperationStructure],
    ) -> NodeT:
        """
        Prune this node parameters based on target nodes

        Parameters
        ----------
        target_node_input_order_pairs: Sequence[Tuple[BaseNode, int]]
            List of target nodes
        input_operation_structures: List[OperationStructure]
            List of input operation structures

        Returns
        -------
        NodeT
        """
        _ = target_node_input_order_pairs, input_operation_structures
        return self

    def _convert_expression_to_variable(
        self,
        var_name_expression: VarNameExpressionStr,
        var_name_generator: VariableNameGenerator,
        node_output_type: NodeOutputType,
        node_output_category: NodeOutputCategory,
        to_associate_with_node_name: bool,
        variable_name_prefix: Optional[str] = None,
    ) -> Tuple[List[StatementT], VariableNameStr]:
        """
        Convert expression to variable

        Parameters
        ----------
        var_name_expression: VarNameExpressionStr
            Variable name expression
        var_name_generator: VariableNameGenerator
            Variable name generator
        node_output_type: NodeOutputType
            Node output type
        node_output_category: NodeOutputCategory
            Node output category
        to_associate_with_node_name: bool
            Whether to associate the variable name with the node name
        variable_name_prefix: Optional[str]
            Variable name prefix (if any)

        Returns
        -------
        VarNameStr
        """
        statements: List[StatementT] = []
        if isinstance(var_name_expression, ExpressionStr):
            if variable_name_prefix:
                var_name = var_name_generator.convert_to_variable_name(
                    variable_name_prefix=variable_name_prefix,
                    node_name=self.name if to_associate_with_node_name else None,
                )
            else:
                var_name = var_name_generator.generate_variable_name(
                    node_output_type=node_output_type,
                    node_output_category=node_output_category,
                    node_name=self.name if to_associate_with_node_name else None,
                )
            statements.append((var_name, var_name_expression))
            return statements, var_name
        return statements, var_name_expression

    def _convert_to_proper_variable_name(
        self,
        var_name: VariableNameStr,
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        required_copy: bool,
        to_associate_with_node_name: bool,
    ) -> Tuple[List[StatementT], VariableNameStr]:
        """
        This method is used to convert variable name to proper variable name if the variable name is
        not a valid identifier.

        Parameters
        ----------
        var_name: VariableNameStr
            Variable name
        var_name_generator: VariableNameGenerator
            Variable name generator
        operation_structure: OperationStructure
            Operation structure of current node
        required_copy: bool
            Whether a copy is required
        to_associate_with_node_name: bool
            Whether to associate the variable name with the node name

        Returns
        -------
        Tuple[List[StatementT], VariableNameStr]
        """
        output_var_name = var_name
        statements: List[StatementT] = []
        is_var_name_valid_identifier = var_name.isidentifier()
        if required_copy or not is_var_name_valid_identifier:
            output_var_name = var_name_generator.generate_variable_name(
                node_output_type=operation_structure.output_type,
                node_output_category=operation_structure.output_category,
                node_name=self.name if to_associate_with_node_name else None,
            )
            if required_copy:
                # Copy is required as the input will be used by other nodes. This is to avoid unexpected
                # side effects when the input is modified by other nodes.
                statements.append((output_var_name, ExpressionStr(f"{var_name}.copy()")))
            else:
                # This is to handle the case where the var_name is not a valid variable name,
                # so we need to assign it to a valid variable name first.
                statements.append((output_var_name, ExpressionStr(var_name)))
        return statements, output_var_name

    @abstractmethod
    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        """
        Derive node operation info abstract method to be implemented at the concrete node class

        Parameters
        ----------
        inputs: List[OperationStructure]
            List of input nodes' operation info
        branch_state: OperationStructureBranchState
            State captures the graph branching state info
        global_state: OperationStructureInfo
            State captures the global graph info (used during operation structure derivation)

        Returns
        -------
        OperationStructure
        """

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        """
        Derive SDK codes based to be implemented at the concrete node class

        Parameters
        ----------
        node_inputs: List[VarNameExpression]
            Inputs for this node to generate SDK codes
        var_name_generator: VariableNameGenerator
            Variable name generator
        operation_structure: OperationStructure
            Operation structure of current node
        config: CodeGenerationConfig
            Code generation configuration
        context: CodeGenerationContext
            Context for code generation

        Returns
        -------
        Tuple[List[StatementT], VarNameExpression]
        """
        # TODO: convert this method to an abstract method and remove the following dummy implementation
        _ = var_name_generator, operation_structure, config, context
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        input_params = ", ".join(var_name_expressions)
        expression = ExpressionStr(f"{self.type}({input_params})")
        return [], expression

    @property
    @abstractmethod
    def max_input_count(self) -> int:
        """
        Maximum number of inputs for this node

        Returns
        -------
        int
        """

    def _validate_get_required_input_columns_input_index(self, input_index: int) -> None:
        """
        Validate that input index value is within the correct range.

        Parameters
        ----------
        input_index: int
            Input index

        Raises
        ------
        ValueError
            If input index is out of range
        """
        if input_index < 0 or input_index >= self.max_input_count:
            raise ValueError(
                f"Input index {input_index} is out of range. "
                f"Input index should be within 0 to {self.max_input_count - 1} (node_name: {self.name})."
            )

    def _assert_empty_required_input_columns(self) -> Sequence[str]:
        """
        Assert empty required input columns and return emtpy list. This is used to check if the node
        parameters has any InColumnStr parameters. If yes, we should update get_required_input_columns
        method to reflect the required input columns.

        Returns
        -------
        Sequence[str]

        Raises
        ------
        AssertionError
            If required input columns is not empty
        """
        input_columns = self._extract_column_str_values(self.parameters.dict(), InColumnStr)
        assert len(input_columns) == 0
        return input_columns


class SeriesOutputNodeOpStructMixin:
    """SeriesOutputNodeOpStructMixin class"""

    name: str
    transform_info: str
    output_type: NodeOutputType

    @abstractmethod
    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        """
        Derive variable type from the input operation structures

        Parameters
        ----------
        inputs: List[OperationStructure]
            Operation structures of the input nodes

        Returns
        -------
        DBVarType
        """

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        """
        Derive node operation info

        Parameters
        ----------
        inputs: List[OperationStructure]
            List of input nodes' operation info
        branch_state: OperationStructureBranchState
            State captures the graph branching state info
        global_state: OperationStructureInfo
            State captures the global graph info (used during operation structure derivation)

        Returns
        -------
        OperationStructure
        """
        _ = branch_state, global_state
        input_operation_info = inputs[0]
        output_category = input_operation_info.output_category
        columns = []
        aggregations = []
        for inp in inputs:
            columns.extend(inp.columns)
            aggregations.extend(inp.aggregations)

        node_kwargs: Dict[str, Any] = {}
        if output_category == NodeOutputCategory.VIEW:
            node_kwargs["columns"] = [
                DerivedDataColumn.create(
                    name=None,
                    columns=columns,
                    transform=self.transform_info,
                    node_name=self.name,
                    dtype=self.derive_var_type(inputs),
                )
            ]
        else:
            node_kwargs["columns"] = columns
            node_kwargs["aggregations"] = [
                PostAggregationColumn.create(
                    name=None,
                    columns=aggregations,
                    transform=self.transform_info,
                    node_name=self.name,
                    dtype=self.derive_var_type(inputs),
                )
            ]

        return OperationStructure(
            **node_kwargs,
            output_type=self.output_type,
            output_category=output_category,
            row_index_lineage=input_operation_info.row_index_lineage,
        )


class BaseSeriesOutputNode(SeriesOutputNodeOpStructMixin, BaseNode, ABC):
    """Base class for node produces series output"""

    output_type: NodeOutputType = Field(NodeOutputType.SERIES, const=True)
    parameters: BaseModel = Field(default=BaseModel(), const=True)


class SingleValueNodeParameters(BaseNodeParameters):
    """SingleValueNodeParameters"""

    value: Optional[ValueParameterType]


class ValueWithRightOpNodeParameters(SingleValueNodeParameters):
    """ValueWithRightOpNodeParameters"""

    right_op: bool = Field(default=False)


class BaseSeriesOutputWithAScalarParamNode(SeriesOutputNodeOpStructMixin, BaseNode, ABC):
    """Base class for node produces series output & contain a single scalar parameter"""

    output_type: NodeOutputType = Field(NodeOutputType.SERIES, const=True)
    parameters: SingleValueNodeParameters

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def _reorder_operands(self, left_operand: str, right_operand: str) -> Tuple[str, str]:
        _ = self
        return left_operand, right_operand

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        """
        Generate expression for the node

        Parameters
        ----------
        left_operand: str
            Left operand
        right_operand: str
            Right operand

        Returns
        -------
        str
        """
        # TODO: make this method abstract and remove the following dummy implementation
        _ = left_operand, right_operand
        return ""

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        left_operand: str = input_var_name_expressions[0].as_input()
        right_operand: str = ValueStr.create(self.parameters.value).as_input()
        if len(input_var_name_expressions) == 2:
            right_operand = input_var_name_expressions[1].as_input()
        left_operand, right_operand = self._reorder_operands(left_operand, right_operand)
        return [], ExpressionStr(self.generate_expression(left_operand, right_operand))


class BinaryLogicalOpNode(BaseSeriesOutputWithAScalarParamNode):
    """BinaryLogicalOpNode class"""

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL


class BinaryRelationalOpNode(BaseSeriesOutputWithAScalarParamNode):
    """BinaryRelationalOpNode class"""

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL


class BinaryArithmeticOpNode(BaseSeriesOutputWithAScalarParamNode):
    """BinaryArithmeticOpNode class"""

    parameters: ValueWithRightOpNodeParameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        input_var_types = {inp.series_output_dtype for inp in inputs}
        if DBVarType.FLOAT in input_var_types:
            return DBVarType.FLOAT
        return inputs[0].series_output_dtype

    def _reorder_operands(self, left_operand: str, right_operand: str) -> Tuple[str, str]:
        if self.parameters.right_op:
            return right_operand, left_operand
        return left_operand, right_operand


class BaseSeriesOutputWithSingleOperandNode(BaseSeriesOutputNode, ABC):
    """BaseSingleOperandNode class"""

    # class variable
    _derive_sdk_code_return_var_name_expression_type: ClassVar[
        Union[Type[VariableNameStr], Type[ExpressionStr]]
    ] = VariableNameStr

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    @abstractmethod
    def generate_expression(self, operand: str) -> str:
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

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name_expression = input_var_name_expressions[0]
        return [], self._derive_sdk_code_return_var_name_expression_type(
            self.generate_expression(var_name_expression.as_input())
        )


class BasePrunableNode(BaseNode):
    """Base class for node that can be pruned during query graph pruning"""

    @abstractmethod
    def resolve_node_pruned(self, input_node_names: List[str]) -> str:
        """
        Method used to resolve the situation when the node get pruned. As all the nodes only produce single
        output, we should only choose one node from the input nodes.

        Parameters
        ----------
        input_node_names: List[str]
            List of input node names

        Returns
        -------
        str
            Node name selected to replace this (pruned) node
        """
