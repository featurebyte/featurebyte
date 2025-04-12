"""
Base classes required for constructing query graph nodes
"""

# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition

import copy
from abc import ABC, abstractmethod
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from pydantic import ConfigDict, Field

from featurebyte.common.model_util import parse_duration_string
from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
from featurebyte.query_graph.node.metadata.column import InColumnStr, OutColumnStr
from featurebyte.query_graph.node.metadata.config import (
    BaseCodeGenConfig,
    OnDemandFunctionCodeGenConfig,
    OnDemandViewCodeGenConfig,
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
    ExpressionStr,
    InfoDict,
    NodeCodeGenOutput,
    StatementT,
    ValueStr,
    VariableNameGenerator,
    VariableNameStr,
    VarNameExpressionInfo,
    VarNameExpressionStr,
    get_object_class_from_function_call,
)
from featurebyte.query_graph.node.scalar import TimestampValue, ValueParameterType
from featurebyte.query_graph.util import hash_input_node_hashes

NODE_TYPES = []
NodeT = TypeVar("NodeT", bound="BaseNode")


class BaseNodeParameters(FeatureByteBaseModel):
    """
    BaseNodeParameters class
    """

    # pydantic model configuration
    model_config = ConfigDict(
        validate_assignment=True,
        use_enum_values=True,
        arbitrary_types_allowed=True,
        extra="forbid",
    )


class BaseNode(FeatureByteBaseModel):
    """
    BaseNode class
    """

    name: str
    type: NodeType
    output_type: NodeOutputType
    parameters: FeatureByteBaseModel

    # class variables
    # _auto_convert_expression_to_variable: when the expression is long, it will convert to a new
    # variable to limit the line width of the generated SDK code.
    _auto_convert_expression_to_variable: ClassVar[bool] = True
    # for generating feature definition hash
    # whether the node is commutative, i.e. the order of the inputs does not matter
    is_commutative: ClassVar[bool] = False
    # normalized output prefix is used to prefix the output column name when the node is normalized
    _normalized_output_prefix: ClassVar[str] = ""
    # whether the node should inherit the first input column name mapping as the output column name mapping
    _inherit_first_input_column_name_mapping: ClassVar[bool] = False
    # window parameter field name used to normalize the window parameter
    _window_parameter_field_name: ClassVar[Optional[str]] = None
    # nested parameter field names to be normalized
    _normalize_nested_parameter_field_names: ClassVar[Optional[List[str]]] = None

    # pydantic model configuration
    model_config = ConfigDict(extra="forbid")

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

        # make sure subclass set certain properties correctly
        assert "Literal" in repr(self.model_fields["type"].annotation)

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        if "Literal" in repr(cls.model_fields["type"].annotation):
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
            for key, value in self.parameters.model_dump().items()
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
    def _assert_no_info_dict(inputs: List[NodeCodeGenOutput]) -> List[VarNameExpressionStr]:
        """
        Assert there is no info dict in the given inputs & convert the type to VarNameExpressionStr

        Parameters
        ----------
        inputs: List[NodeCodeGenOutput]
            List of inputs

        Returns
        -------
        List[VarNameExpressionStr]
        """
        out: List[VarNameExpressionStr] = []
        for input_ in inputs:
            assert not isinstance(input_.var_name_or_expr, InfoDict)
            out.append(input_.var_name_or_expr)
        return out

    @classmethod
    def detect_dtype_info_from_value(cls, value: Any) -> DBVarTypeInfo:
        """
        Detect variable type of the given scalar value

        Parameters
        ----------
        value: Any
            Input value

        Returns
        -------
        DBVarTypeInfo
        """
        if isinstance(value, bool):
            return DBVarTypeInfo(dtype=DBVarType.BOOL)
        if isinstance(value, int):
            return DBVarTypeInfo(dtype=DBVarType.INT)
        if isinstance(value, float):
            return DBVarTypeInfo(dtype=DBVarType.FLOAT)
        if isinstance(value, str):
            return DBVarTypeInfo(dtype=DBVarType.VARCHAR)
        return DBVarTypeInfo(dtype=DBVarType.UNKNOWN)

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
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        """
        Derive node operation info

        Parameters
        ----------
        inputs: List[OperationStructure]
            List of input nodes' operation info
        global_state: OperationStructureInfo
            State captures the global graph info (used during operation structure derivation)

        Returns
        -------
        OperationStructure
        """
        operation_info = self._derive_node_operation_info(inputs=inputs, global_state=global_state)
        if operation_info.columns or operation_info.aggregations:
            # make sure node name should be included in the node operation info
            assert self.name in operation_info.all_node_names

        # update is_time_based based on the inputs, or if the derive_node_operation_info returns true
        operation_info.is_time_based = (
            any(input_.is_time_based for input_ in inputs) or operation_info.is_time_based
        )
        return operation_info

    def _handle_statement_line_width(
        self,
        var_name_generator: VariableNameGenerator,
        statements: List[StatementT],
        var_name_expression_info: VarNameExpressionInfo,
        config: BaseCodeGenConfig,
        operation_structure: Optional[OperationStructure],
        variable_name_prefix: Optional[str],
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        assert (
            operation_structure or variable_name_prefix
        ), "Either operation_structure or variable_name_prefix should be provided"

        if (
            self._auto_convert_expression_to_variable
            and isinstance(var_name_expression_info, ExpressionStr)
            and len(var_name_expression_info) > config.max_expression_length
        ):
            # if the output of the var_name_expression is an expression and
            # the length of expression exceeds limit specified in code generation config,
            # then assign a new variable to reduce line width.
            if operation_structure:
                var_name = var_name_generator.generate_variable_name(
                    node_output_type=operation_structure.output_type,
                    node_output_category=operation_structure.output_category,
                    node_name=self.name,
                )
            else:
                assert variable_name_prefix is not None
                var_name = var_name_generator.convert_to_variable_name(
                    variable_name_prefix=variable_name_prefix, node_name=self.name
                )

            statements.append((var_name, var_name_expression_info))
            return statements, var_name
        return statements, var_name_expression_info

    def derive_sdk_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        """
        Derive SDK codes based on the graph traversal from starting node(s) to this node

        Parameters
        ----------
        node_inputs: List[NodeCodeGenOutput]
            Node inputs to derive SDK code
        var_name_generator: VariableNameGenerator
            Variable name generator
        operation_structure: OperationStructure
            Operation structure of current node
        config: SDKCodeGenConfig
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
        return self._handle_statement_line_width(
            var_name_generator=var_name_generator,
            statements=statements,
            var_name_expression_info=var_name_expression_info,
            config=config,
            operation_structure=operation_structure,
            variable_name_prefix=None,
        )

    def derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        """
        Derive Feast on demand feature view code based on the graph traversal from starting node(s) to this node

        Parameters
        ----------
        node_inputs: List[NodeCodeGenOutput]
            Node inputs to derive on demand view code
        var_name_generator: VariableNameGenerator
            Variable name generator
        config: OnDemandViewCodeGenConfig
            Code generation configuration

        Returns
        -------
        Tuple[List[StatementT], VarNameExpressionStr]
        """
        statements, var_name_expression_info = self._derive_on_demand_view_code(
            node_inputs=node_inputs,
            var_name_generator=var_name_generator,
            config=config,
        )
        return self._handle_statement_line_width(
            var_name_generator=var_name_generator,
            statements=statements,
            var_name_expression_info=var_name_expression_info,
            config=config,
            operation_structure=None,
            variable_name_prefix="feat",
        )

    def derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        """
        Derive DataBricks on demand function code based on the graph traversal from starting node(s) to this node

        Parameters
        ----------
        node_inputs: List[NodeCodeGenOutput]
            Node inputs to derive on demand function code
        var_name_generator: VariableNameGenerator
            Variable name generator
        config: OnDemandFunctionCodeGenConfig
            Code generation configuration

        Returns
        -------
        Tuple[List[StatementT], VarNameExpressionStr]
        """
        statements, var_name_expression_info = self._derive_user_defined_function_code(
            node_inputs=node_inputs,
            var_name_generator=var_name_generator,
            config=config,
        )
        return self._handle_statement_line_width(
            var_name_generator=var_name_generator,
            statements=statements,
            var_name_expression_info=var_name_expression_info,
            config=config,
            operation_structure=None,
            variable_name_prefix="feat",
        )

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
        return type(self)(**{**self.model_dump(), **kwargs})

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
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        """
        Derive node operation info abstract method to be implemented at the concrete node class

        Parameters
        ----------
        inputs: List[OperationStructure]
            List of input nodes' operation info
        global_state: OperationStructureInfo
            State captures the global graph info (used during operation structure derivation)

        Returns
        -------
        OperationStructure
        """

    def _derive_sdk_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        """
        Derive SDK codes to be implemented at the concrete node class

        Parameters
        ----------
        node_inputs: List[VarNameExpression]
            Inputs for this node to generate SDK codes
        var_name_generator: VariableNameGenerator
            Variable name generator
        operation_structure: OperationStructure
            Operation structure of current node
        config: SDKCodeGenConfig
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

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        """
        Derive Feast on demand feature view code to be implemented at the concrete node class

        Parameters
        ----------
        node_inputs: List[VarNameExpression]
            Inputs for this node to generate SDK codes
        var_name_generator: VariableNameGenerator
            Variable name generator
        config: OnDemandViewCodeGenConfig
            Code generation configuration

        Returns
        -------
        Tuple[List[StatementT], VarNameExpression]
        # noqa: DAR202

        Raises
        ------
        RuntimeError
            If this method is not supposed to be called
        # noqa: DAR401
        """
        raise RuntimeError(f"This method should not be called by {self.__class__.__name__}")

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        """
        Derive DataBricks on demand function code to be implemented at the concrete node class

        Parameters
        ----------
        node_inputs: List[VarNameExpression]
            Inputs for this node to generate SDK codes
        var_name_generator: VariableNameGenerator
            Variable name generator
        config: OnDemandFunctionCodeGenConfig
            Code generation configuration

        Returns
        -------
        Tuple[List[StatementT], VarNameExpression]
        # noqa: DAR202

        Raises
        ------
        RuntimeError
            If this method is not supposed to be called
        # noqa: DAR401
        """
        raise RuntimeError(f"This method should not be called by {self.__class__.__name__}")

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
        input_columns = self._extract_column_str_values(self.parameters.model_dump(), InColumnStr)
        assert len(input_columns) == 0
        return input_columns

    @staticmethod
    def _get_mapped_input_column(
        column_name: str, input_node_column_mappings: List[Dict[str, str]]
    ) -> str:
        for input_node_column_mapping in input_node_column_mappings:
            if column_name in input_node_column_mapping:
                return input_node_column_mapping[column_name]
        return column_name

    def _normalize_nested_parameters(
        self, nested_parameters: Dict[str, Any], input_node_column_mappings: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        output = copy.deepcopy(nested_parameters)
        for param_name, value in nested_parameters.items():
            if isinstance(value, InColumnStr):
                output[param_name] = self._get_mapped_input_column(
                    value, input_node_column_mappings
                )
            if isinstance(value, list) and all(isinstance(val, InColumnStr) for val in value):
                output[param_name] = [
                    self._get_mapped_input_column(val, input_node_column_mappings) for val in value
                ]
        return output

    def normalize_and_recreate_node(
        self: NodeT,
        input_node_hashes: List[str],
        input_node_column_mappings: List[Dict[str, str]],
    ) -> Tuple[NodeT, Dict[str, str]]:
        """
        This method is used to recreate a normalized node for the construction of the feature definition hash.
        Normalized node means that the node parameters are normalized to a certain format. For example,
        those user specified string parameters that do not affect the node operation will be replaced by
        a normalized string parameter. This method will also return the original column name to remapped
        column name mapping for the output node to use.

        Parameters
        ----------
        input_node_hashes: List[str]
            List of input node hashes
        input_node_column_mappings: List[Dict[str, str]]
            List of input node column mapping (original column name to normalized column name)

        Returns
        -------
        NodeT
            Normalized node
        Dict[str, str]
            Output column name to normalized column name mapping
        """
        if not input_node_column_mappings:
            # if the node does not have any input, then no need to remap the column
            return self, {}

        input_node_column_mapping = input_node_column_mappings[0]
        input_nodes_hash = hash_input_node_hashes(input_node_hashes)

        output_column_remap: Dict[str, str] = {}
        if self._inherit_first_input_column_name_mapping:
            output_column_remap.update(**input_node_column_mapping)

        node_params = self.parameters.model_dump(by_alias=True)
        for param_name, value in node_params.items():
            if isinstance(value, InColumnStr):
                node_params[param_name] = input_node_column_mapping.get(value, value)
            if isinstance(value, OutColumnStr):
                output_column_remap[value] = f"{self._normalized_output_prefix}{input_nodes_hash}"
                node_params[param_name] = output_column_remap[value]
            if isinstance(value, list) and all(isinstance(val, InColumnStr) for val in value):
                node_params[param_name] = [input_node_column_mapping.get(val, val) for val in value]
            if param_name == self._window_parameter_field_name:
                window_param = node_params[param_name]
                if isinstance(window_param, list):
                    windows = []
                    for window in window_param:
                        windows.append(f"{parse_duration_string(window)}s" if window else window)
                    node_params[param_name] = windows
                if isinstance(window_param, str):
                    node_params[param_name] = f"{parse_duration_string(window_param)}s"

            if (
                self._normalize_nested_parameter_field_names
                and param_name in self._normalize_nested_parameter_field_names
                and isinstance(value, dict)
            ):
                node_params[param_name] = self._normalize_nested_parameters(
                    nested_parameters=value, input_node_column_mappings=input_node_column_mappings
                )

        return self.clone(parameters=node_params), output_column_remap

    @staticmethod
    def _to_datetime_expr(
        var_name_expr: VarNameExpressionStr, to_handle_none: bool = True, **to_datetime_kwargs: Any
    ) -> ExpressionStr:
        """
        Convert input variable name expression to datetime expression

        Parameters
        ----------
        var_name_expr: VarNameExpressionStr
            Variable name expression
        to_handle_none: bool
            Whether to handle null values
        to_datetime_kwargs: Any
            Additional keyword arguments for pd.to_datetime function

        Returns
        -------
        ExpressionStr
        """
        to_dt_expr = get_object_class_from_function_call(
            "pd.to_datetime", var_name_expr, utc=True, **to_datetime_kwargs
        )
        if to_handle_none:
            return ExpressionStr(f"pd.NaT if {var_name_expr} is None else {to_dt_expr}")
        return ExpressionStr(to_dt_expr)


class SeriesOutputNodeOpStructMixin:
    """SeriesOutputNodeOpStructMixin class"""

    name: str
    transform_info: ClassVar[Callable[[], str]]
    output_type: NodeOutputType

    @abstractmethod
    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        """
        Derive variable type from the input operation structures

        Parameters
        ----------
        inputs: List[OperationStructure]
            Operation structures of the input nodes

        Returns
        -------
        DBVarTypeInfo
        """

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        """
        Derive node operation info

        Parameters
        ----------
        inputs: List[OperationStructure]
            List of input nodes' operation info
        global_state: OperationStructureInfo
            State captures the global graph info (used during operation structure derivation)

        Returns
        -------
        OperationStructure
        """
        _ = global_state
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
                    transform=self.transform_info,  # type: ignore
                    node_name=self.name,
                    dtype_info=self.derive_dtype_info(inputs),
                )
            ]
        else:
            node_kwargs["columns"] = columns
            node_kwargs["aggregations"] = [
                PostAggregationColumn.create(
                    name=None,
                    columns=aggregations,
                    transform=self.transform_info,  # type: ignore
                    node_name=self.name,
                    dtype_info=self.derive_dtype_info(inputs),
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

    output_type: NodeOutputType = NodeOutputType.SERIES
    parameters: FeatureByteBaseModel = Field(default_factory=FeatureByteBaseModel)


class SingleValueNodeParameters(BaseNodeParameters):
    """SingleValueNodeParameters"""

    value: Optional[ValueParameterType] = Field(default=None)


class ValueWithRightOpNodeParameters(SingleValueNodeParameters):
    """ValueWithRightOpNodeParameters"""

    right_op: bool = Field(default=False)


class BaseSeriesOutputWithAScalarParamNode(SeriesOutputNodeOpStructMixin, BaseNode, ABC):
    """Base class for node produces series output & contain a single scalar parameter"""

    output_type: NodeOutputType = NodeOutputType.SERIES
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

    def generate_odfv_expression(self, left_operand: str, right_operand: str) -> str:
        """
        Generate expression for the on demand feature view

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
        return self.generate_expression(left_operand, right_operand)

    def generate_udf_expression(self, left_operand: str, right_operand: str) -> str:
        """
        Generate expression for the DataBricks user defined function (UDF) for on demand feature

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
        return self.generate_expression(left_operand, right_operand)

    def _derive_python_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        to_timestamp_func: Union[ClassEnum, str],
        generate_expression_func: Callable[[str, str], str],
    ) -> Tuple[List[StatementT], ExpressionStr]:
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        left_operand: str = input_var_name_expressions[0].as_input()
        statements: List[StatementT] = []
        right_operand: Union[str, VariableNameStr]
        if isinstance(self.parameters.value, TimestampValue):
            timestamp_var = var_name_generator.convert_to_variable_name(
                variable_name_prefix="timestamp_value",
                node_name=self.name,
            )
            if isinstance(to_timestamp_func, ClassEnum):
                timestamp_val = to_timestamp_func(self.parameters.value.iso_format_str)
            else:
                timestamp_val = get_object_class_from_function_call(
                    to_timestamp_func, self.parameters.value.iso_format_str
                )

            statements.append((timestamp_var, timestamp_val))
            right_operand = timestamp_var
        else:
            right_operand = ValueStr.create(self.parameters.value).as_input()

        if len(input_var_name_expressions) == 2:
            right_operand = input_var_name_expressions[1].as_input()
        left_operand, right_operand = self._reorder_operands(left_operand, right_operand)
        return statements, ExpressionStr(generate_expression_func(left_operand, right_operand))

    def _derive_sdk_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = operation_structure, config, context
        return self._derive_python_code(
            node_inputs, var_name_generator, ClassEnum.PD_TIMESTAMP, self.generate_expression
        )

    def _generate_odfv_expression_with_null_value_handling_for_single_input(
        self, left_operand: str, right_operand: str
    ) -> str:
        expr = self.generate_odfv_expression(left_operand, right_operand)
        where_expr = f"np.where(pd.isna({left_operand}), np.nan, {expr})"
        return f"pd.Series({where_expr}, index={left_operand}.index)"

    def _generate_odfv_expression_with_null_value_handling(
        self, left_operand: str, right_operand: str
    ) -> str:
        expr = self.generate_odfv_expression(left_operand, right_operand)
        where_expr = f"np.where(pd.isna({left_operand}) | pd.isna({right_operand}), np.nan, {expr})"
        return f"pd.Series({where_expr}, index={left_operand}.index)"

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = config
        generate_expression_func = self._generate_odfv_expression_with_null_value_handling
        if len(node_inputs) == 1:
            generate_expression_func = (
                self._generate_odfv_expression_with_null_value_handling_for_single_input
            )
        return self._derive_python_code(
            node_inputs=node_inputs,
            var_name_generator=var_name_generator,
            to_timestamp_func="pd.Timestamp",
            generate_expression_func=generate_expression_func,
        )

    def _generate_udf_expression_with_null_value_handling_for_single_input(
        self, left_operand: str, right_operand: str
    ) -> str:
        expr = self.generate_udf_expression(left_operand, right_operand)
        return f"np.nan if pd.isna({left_operand}) else {expr}"

    def _generate_udf_expression_with_null_value_handling(
        self, left_operand: str, right_operand: str
    ) -> str:
        expr = self.generate_udf_expression(left_operand, right_operand)
        return f"np.nan if pd.isna({left_operand}) or pd.isna({right_operand}) else {expr}"

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = config
        generate_expression_func = self._generate_udf_expression_with_null_value_handling
        if len(node_inputs) == 1:
            generate_expression_func = (
                self._generate_udf_expression_with_null_value_handling_for_single_input
            )
        return self._derive_python_code(
            node_inputs=node_inputs,
            var_name_generator=var_name_generator,
            to_timestamp_func="pd.Timestamp",
            generate_expression_func=generate_expression_func,
        )


class BinaryOpWithBoolOutputNode(BaseSeriesOutputWithAScalarParamNode):
    """BinaryLogicalOpNode class"""

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.BOOL)

    def _generate_odfv_expression_with_null_value_handling_for_single_input(
        self, left_operand: str, right_operand: str
    ) -> str:
        # explicitly convert the result to bool type
        expr = super()._generate_odfv_expression_with_null_value_handling_for_single_input(
            left_operand, right_operand
        )
        return f"{expr}.apply(lambda x: np.nan if pd.isna(x) else bool(x))"

    def _generate_odfv_expression_with_null_value_handling(
        self, left_operand: str, right_operand: str
    ) -> str:
        # explicitly convert the result to bool type
        expr = super()._generate_odfv_expression_with_null_value_handling(
            left_operand, right_operand
        )
        return f"{expr}.apply(lambda x: np.nan if pd.isna(x) else bool(x))"


class BinaryArithmeticOpNode(BaseSeriesOutputWithAScalarParamNode):
    """BinaryArithmeticOpNode class"""

    parameters: ValueWithRightOpNodeParameters

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        input_var_types = {inp.series_output_dtype_info.dtype for inp in inputs}
        if DBVarType.FLOAT in input_var_types:
            return DBVarTypeInfo(dtype=DBVarType.FLOAT)
        return inputs[0].series_output_dtype_info

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

    def generate_odfv_expression(self, operand: str) -> str:
        """
        Generate expression for the on demand feature view

        Parameters
        ----------
        operand: str
            Left operand

        Returns
        -------
        str
        """
        return self.generate_expression(operand)

    def generate_udf_expression(self, operand: str) -> str:
        """
        Generate expression for the DataBricks user defined function (UDF) for on demand feature

        Parameters
        ----------
        operand: str
            Left operand

        Returns
        -------
        str
        """
        return self.generate_expression(operand)

    def _derive_sdk_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name_expression = input_var_name_expressions[0]
        return [], self._derive_sdk_code_return_var_name_expression_type(
            self.generate_expression(var_name_expression.as_input())
        )

    def _generate_odfv_expression_with_null_value_handling(self, operand: str) -> str:
        return self.generate_odfv_expression(operand)

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name_expression = input_var_name_expressions[0]
        expr = self._generate_odfv_expression_with_null_value_handling(
            var_name_expression.as_input()
        )
        return [], ExpressionStr(expr)

    def _generate_udf_expression_with_null_value_handling(self, operand: str) -> str:
        expr = self.generate_udf_expression(operand)
        return f"np.nan if pd.isna({operand}) else {expr}"

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name_expression = input_var_name_expressions[0]
        expr = self._generate_udf_expression_with_null_value_handling(
            var_name_expression.as_input()
        )
        return [], ExpressionStr(expr)


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
