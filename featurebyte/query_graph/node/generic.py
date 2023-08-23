"""
This module contains SQL operation related node classes
"""
# pylint: disable=too-many-lines
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Any, ClassVar, Dict, List, Literal, Optional, Sequence, Set, Tuple, Union

from pydantic import BaseModel, Field, root_validator, validator

from featurebyte.enum import DBVarType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.base import (
    BaseNode,
    BasePrunableNode,
    BaseSeriesOutputNode,
    BaseSeriesOutputWithAScalarParamNode,
    NodeT,
)
from featurebyte.query_graph.node.metadata.column import InColumnStr, OutColumnStr
from featurebyte.query_graph.node.metadata.operation import (
    AggregationColumn,
    DerivedDataColumn,
    NodeOutputCategory,
    OperationStructure,
    OperationStructureBranchState,
    OperationStructureInfo,
    PostAggregationColumn,
    ViewDataColumn,
)
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    CodeGenerationConfig,
    CodeGenerationContext,
    ExpressionStr,
    InfoDict,
    ObjectClass,
    RightHandSide,
    StatementT,
    ValueStr,
    VariableNameGenerator,
    VariableNameStr,
    VarNameExpressionInfo,
    get_object_class_from_function_call,
)
from featurebyte.query_graph.node.mixin import AggregationOpStructMixin, BaseGroupbyParameters
from featurebyte.query_graph.util import append_to_lineage


class ProjectNode(BaseNode):
    """ProjectNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        columns: List[InColumnStr]

    type: Literal[NodeType.PROJECT] = Field(NodeType.PROJECT, const=True)
    parameters: Parameters

    @property
    def max_input_count(self) -> int:
        return 1

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self.parameters.columns

    def prune(
        self: NodeT,
        target_node_input_order_pairs: Sequence[Tuple[NodeT, int]],
        input_operation_structures: List[OperationStructure],
    ) -> NodeT:
        assert len(input_operation_structures) == 1
        input_op_struct = input_operation_structures[0]
        if input_op_struct.output_category == NodeOutputCategory.VIEW:
            # for view, the available columns are the columns
            avail_columns = set(col.name for col in input_op_struct.columns)
        else:
            # for feature, the available columns are the aggregations
            avail_columns = set(col.name for col in input_op_struct.aggregations)

        node_params = self.parameters.dict()
        node_params["columns"] = [col for col in self.parameters.columns if col in avail_columns]  # type: ignore
        return self.clone(parameters=node_params)

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        _ = branch_state, global_state
        input_operation_info = inputs[0]
        output_category = input_operation_info.output_category
        names = set(self.parameters.columns)
        node_kwargs: Dict[str, Any] = {}
        if output_category == NodeOutputCategory.VIEW:
            node_kwargs["columns"] = [
                col.clone(node_names=col.node_names.union([self.name]))
                for col in input_operation_info.columns
                if col.name in names
            ]
        else:
            node_kwargs["columns"] = input_operation_info.columns
            node_kwargs["aggregations"] = [
                col.clone(node_names=col.node_names.union([self.name]))
                for col in input_operation_info.aggregations
                if col.name in names
            ]

        return OperationStructure(
            **node_kwargs,
            output_type=self.output_type,
            output_category=output_category,
            row_index_lineage=input_operation_info.row_index_lineage,
        )

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        statements, var_name = self._convert_expression_to_variable(
            var_name_expression=var_name_expressions[0],
            var_name_generator=var_name_generator,
            node_output_type=NodeOutputType.FRAME,
            node_output_category=operation_structure.output_category,
            to_associate_with_node_name=False,
        )
        out_var_name = var_name_generator.generate_variable_name(
            node_output_type=operation_structure.output_type,
            node_output_category=operation_structure.output_category,
            node_name=self.name,
        )

        # must assign the projection result to a new variable
        # otherwise, it could cause issue when the original variable is modified
        # for example, the second `view["col_int", "col_float"]]` is different from the first one:
        #     view.join(view["col_int", "col_float"]], rsuffix="_y")
        #     view.join(view["col_int", "col_float"]], rsuffix="_z")
        # after the first join, the `view` node get updated and the second join will refer to the updated `view`
        if operation_structure.output_type == NodeOutputType.FRAME:
            expression = ExpressionStr(f"{var_name}[{self.parameters.columns}]")
        else:
            expression = ExpressionStr(f"{var_name}['{self.parameters.columns[0]}']")

        statements.append((out_var_name, expression))
        return statements, out_var_name


class FilterNode(BaseNode):
    """FilterNode class"""

    type: Literal[NodeType.FILTER] = Field(NodeType.FILTER, const=True)
    parameters: BaseModel = Field(default=BaseModel(), const=True)

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        # the first input is the input view and the second input is the mask view
        if input_index == 0:
            # for the input view, all columns are required, otherwise it may drop some columns
            # during the preview (where the final output is a filter node)
            return available_column_names
        return self._assert_empty_required_input_columns()

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        _ = branch_state, global_state
        input_operation_info, mask_operation_info = inputs
        output_category = input_operation_info.output_category
        node_kwargs: Dict[str, Any] = {}
        if output_category == NodeOutputCategory.VIEW:
            other_node_names = {self.name}.union(mask_operation_info.all_node_names)
            node_kwargs["columns"] = [
                col.clone(
                    filter=True,
                    node_names=col.node_names.union(other_node_names),
                    node_name=self.name,
                )
                for col in input_operation_info.columns
            ]
        else:
            node_kwargs["columns"] = input_operation_info.columns
            node_kwargs["aggregations"] = [
                PostAggregationColumn.create(
                    name=col.name,
                    columns=[col],
                    transform=self.transform_info,
                    node_name=self.name,
                    other_node_names=mask_operation_info.all_node_names,
                    dtype=col.dtype,
                )
                for col in input_operation_info.aggregations
            ]

        return OperationStructure(
            **node_kwargs,
            output_type=input_operation_info.output_type,
            output_category=output_category,
            row_index_lineage=append_to_lineage(input_operation_info.row_index_lineage, self.name),
        )

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name_expr = var_name_expressions[0]
        statements, var_name = self._convert_expression_to_variable(
            var_name_expression=var_name_expr,
            var_name_generator=var_name_generator,
            node_output_type=operation_structure.output_type,
            node_output_category=operation_structure.output_category,
            to_associate_with_node_name=False,
        )
        mask_name = var_name_expressions[1].as_input()
        expression = ExpressionStr(f"{var_name}[{mask_name}]")
        return statements, expression


class AssignColumnMixin:
    """AssignColumnMixin class"""

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
        # for the assign-like operation, the first column is a frame view
        # if the node is pruned (mean that the new column is not required),
        # we should use the frame input to resolve the situation.
        return input_node_names[0]

    @staticmethod
    def _validate_view(view_op_structure: OperationStructure) -> None:
        assert view_op_structure.output_category == NodeOutputCategory.VIEW
        assert view_op_structure.output_type == NodeOutputType.FRAME

    @staticmethod
    def _construct_operation_structure(
        input_operation_info: OperationStructure,
        new_column_name: str,
        columns: List[ViewDataColumn],
        node_name: str,
        new_column_var_type: DBVarType,
    ) -> OperationStructure:
        """
        Construct operation structure of the assign-column-like operation

        Parameters
        ----------
        input_operation_info: OperationStructure
            Input operation info of a frame that a series will be assigned to
        new_column_name: str
            Column name that will be assigned to the new column (from the series input)
        columns: List[ViewDataColumn]
            List of columns that are used to derive the series
        node_name: str
            Node name of the operation that assign a series to a frame
        new_column_var_type: DBVarType
            Variable type of new column

        Returns
        -------
        OperationStructure
        """
        input_columns = [col for col in input_operation_info.columns if col.name != new_column_name]
        new_column = DerivedDataColumn.create(
            name=new_column_name,
            columns=columns,
            transform=None,
            node_name=node_name,
            dtype=new_column_var_type,
        )
        return OperationStructure(
            columns=input_columns + [new_column],
            output_type=NodeOutputType.FRAME,
            output_category=NodeOutputCategory.VIEW,
            row_index_lineage=input_operation_info.row_index_lineage,
        )


class AssignNode(AssignColumnMixin, BasePrunableNode):
    """AssignNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        name: OutColumnStr
        value: Optional[Any]

    type: Literal[NodeType.ASSIGN] = Field(NodeType.ASSIGN, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Parameters

    @property
    def max_input_count(self) -> int:
        return 2

    @property
    def is_inplace_operation_in_sdk_code(self) -> bool:
        return True

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    @staticmethod
    def _validate_series(series_op_structure: OperationStructure) -> None:
        assert series_op_structure.output_type == NodeOutputType.SERIES
        assert series_op_structure.output_category == NodeOutputCategory.VIEW

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        # First input is a View
        input_operation_info = inputs[0]
        self._validate_view(input_operation_info)

        # AssignNode can only take 1 or 2 parameters (1 parameters is frame, 2nd optional parameter is series)
        columns = []
        if len(inputs) == 2:
            series_input = inputs[1]
            self._validate_series(series_input)
            columns = series_input.columns
            dtype = series_input.series_output_dtype
        else:
            dtype = self.detect_var_type_from_value(self.parameters.value)

        return self._construct_operation_structure(
            input_operation_info=input_operation_info,
            new_column_name=self.parameters.name,
            columns=columns,
            node_name=self.name,
            new_column_var_type=dtype,
        )

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expr = node_inputs[0]
        assert not isinstance(var_name_expr, InfoDict)
        column_name = self.parameters.name
        statements, var_name = self._convert_expression_to_variable(
            var_name_expression=var_name_expr,
            var_name_generator=var_name_generator,
            node_output_type=NodeOutputType.FRAME,
            node_output_category=operation_structure.output_category,
            to_associate_with_node_name=False,
        )
        second_input = None
        if len(node_inputs) == 2:
            second_input = node_inputs[1]

        output_var_name = var_name
        if context.required_copy:
            output_var_name = var_name_generator.generate_variable_name(
                node_output_type=operation_structure.output_type,
                node_output_category=operation_structure.output_category,
                node_name=self.name,
            )
            statements.append((output_var_name, ExpressionStr(f"{var_name}.copy()")))

        value: RightHandSide
        if isinstance(second_input, InfoDict):
            mask_var = second_input["mask"]
            if second_input.get("is_series_assignment"):
                value = second_input["value"]
            else:
                value = ValueStr.create(second_input["value"])
            statements.append(
                (
                    VariableNameStr(f"{output_var_name}['{column_name}'][{mask_var}]"),
                    value,
                )
            )
        else:
            value = second_input if second_input else ValueStr.create(self.parameters.value)
            statements.append((VariableNameStr(f"{output_var_name}['{column_name}']"), value))
        return statements, output_var_name


class LagNode(BaseSeriesOutputNode):
    """LagNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        entity_columns: List[InColumnStr]
        timestamp_column: InColumnStr
        offset: int

    type: Literal[NodeType.LAG] = Field(NodeType.LAG, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.SERIES, const=True)
    parameters: Parameters

    @property
    def max_input_count(self) -> int:
        return len(self.parameters.entity_columns) + 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        # this node has the following input structure:
        # [0] column to lag
        # [1...n-1] entity column(s)
        # [n] timestamp column
        if input_index == 0:
            # first input (zero-based)
            return []
        if input_index == len(self.parameters.entity_columns):
            # last input (zero-based)
            return [self.parameters.timestamp_column]
        # entity column
        return [self.parameters.entity_columns[input_index - 1]]

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return inputs[0].series_output_dtype

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        col_name = var_name_expressions[0].as_input()
        entity_columns = ValueStr.create(self.parameters.entity_columns)
        offset = ValueStr.create(self.parameters.offset)
        expression = f"{col_name}.lag(entity_columns={entity_columns}, offset={offset})"
        return [], ExpressionStr(expression)


class ForwardAggregateParameters(BaseGroupbyParameters):
    """
    Forward aggregate parameters
    """

    name: str
    window: Optional[str]
    timestamp_col: InColumnStr


class ForwardAggregateNode(AggregationOpStructMixin, BaseNode):
    """
    ForwardAggregateNode class.
    """

    type: Literal[NodeType.FORWARD_AGGREGATE] = Field(NodeType.FORWARD_AGGREGATE, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: ForwardAggregateParameters

    @property
    def max_input_count(self) -> int:
        return 1

    def _get_aggregations(
        self,
        columns: List[ViewDataColumn],
        node_name: str,
        other_node_names: Set[str],
        output_var_type: DBVarType,
    ) -> List[AggregationColumn]:
        col_name_map = {col.name: col for col in columns}
        return [
            AggregationColumn(
                name=self.parameters.name,
                method=self.parameters.agg_func,
                keys=self.parameters.keys,
                window=self.parameters.window,
                category=self.parameters.value_by,
                column=col_name_map.get(self.parameters.parent),
                filter=any(col.filter for col in columns),
                aggregation_type=self.type,
                node_names={node_name}.union(other_node_names),
                node_name=node_name,
                dtype=output_var_type,
            )
        ]

    def _exclude_source_columns(self) -> List[str]:
        cols = self.parameters.keys
        return [str(col) for col in cols]

    def _is_time_based(self) -> bool:
        return True

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._extract_column_str_values(self.parameters.dict(), InColumnStr)

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        statements, var_name = self._convert_expression_to_variable(
            var_name_expression=var_name_expressions[0],
            var_name_generator=var_name_generator,
            node_output_type=NodeOutputType.FRAME,
            node_output_category=NodeOutputCategory.VIEW,
            to_associate_with_node_name=False,
        )
        keys = ValueStr.create(self.parameters.keys)
        category = ValueStr.create(self.parameters.value_by)
        grouped = f"{var_name}.groupby(by_keys={keys}, category={category})"
        out_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="target",
            node_name=self.name,
        )
        expression = get_object_class_from_function_call(
            callable_name=f"{grouped}.forward_aggregate",
            value_column=self.parameters.parent,
            method=self.parameters.agg_func,
            window=self.parameters.window,
            target_name=self.parameters.name,
            skip_fill_na=True,
        )
        statements.append((out_var_name, expression))
        return statements, out_var_name


class GroupByNode(AggregationOpStructMixin, BaseNode):
    """GroupByNode class"""

    class Parameters(BaseGroupbyParameters):
        """Parameters"""

        windows: List[Optional[str]]
        timestamp: InColumnStr
        blind_spot: int
        time_modulo_frequency: int
        frequency: int
        names: List[OutColumnStr]
        tile_id: Optional[str]
        aggregation_id: Optional[str]

    type: Literal[NodeType.GROUPBY] = Field(NodeType.GROUPBY, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Parameters

    @property
    def max_input_count(self) -> int:
        return 1

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._extract_column_str_values(self.parameters.dict(), InColumnStr)

    def _exclude_source_columns(self) -> List[str]:
        cols = self.parameters.keys + [self.parameters.timestamp]
        return [str(col) for col in cols]

    def _is_time_based(self) -> bool:
        return True

    def _get_aggregations(
        self,
        columns: List[ViewDataColumn],
        node_name: str,
        other_node_names: Set[str],
        output_var_type: DBVarType,
    ) -> List[AggregationColumn]:
        col_name_map = {col.name: col for col in columns}
        return [
            AggregationColumn(
                name=name,
                method=self.parameters.agg_func,
                keys=self.parameters.keys,
                window=window,
                category=self.parameters.value_by,
                column=col_name_map.get(self.parameters.parent),
                filter=any(col.filter for col in columns),
                aggregation_type=self.type,
                node_names={node_name}.union(other_node_names),
                node_name=node_name,
                dtype=output_var_type,
            )
            for name, window in zip(self.parameters.names, self.parameters.windows)
        ]

    def prune(
        self: NodeT,
        target_node_input_order_pairs: Sequence[Tuple[NodeT, int]],
        input_operation_structures: List[OperationStructure],
    ) -> NodeT:
        if target_node_input_order_pairs:
            required_columns = set().union(
                *(
                    node.get_required_input_columns(
                        input_index=input_order, available_column_names=self.parameters.names  # type: ignore
                    )
                    for node, input_order in target_node_input_order_pairs
                )
            )
            params = self.parameters
            pruned_params_dict = self.parameters.dict()
            pruned_params_dict.update(names=[], windows=[])
            for name, window in zip(params.names, params.windows):  # type: ignore
                if name in required_columns:
                    pruned_params_dict["names"].append(name)
                    pruned_params_dict["windows"].append(window)
            return self.clone(parameters=pruned_params_dict)
        return self

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        statements, var_name = self._convert_expression_to_variable(
            var_name_expression=var_name_expressions[0],
            var_name_generator=var_name_generator,
            node_output_type=NodeOutputType.FRAME,
            node_output_category=NodeOutputCategory.VIEW,
            to_associate_with_node_name=False,
        )
        keys = ValueStr.create(self.parameters.keys)
        category = ValueStr.create(self.parameters.value_by)
        feature_job_setting: ObjectClass = ClassEnum.FEATURE_JOB_SETTING(
            blind_spot=f"{self.parameters.blind_spot}s",
            frequency=f"{self.parameters.frequency}s",
            time_modulo_frequency=f"{self.parameters.time_modulo_frequency}s",
        )
        grouped = f"{var_name}.groupby(by_keys={keys}, category={category})"
        out_var_name = var_name_generator.generate_variable_name(
            node_output_type=operation_structure.output_type,
            node_output_category=operation_structure.output_category,
            node_name=self.name,
        )
        expression = get_object_class_from_function_call(
            callable_name=f"{grouped}.aggregate_over",
            value_column=self.parameters.parent,
            method=self.parameters.agg_func,
            windows=self.parameters.windows,
            feature_names=self.parameters.names,
            feature_job_setting=feature_job_setting,
            skip_fill_na=True,
        )
        statements.append((out_var_name, expression))
        return statements, out_var_name


class ItemGroupbyParameters(BaseGroupbyParameters):
    """ItemGroupbyNode parameters"""

    name: OutColumnStr


class ItemGroupbyNode(AggregationOpStructMixin, BaseNode):
    """ItemGroupbyNode class"""

    type: Literal[NodeType.ITEM_GROUPBY] = Field(NodeType.ITEM_GROUPBY, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: ItemGroupbyParameters

    # class variable
    _auto_convert_expression_to_variable: ClassVar[bool] = False

    @property
    def max_input_count(self) -> int:
        return 1

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._extract_column_str_values(self.parameters.dict(), InColumnStr)

    def _exclude_source_columns(self) -> List[str]:
        return [str(key) for key in self.parameters.keys]

    def _is_time_based(self) -> bool:
        return False

    def _get_aggregations(
        self,
        columns: List[ViewDataColumn],
        node_name: str,
        other_node_names: Set[str],
        output_var_type: DBVarType,
    ) -> List[AggregationColumn]:
        col_name_map = {col.name: col for col in columns}
        return [
            AggregationColumn(
                name=self.parameters.name,
                method=self.parameters.agg_func,
                keys=self.parameters.keys,
                window=None,
                category=None,
                column=col_name_map.get(self.parameters.parent),
                filter=any(col.filter for col in columns),
                aggregation_type=self.type,
                node_names={node_name}.union(other_node_names),
                node_name=node_name,
                dtype=output_var_type,
            )
        ]

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        # Note: this node is a special case as the output of this node is not a complete SDK code.
        # Currently, `item_view.groupby(...).aggregate()` will generate ItemGroupbyNode + ProjectNode.
        # Output of ItemGroupbyNode is just an expression, the actual variable assignment
        # will be done at the ProjectNode.
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        statements, var_name = self._convert_expression_to_variable(
            var_name_expression=var_name_expressions[0],
            var_name_generator=var_name_generator,
            node_output_type=NodeOutputType.FRAME,
            node_output_category=NodeOutputCategory.VIEW,
            to_associate_with_node_name=False,
        )
        keys = ValueStr.create(self.parameters.keys)
        category = ValueStr.create(self.parameters.value_by)
        value_column = ValueStr.create(self.parameters.parent)
        method = ValueStr.create(self.parameters.agg_func)
        feature_name = ValueStr.create(self.parameters.name)
        grouped = f"{var_name}.groupby(by_keys={keys}, category={category})"
        agg = (
            f"aggregate(value_column={value_column}, "
            f"method={method}, "
            f"feature_name={feature_name}, "
            f"skip_fill_na=True)"
        )
        return statements, ExpressionStr(f"{grouped}.{agg}")


class SCDBaseParameters(BaseModel):
    """Parameters common to SCD table"""

    effective_timestamp_column: InColumnStr
    natural_key_column: Optional[InColumnStr] = Field(default=None)  # DEV-556: should be compulsory
    current_flag_column: Optional[InColumnStr]
    end_timestamp_column: Optional[InColumnStr]

    @root_validator(pre=True)
    @classmethod
    def _convert_node_parameters_format(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # DEV-556: backward compatibility
        if "right_timestamp_column" in values:
            values["effective_timestamp_column"] = values["right_timestamp_column"]
        return values


class SCDJoinParameters(SCDBaseParameters):
    """Parameters for SCD join"""

    left_timestamp_column: InColumnStr


class SCDLookupParameters(SCDBaseParameters):
    """Parameters for SCD lookup"""

    offset: Optional[str]


class EventLookupParameters(BaseModel):
    """Parameters for EventTable lookup"""

    event_timestamp_column: InColumnStr


class LookupParameters(BaseModel):
    """Lookup NOde Parameters"""

    input_column_names: List[InColumnStr]
    feature_names: List[OutColumnStr]
    entity_column: InColumnStr
    serving_name: str
    entity_id: PydanticObjectId
    scd_parameters: Optional[SCDLookupParameters]
    event_parameters: Optional[EventLookupParameters]

    @root_validator(skip_on_failure=True)
    @classmethod
    def _validate_input_column_names_feature_names_same_length(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        input_column_names = values["input_column_names"]
        feature_names = values["feature_names"]
        assert len(input_column_names) == len(feature_names)
        return values


class BaseLookupNode(AggregationOpStructMixin, BaseNode):
    """BaseLookupNode class"""

    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: LookupParameters

    @property
    def max_input_count(self) -> int:
        return 1

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._extract_column_str_values(self.parameters.dict(), InColumnStr)

    def _get_parent_columns(self, columns: List[ViewDataColumn]) -> Optional[List[ViewDataColumn]]:
        parent_columns = [col for col in columns if col.name in self.parameters.input_column_names]
        return parent_columns

    def _is_time_based(self) -> bool:
        return (
            self.parameters.scd_parameters is not None
            or self.parameters.event_parameters is not None
        )

    def _get_aggregations(
        self,
        columns: List[ViewDataColumn],
        node_name: str,
        other_node_names: Set[str],
        output_var_type: DBVarType,
    ) -> List[AggregationColumn]:
        name_to_column = {col.name: col for col in columns}
        return [
            AggregationColumn(
                name=feature_name,
                method=None,
                keys=[self.parameters.entity_column],
                window=None,
                category=None,
                column=name_to_column[input_column_name],
                aggregation_type=self.type,  # type: ignore[arg-type]
                node_names={node_name}.union(other_node_names),
                node_name=node_name,
                filter=any(col.filter for col in columns),
                dtype=name_to_column[input_column_name].dtype,
            )
            for input_column_name, feature_name in zip(
                self.parameters.input_column_names, self.parameters.feature_names
            )
        ]

    def _exclude_source_columns(self) -> List[str]:
        return [self.parameters.entity_column]


class LookupNode(BaseLookupNode):
    """LookupNode class"""

    type: Literal[NodeType.LOOKUP] = Field(NodeType.LOOKUP, const=True)

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        statements, var_name = self._convert_expression_to_variable(
            var_name_expression=var_name_expressions[0],
            var_name_generator=var_name_generator,
            node_output_type=NodeOutputType.FRAME,
            node_output_category=NodeOutputCategory.VIEW,
            to_associate_with_node_name=False,
        )
        input_column_names = self.parameters.input_column_names
        feature_names = self.parameters.feature_names
        offset = None
        if self.parameters.scd_parameters:
            offset = self.parameters.scd_parameters.offset
        grouped = (
            f"{var_name}.as_features(column_names={input_column_names}, "
            f"feature_names={feature_names}, "
            f"offset={ValueStr.create(offset)})"
        )
        out_var_name = var_name_generator.generate_variable_name(
            node_output_type=operation_structure.output_type,
            node_output_category=operation_structure.output_category,
            node_name=self.name,
        )
        statements.append((out_var_name, ExpressionStr(grouped)))
        return statements, out_var_name


class LookupTargetParameters(LookupParameters):
    """LookupTargetParameters"""

    offset: Optional[str]


class LookupTargetNode(BaseLookupNode):
    """LookupTargetNode class"""

    type: Literal[NodeType.LOOKUP_TARGET] = Field(NodeType.LOOKUP_TARGET, const=True)
    parameters: LookupTargetParameters


class JoinMetadata(BaseModel):
    """Metadata to track general `view.join(...)` operation"""

    type: str = Field("join", const=True)
    rsuffix: str


class JoinEventTableAttributesMetadata(BaseModel):
    """Metadata to track `item_view.join_event_table_attributes(...)` operation"""

    type: str = Field("join_event_table_attributes", const=True)
    columns: List[str]
    event_suffix: Optional[str]


class JoinNodeParameters(BaseModel):
    """JoinNodeParameters"""

    left_on: str
    right_on: str
    left_input_columns: List[InColumnStr]
    left_output_columns: List[OutColumnStr]
    right_input_columns: List[InColumnStr]
    right_output_columns: List[OutColumnStr]
    join_type: Literal["left", "inner"]
    scd_parameters: Optional[SCDJoinParameters]
    metadata: Optional[Union[JoinMetadata, JoinEventTableAttributesMetadata]] = Field(
        default=None
    )  # DEV-556: should be compulsory

    @validator(
        "left_input_columns",
        "right_input_columns",
        "left_output_columns",
        "right_output_columns",
    )
    @classmethod
    def _validate_columns_are_unique(cls, values: List[str]) -> List[str]:
        if len(values) != len(set(values)):
            raise ValueError(f"Column names (values: {values}) must be unique!")
        return values

    @root_validator
    @classmethod
    def _validate_left_and_right_output_columns(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        duplicated_output_cols = set(values.get("left_output_columns", [])).intersection(
            values.get("right_output_columns", [])
        )
        if duplicated_output_cols:
            raise ValueError("Left and right output columns should not have common item(s).")
        return values


class JoinNode(BasePrunableNode):
    """Join class"""

    type: Literal[NodeType.JOIN] = Field(NodeType.JOIN, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: JoinNodeParameters

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        if input_index == 0:
            return list(set(self.parameters.left_input_columns).union([self.parameters.left_on]))
        return list(set(self.parameters.right_input_columns).union([self.parameters.right_on]))

    @staticmethod
    def _filter_columns(
        input_columns: Sequence[str], output_columns: Sequence[str], available_columns: Set[str]
    ) -> Tuple[List[str], List[str]]:
        # filter input & output columns using the available columns
        in_cols, out_cols = [], []
        for in_col, out_col in zip(input_columns, output_columns):
            if in_col in available_columns:
                in_cols.append(in_col)
                out_cols.append(out_col)
        return in_cols, out_cols

    def prune(
        self: NodeT,
        target_node_input_order_pairs: Sequence[Tuple[NodeT, int]],
        input_operation_structures: List[OperationStructure],
    ) -> NodeT:
        # Prune the join node parameters by using the available columns. If the input column is not found in the
        # input operation structure, remove it & its corresponding output column name from the join node parameters.
        assert len(input_operation_structures) == 2
        left_avail_columns = set(col.name for col in input_operation_structures[0].columns)
        right_avail_columns = set(col.name for col in input_operation_structures[1].columns)
        node_params = self.parameters.dict()
        (
            node_params["left_input_columns"],
            node_params["left_output_columns"],
        ) = self._filter_columns(  # type: ignore[attr-defined]
            self.parameters.left_input_columns,  # type: ignore[attr-defined]
            self.parameters.left_output_columns,  # type: ignore[attr-defined]
            left_avail_columns,
        )
        (
            node_params["right_input_columns"],
            node_params["right_output_columns"],
        ) = self._filter_columns(  # type: ignore[attr-defined]
            self.parameters.right_input_columns,  # type: ignore[attr-defined]
            self.parameters.right_output_columns,  # type: ignore[attr-defined]
            right_avail_columns,
        )
        metadata = node_params.get("metadata") or {}
        if metadata.get("type") == "join_event_table_attributes":
            node_params["metadata"]["columns"] = [
                col for col in node_params["metadata"]["columns"] if col in right_avail_columns
            ]
        return self.clone(parameters=node_params)

    def resolve_node_pruned(self, input_node_names: List[str]) -> str:
        # if this join node is pruned, use the first input node to resolve pruned node not found issue
        return input_node_names[0]

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        params = self.parameters
        # construct input column name to output column name mapping for left & right columns
        left_col_map = dict(zip(params.left_input_columns, params.left_output_columns))
        right_col_map = dict(zip(params.right_input_columns, params.right_output_columns))

        # construct input column name to output column mapping for left & right columns
        left_columns = {
            col.name: col.clone(
                name=left_col_map[col.name],  # type: ignore
                # if the join type is left, current node is not a compulsory node for the column
                node_names=col.node_names.union([self.name])
                if params.join_type != "left"
                else col.node_names,
                node_name=self.name,
            )
            for col in inputs[0].columns
            if col.name in left_col_map
        }
        left_on_col = next(col for col in inputs[0].columns if col.name == self.parameters.left_on)
        right_columns = {}
        right_on_col = next(
            col for col in inputs[1].columns if col.name == self.parameters.right_on
        )
        for col in inputs[1].columns:
            if col.name in right_col_map:
                if global_state.keep_all_source_columns:
                    # when keep_all_source_columns is True, we should include the right_on column in the join
                    # so that any changes on the right_on column can be tracked.
                    right_columns[col.name] = DerivedDataColumn.create(
                        name=right_col_map[col.name],  # type: ignore
                        # the main source column must be on the right most side
                        # this is used to decide the timestamp column source table in
                        # `iterate_group_by_node_and_table_id_pairs`
                        columns=[left_on_col, right_on_col, col],
                        transform=self.transform_info,
                        node_name=self.name,
                        dtype=col.dtype,
                        other_node_names=col.node_names,
                    )
                else:
                    right_columns[col.name] = col.clone(
                        name=right_col_map[col.name],  # type: ignore
                        node_names=col.node_names.union([self.name]),
                        node_name=self.name,
                    )

        if self.parameters.join_type == "left":
            row_index_lineage = inputs[0].row_index_lineage
        else:
            row_index_lineage = append_to_lineage(inputs[0].row_index_lineage, self.name)

        # construct left & right output columns
        left_cols = [left_columns[col_name] for col_name in params.left_input_columns]
        right_cols = [right_columns[col_name] for col_name in params.right_input_columns]

        return OperationStructure(
            columns=left_cols + right_cols,  # type: ignore
            output_type=NodeOutputType.FRAME,
            output_category=NodeOutputCategory.VIEW,
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
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        left_statements, left_var_name = self._convert_expression_to_variable(
            var_name_expression=var_name_expressions[0],
            var_name_generator=var_name_generator,
            node_output_type=NodeOutputType.FRAME,
            node_output_category=NodeOutputCategory.VIEW,
            to_associate_with_node_name=False,
        )
        right_statements, right_var_name = self._convert_expression_to_variable(
            var_name_expression=var_name_expressions[1],
            var_name_generator=var_name_generator,
            node_output_type=NodeOutputType.FRAME,
            node_output_category=NodeOutputCategory.VIEW,
            to_associate_with_node_name=False,
        )
        statements = left_statements + right_statements
        var_name = left_var_name
        assert self.parameters.metadata is not None, "Join node metadata is not set."
        if isinstance(self.parameters.metadata, JoinMetadata):
            other_var_name = right_var_name
            expression = ExpressionStr(
                f"{var_name}.join({other_var_name}, "
                f"on={ValueStr.create(self.parameters.left_on)}, "
                f"how={ValueStr.create(self.parameters.join_type)}, "
                f"rsuffix={ValueStr.create(self.parameters.metadata.rsuffix)})"
            )
        else:
            expression = ExpressionStr(
                f"{var_name}.join_event_table_attributes("
                f"columns={ValueStr.create(self.parameters.metadata.columns)}, "
                f"event_suffix={ValueStr.create(self.parameters.metadata.event_suffix)})"
            )

        var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="joined_view", node_name=self.name
        )
        statements.append((var_name, expression))
        return statements, var_name


class JoinFeatureNode(AssignColumnMixin, BasePrunableNode):
    """JoinFeatureNode class

    This node should have two input nodes. The first input node is the View's node, and the second
    input node is the Feature's node.
    """

    class Parameters(BaseModel):
        """
        Parameters for JoinFeatureNode

        Parameters
        ----------
        view_entity_column: str
            Column name in the View to be used as join key
        view_point_in_time_column: Optional[str]
            Column name in the View to be used as point in time column when joining with a time
            based feature
        feature_entity_column: str
            Join key for the feature. For non-time based features, this should be the key parameter
            of the ItemGroupbyNode that generated the feature
        name: str
            Name of the column when the feature is added to the EventView
        """

        view_entity_column: InColumnStr
        view_point_in_time_column: Optional[InColumnStr]
        feature_entity_column: InColumnStr
        name: OutColumnStr

    type: Literal[NodeType.JOIN_FEATURE] = Field(NodeType.JOIN_FEATURE, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Parameters

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        if input_index == 0:
            view_required_columns = [self.parameters.view_entity_column]
            if self.parameters.view_point_in_time_column:
                view_required_columns.append(self.parameters.view_point_in_time_column)
            return view_required_columns
        return [self.parameters.feature_entity_column]

    @staticmethod
    def _validate_feature(feature_op_structure: OperationStructure) -> None:
        columns = feature_op_structure.columns
        assert len(columns) == 1
        # For now, the supported feature should have an item_groupby node in its lineage
        assert any(node_name.startswith("item_groupby") for node_name in columns[0].node_names)
        assert feature_op_structure.output_type == NodeOutputType.SERIES

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        # First input is a View
        input_operation_info = inputs[0]
        self._validate_view(input_operation_info)

        # Second input node is a Feature
        feature_operation_info = inputs[1]
        self._validate_feature(feature_operation_info)

        # If this View has a column that has the same name as the feature to be added, it will be
        # omitted. This is because the added feature will replace that existing column.
        return self._construct_operation_structure(
            input_operation_info=input_operation_info,
            new_column_name=self.parameters.name,
            columns=feature_operation_info.columns,
            node_name=self.name,
            new_column_var_type=feature_operation_info.series_output_dtype,
        )

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        new_column_name = ValueStr.create(self.parameters.name)
        feature = var_name_expressions[1]
        entity_column = ValueStr.create(self.parameters.view_entity_column)
        statements, var_name = self._convert_expression_to_variable(
            var_name_expression=var_name_expressions[0],
            var_name_generator=var_name_generator,
            node_output_type=NodeOutputType.FRAME,
            node_output_category=NodeOutputCategory.VIEW,
            to_associate_with_node_name=False,
        )
        expression = ExpressionStr(
            f"{var_name}.add_feature(new_column_name={new_column_name}, "
            f"feature={feature}, entity_column={entity_column})"
        )
        out_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="joined_view", node_name=self.name
        )
        statements.append((out_var_name, expression))
        return statements, out_var_name


class TrackChangesNodeParameters(BaseModel):
    """Parameters for TrackChangesNode"""

    natural_key_column: InColumnStr
    effective_timestamp_column: InColumnStr
    tracked_column: InColumnStr
    previous_tracked_column_name: OutColumnStr
    new_tracked_column_name: OutColumnStr
    previous_valid_from_column_name: OutColumnStr
    new_valid_from_column_name: OutColumnStr


class TrackChangesNode(BaseNode):
    """TrackChangesNode class"""

    type: Literal[NodeType.TRACK_CHANGES] = Field(NodeType.TRACK_CHANGES, const=True)
    parameters: TrackChangesNodeParameters

    @property
    def max_input_count(self) -> int:
        return 1

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return [
            self.parameters.natural_key_column,
            self.parameters.effective_timestamp_column,
            self.parameters.tracked_column,
        ]

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        _ = branch_state, global_state
        input_operation_info = inputs[0]
        effective_timestamp_source_column = next(
            column
            for column in input_operation_info.columns
            if column.name == self.parameters.effective_timestamp_column
        )
        tracked_source_column = next(
            column
            for column in input_operation_info.columns
            if column.name == self.parameters.tracked_column
        )
        natural_key_source_column = next(
            column
            for column in input_operation_info.columns
            if column.name == self.parameters.natural_key_column
        )
        columns = [natural_key_source_column]
        track_dtype = tracked_source_column.dtype
        valid_dtype = effective_timestamp_source_column.dtype
        for column_name, dtype in [
            (self.parameters.previous_tracked_column_name, track_dtype),
            (self.parameters.new_tracked_column_name, track_dtype),
            (self.parameters.previous_valid_from_column_name, valid_dtype),
            (self.parameters.new_valid_from_column_name, valid_dtype),
        ]:
            derived_column = DerivedDataColumn.create(
                name=column_name,
                columns=[effective_timestamp_source_column, tracked_source_column],
                transform=self.transform_info,
                node_name=self.name,
                dtype=dtype,
            )
            columns.append(derived_column)
        return OperationStructure(
            columns=columns,
            output_type=NodeOutputType.FRAME,
            output_category=NodeOutputCategory.VIEW,
            row_index_lineage=append_to_lineage(input_operation_info.row_index_lineage, self.name),
        )

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        raise NotImplementedError()


class AggregateAsAtParameters(BaseGroupbyParameters, SCDBaseParameters):
    """Parameters for AggregateAsAtNode"""

    name: OutColumnStr
    offset: Optional[str]
    backward: Optional[bool]


class AggregateAsAtNode(AggregationOpStructMixin, BaseNode):
    """AggregateAsAt class"""

    type: Literal[NodeType.AGGREGATE_AS_AT] = Field(NodeType.AGGREGATE_AS_AT, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: AggregateAsAtParameters

    # class variable
    _auto_convert_expression_to_variable: ClassVar[bool] = False

    @property
    def max_input_count(self) -> int:
        return 1

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._extract_column_str_values(self.parameters.dict(), InColumnStr)

    def _exclude_source_columns(self) -> List[str]:
        return [str(key) for key in self.parameters.keys]

    def _is_time_based(self) -> bool:
        return True

    def _get_aggregations(
        self,
        columns: List[ViewDataColumn],
        node_name: str,
        other_node_names: Set[str],
        output_var_type: DBVarType,
    ) -> List[AggregationColumn]:
        col_name_map = {col.name: col for col in columns}
        return [
            AggregationColumn(
                name=self.parameters.name,
                method=self.parameters.agg_func,
                keys=self.parameters.keys,
                window=None,
                category=None,
                column=col_name_map.get(self.parameters.parent),
                filter=any(col.filter for col in columns),
                aggregation_type=self.type,
                node_names={node_name}.union(other_node_names),
                node_name=node_name,
                dtype=output_var_type,
            )
        ]

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        # Note: this node is a special case as the output of this node is not a complete SDK code.
        # Currently, `scd_view.groupby(...).aggregate_asat()` will generate AggregateAsAtNode + ProjectNode.
        # Output of AggregateAsAtNode is just an expression, the actual variable assignment
        # will be done at the ProjectNode.
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        statements, var_name = self._convert_expression_to_variable(
            var_name_expression=var_name_expressions[0],
            var_name_generator=var_name_generator,
            node_output_type=NodeOutputType.FRAME,
            node_output_category=NodeOutputCategory.VIEW,
            to_associate_with_node_name=False,
        )
        keys = ValueStr.create(self.parameters.keys)
        category = ValueStr.create(self.parameters.value_by)
        value_column = ValueStr.create(self.parameters.parent)
        method = ValueStr.create(self.parameters.agg_func)
        feature_name = ValueStr.create(self.parameters.name)
        offset = ValueStr.create(self.parameters.offset)
        backward = ValueStr.create(self.parameters.backward)
        grouped = f"{var_name}.groupby(by_keys={keys}, category={category})"
        agg = (
            f"aggregate_asat(value_column={value_column}, "
            f"method={method}, "
            f"feature_name={feature_name}, "
            f"offset={offset}, "
            f"backward={backward}, "
            f"skip_fill_na=True)"
        )
        return statements, ExpressionStr(f"{grouped}.{agg}")


class AliasNode(BaseNode):
    """AliasNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        name: OutColumnStr

    type: Literal[NodeType.ALIAS] = Field(NodeType.ALIAS, const=True)
    parameters: Parameters

    @property
    def max_input_count(self) -> int:
        return 1

    @property
    def is_inplace_operation_in_sdk_code(self) -> bool:
        return True

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        _ = branch_state, global_state
        input_operation_info = inputs[0]
        output_category = input_operation_info.output_category

        node_kwargs: Dict[str, Any] = {}
        new_name = self.parameters.name
        if output_category == NodeOutputCategory.VIEW:
            last_col = input_operation_info.columns[-1]
            node_kwargs["columns"] = list(input_operation_info.columns)
            node_kwargs["columns"][-1] = last_col.clone(
                name=new_name,
                node_names=last_col.node_names.union([self.name]),
                node_name=self.name,
            )
        else:
            last_agg = input_operation_info.aggregations[-1]
            node_kwargs["columns"] = input_operation_info.columns
            node_kwargs["aggregations"] = list(input_operation_info.aggregations)
            node_kwargs["aggregations"][-1] = last_agg.clone(
                name=new_name,
                node_names=last_agg.node_names.union([self.name]),
                node_name=self.name,
            )

        return OperationStructure(
            **node_kwargs,
            output_type=self.output_type,
            output_category=output_category,
            row_index_lineage=input_operation_info.row_index_lineage,
        )

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name_expr = var_name_expressions[0]
        statements, var_name = self._convert_expression_to_variable(
            var_name_expression=var_name_expr,
            var_name_generator=var_name_generator,
            node_output_type=operation_structure.output_type,
            node_output_category=operation_structure.output_category,
            to_associate_with_node_name=False,
        )
        var_statements, output_var_name = self._convert_to_proper_variable_name(
            var_name=var_name,
            var_name_generator=var_name_generator,
            operation_structure=operation_structure,
            required_copy=context.required_copy,
            to_associate_with_node_name=True,
        )
        statements.extend(var_statements)
        statements.append(
            (VariableNameStr(f"{output_var_name}.name"), ValueStr.create(self.parameters.name))
        )
        return statements, output_var_name


class ConditionalNode(BaseSeriesOutputWithAScalarParamNode):
    """ConditionalNode class"""

    type: Literal[NodeType.CONDITIONAL] = Field(NodeType.CONDITIONAL, const=True)

    @property
    def max_input_count(self) -> int:
        return 3

    @property
    def is_inplace_operation_in_sdk_code(self) -> bool:
        return True

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return inputs[0].series_output_dtype

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name_expr = var_name_expressions[0]
        mask_var_name_expr = var_name_expressions[1]
        statements, var_name = self._convert_expression_to_variable(
            var_name_expression=var_name_expr,
            var_name_generator=var_name_generator,
            node_output_type=operation_structure.output_type,
            node_output_category=operation_structure.output_category,
            to_associate_with_node_name=False,
        )
        mask_statements, mask_var_name = self._convert_expression_to_variable(
            var_name_expression=mask_var_name_expr,
            var_name_generator=var_name_generator,
            node_output_type=NodeOutputType.SERIES,
            node_output_category=operation_structure.output_category,
            to_associate_with_node_name=False,
            variable_name_prefix="mask",
        )
        statements.extend(mask_statements)
        # only make the copy if we are not generating info dict
        # if generating info dict, we will delay the copy to the assign node
        var_statements, output_var_name = self._convert_to_proper_variable_name(
            var_name=var_name,
            var_name_generator=var_name_generator,
            operation_structure=operation_structure,
            required_copy=context.required_copy and not context.as_info_dict,
            to_associate_with_node_name=True,
        )
        statements.extend(var_statements)

        value: RightHandSide = ValueStr.create(self.parameters.value)
        is_series_assignment = len(var_name_expressions) == 3
        if is_series_assignment:
            value = var_name_expressions[2]

        if context.as_info_dict:
            # This is to handle the case where `View[<column>][<condition>] = <value>` is used.
            # Since there is no single line in SDK code to generate conditional expression, we output info instead
            # and delay the generation of SDK code to the assign node. This method only generates the conditional part,
            # the assignment part will be generated in the assign node.
            info_dict_data: Dict[str, Any] = {"value": self.parameters.value, "mask": mask_var_name}
            if is_series_assignment:
                info_dict_data["value"] = value
                info_dict_data["is_series_assignment"] = True
            return statements, InfoDict(info_dict_data)

        # This handles the normal series assignment case where `col[<condition>] = <value>` is used. In this case,
        # the conditional assignment should not update their parent view.
        statements.append((VariableNameStr(f"{output_var_name}[{mask_var_name}]"), value))
        return statements, output_var_name
