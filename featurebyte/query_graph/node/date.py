"""
This module contains datetime operation related node classes
"""

# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
import textwrap
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Union

from pydantic import Field
from typing_extensions import Literal

from featurebyte.enum import DBVarType, SourceType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeInfo, DBVarTypeMetadata
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema, TimestampTupleSchema
from featurebyte.query_graph.node.base import (
    BaseSeriesOutputNode,
    BaseSeriesOutputWithSingleOperandNode,
)
from featurebyte.query_graph.node.metadata.config import (
    OnDemandFunctionCodeGenConfig,
    OnDemandViewCodeGenConfig,
    SDKCodeGenConfig,
)
from featurebyte.query_graph.node.metadata.operation import NodeOutputCategory, OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    CodeGenerationContext,
    ExpressionStr,
    NodeCodeGenOutput,
    StatementStr,
    StatementT,
    ValueStr,
    VariableNameGenerator,
    VariableNameStr,
    VarNameExpressionInfo,
    get_object_class_from_function_call,
)
from featurebyte.session.time_formatter import convert_time_format
from featurebyte.typing import DatetimeSupportedPropertyType, TimedeltaSupportedUnitType


def generate_to_datetime_expression(
    operand_expr: str,
    operation_structure: OperationStructure,
    timestamp_schema: Optional[TimestampSchema],
    source_type: SourceType,
    mode: Literal["odfv", "udf"],
) -> str:
    """
    Generate to_datetime expression for the given operand expression.

    Parameters
    ----------
    operand_expr: str
        Operand expression
    operation_structure: OperationStructure
        Operation structure
    timestamp_schema: Optional[TimestampSchema]
        Timestamp schema
    source_type: SourceType
        Source type
    mode: Literal["odfv", "udf"]
        Mode of operation, either "odfv" or "udf"

    Returns
    -------
    str
        to_datetime expression
    """
    to_datetime_params = ""
    tz_conversion = ""
    dt_acc = ".dt" if mode == "odfv" else ""
    if timestamp_schema:
        if operation_structure.series_output_dtype_info.dtype == DBVarType.VARCHAR:
            assert timestamp_schema.format_string is not None
            py_format_string = convert_time_format(
                source_type=source_type, format_string=timestamp_schema.format_string
            )
            to_datetime_params = f', format="{py_format_string}"'

        if timestamp_schema.timezone and timestamp_schema.timezone_offset_column_name is None:
            tz_conversion += (
                f'{dt_acc}.tz_localize("{timestamp_schema.timezone}"){dt_acc}.tz_convert("UTC")'
            )

    if tz_conversion:
        # utc=True should not be used in to_datetime
        output_expr = f"pd.to_datetime({operand_expr}{to_datetime_params}){tz_conversion}"
    else:
        output_expr = f"pd.to_datetime({operand_expr}{to_datetime_params}, utc=True)"
    return output_expr


class DatetimeExtractNodeParameters(FeatureByteBaseModel):
    """Parameters"""

    property: DatetimeSupportedPropertyType
    timezone_offset: Optional[str] = Field(default=None)
    timestamp_metadata: Optional[DBVarTypeMetadata] = Field(default=None)

    @property  # type: ignore
    def timestamp_schema(self) -> Optional[TimestampSchema]:
        """
        Timestamp schema

        Returns
        -------
        Optional[TimestampSchema]
            Timestamp schema
        """
        if self.timestamp_metadata:
            return self.timestamp_metadata.timestamp_schema
        return None

    @property  # type: ignore
    def timestamp_tuple_schema(self) -> Optional[TimestampTupleSchema]:
        """
        Timestamp tuple schema

        Returns
        -------
        Optional[TimestampTupleSchema]
            Timestamp tuple schema
        """
        if self.timestamp_metadata:
            return self.timestamp_metadata.timestamp_tuple_schema
        return None


class DatetimeExtractNode(BaseSeriesOutputNode):
    """DatetimeExtractNode class"""

    type: Literal[NodeType.DT_EXTRACT] = NodeType.DT_EXTRACT
    parameters: DatetimeExtractNodeParameters

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.INT)

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def _derive_sdk_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        ts_operand: str = var_name_expressions[0].as_input()

        offset_operand: Optional[str]
        if self.parameters.timezone_offset is not None:
            offset_operand = ValueStr.create(self.parameters.timezone_offset).as_input()
        elif len(var_name_expressions) == 2:
            offset_operand = var_name_expressions[1].as_input()
        else:
            offset_operand = None

        date_property: str = self.parameters.property
        if date_property == "dayofweek":
            date_property = "day_of_week"

        output: VarNameExpressionInfo
        if offset_operand is None:
            output = VariableNameStr(f"{ts_operand}.dt.{date_property}")
        else:
            output = ExpressionStr(f"{ts_operand}.dt.tz_offset({offset_operand}).{date_property}")

        return [], output

    def _derive_on_demand_view_or_user_defined_function_helper(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        offset_adj_var_name_prefix: str,
        expr_func: Callable[[str, str], str],
        source_type: SourceType,
        mode: Literal["odfv", "udf"],
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)

        statements: List[StatementT] = []
        offset_operand: Optional[Union[str, VariableNameStr]]
        if self.parameters.timezone_offset is not None:
            delta_val = f"{self.parameters.timezone_offset}:00"
            delta = get_object_class_from_function_call("pd.to_timedelta", delta_val)
            offset_operand = var_name_generator.convert_to_variable_name(
                variable_name_prefix="tz_offset", node_name=None
            )
            statements.append((offset_operand, delta))
        elif len(var_name_expressions) == 2:
            offset_operand = f"pd.to_timedelta({var_name_expressions[1].as_input()})"
        else:
            offset_operand = None

        dt_var_name: Union[str, VariableNameStr]
        ts_operand: str = var_name_expressions[0].as_input()
        dt_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix=offset_adj_var_name_prefix, node_name=None
        )
        to_datetime_expr = generate_to_datetime_expression(
            operand_expr=ts_operand,
            operation_structure=node_inputs[0].operation_structure,
            timestamp_schema=self.parameters.timestamp_schema,
            source_type=source_type,
            mode=mode,
        )
        if offset_operand:
            expr = ExpressionStr(f"{to_datetime_expr} + {offset_operand}")
            statements.append((dt_var_name, expr))
        else:
            if self.parameters.timestamp_schema:
                dt_var_name = ExpressionStr(to_datetime_expr)
            else:
                dt_var_name = ts_operand

        output = ExpressionStr(expr_func(dt_var_name, self.parameters.property))
        return statements, output

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        def _datetime_proper_expr(dt_var_name: str, prop: str) -> str:
            if prop == "week":
                return f"pd.to_datetime({dt_var_name}, utc=True).dt.isocalendar().week"
            return f"pd.to_datetime({dt_var_name}, utc=True).dt.{prop}"

        return self._derive_on_demand_view_or_user_defined_function_helper(
            node_inputs,
            var_name_generator,
            offset_adj_var_name_prefix="feat_dt",
            expr_func=_datetime_proper_expr,
            source_type=config.source_type,
            mode="odfv",
        )

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        return self._derive_on_demand_view_or_user_defined_function_helper(
            node_inputs,
            var_name_generator,
            offset_adj_var_name_prefix="feat",
            expr_func=lambda dt_var_name, prop: f"pd.to_datetime({dt_var_name}, utc=True).{prop}",
            source_type=config.source_type,
            mode="udf",
        )


class TimeDeltaExtractNode(BaseSeriesOutputWithSingleOperandNode):
    """TimeDeltaExtractNode class"""

    class Parameters(FeatureByteBaseModel):
        """Parameters"""

        property: TimedeltaSupportedUnitType

    type: Literal[NodeType.TIMEDELTA_EXTRACT] = NodeType.TIMEDELTA_EXTRACT
    parameters: Parameters

    @property
    def unit_to_seconds(self) -> Dict[str, int]:
        """
        Mapping from unit to seconds

        Returns
        -------
        Dict[str, int]
            mapping from unit to seconds
        """
        return {
            "day": 24 * 60 * 60,
            "hour": 60 * 60,
            "minute": 60,
        }

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.FLOAT)

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.dt.{self.parameters.property}"

    def generate_odfv_expression(self, operand: str) -> str:
        if self.parameters.property == "millisecond":
            return f"1e3 * pd.to_timedelta({operand}).dt.total_seconds()"
        if self.parameters.property == "microsecond":
            return f"1e6 * pd.to_timedelta({operand}).dt.total_seconds()"
        if self.parameters.property == "second":
            return f"pd.to_timedelta({operand}).dt.total_seconds()"

        return f"pd.to_timedelta({operand}).dt.total_seconds() / {self.unit_to_seconds[self.parameters.property]}"

    def generate_udf_expression(self, operand: str) -> str:
        if self.parameters.property == "millisecond":
            return f"1e3 * pd.to_timedelta({operand}).total_seconds()"
        if self.parameters.property == "microsecond":
            return f"1e6 * pd.to_timedelta({operand}).total_seconds()"
        if self.parameters.property == "second":
            return f"pd.to_timedelta({operand}).total_seconds()"

        return f"pd.to_timedelta({operand}).total_seconds() / {self.unit_to_seconds[self.parameters.property]}"


class DateDifferenceParameters(FeatureByteBaseModel):
    """Parameters for DateDifferenceNode"""

    left_timestamp_metadata: Optional[DBVarTypeMetadata] = Field(default=None)
    right_timestamp_metadata: Optional[DBVarTypeMetadata] = Field(default=None)

    @property
    def left_timestamp_schema(self) -> Optional[TimestampSchema]:
        """
        Left timestamp schema

        Returns
        -------
        Optional[TimestampSchema]
            Left timestamp schema
        """
        if self.left_timestamp_metadata:
            return self.left_timestamp_metadata.timestamp_schema
        return None

    @property
    def right_timestamp_schema(self) -> Optional[TimestampSchema]:
        """
        Right timestamp schema

        Returns
        -------
        Optional[TimestampSchema]
            Right timestamp schema
        """
        if self.right_timestamp_metadata:
            return self.right_timestamp_metadata.timestamp_schema
        return None

    @property
    def left_timestamp_tuple_schema(self) -> Optional[TimestampTupleSchema]:
        """
        Left timestamp tuple schema

        Returns
        -------
        Optional[TimestampTupleSchema]
            Left timestamp tuple schema
        """
        if self.left_timestamp_metadata:
            return self.left_timestamp_metadata.timestamp_tuple_schema
        return None

    @property
    def right_timestamp_tuple_schema(self) -> Optional[TimestampTupleSchema]:
        """
        Right timestamp tuple schema

        Returns
        -------
        Optional[TimestampTupleSchema]
            Right timestamp tuple schema
        """
        if self.right_timestamp_metadata:
            return self.right_timestamp_metadata.timestamp_tuple_schema
        return None


class DateDifferenceNode(BaseSeriesOutputNode):
    """DateDifferenceNode class"""

    type: Literal[NodeType.DATE_DIFF] = NodeType.DATE_DIFF
    parameters: DateDifferenceParameters

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.TIMEDELTA)

    def _derive_python_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        mode: Literal["sdk", "odfv", "udf"],
        source_type: Optional[SourceType] = None,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        if len(node_inputs) == 1:
            # we don't allow subtracting timestamp with a scalar timedelta through SDK
            raise RuntimeError("DateAddNode with only one input is not supported")

        var_name_expressions = self._assert_no_info_dict(node_inputs)
        left_operand = var_name_expressions[0].as_input()
        right_operand = var_name_expressions[1].as_input()
        statements: List[StatementT] = []
        if mode == "sdk":
            expr = ExpressionStr(f"{left_operand} - {right_operand}")
        else:
            assert source_type is not None
            left_expr = generate_to_datetime_expression(
                operand_expr=left_operand,
                operation_structure=node_inputs[0].operation_structure,
                timestamp_schema=self.parameters.left_timestamp_schema,
                source_type=source_type,
                mode=mode,
            )
            right_expr = generate_to_datetime_expression(
                operand_expr=right_operand,
                operation_structure=node_inputs[1].operation_structure,
                timestamp_schema=self.parameters.right_timestamp_schema,
                source_type=source_type,
                mode=mode,
            )
            expr = ExpressionStr(f"{left_expr} - {right_expr}")
        return statements, expr

    def _derive_sdk_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = var_name_generator, operation_structure, config, context
        return self._derive_python_code(node_inputs, mode="sdk")

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = var_name_generator, config
        return self._derive_python_code(node_inputs, mode="odfv", source_type=config.source_type)

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = var_name_generator, config
        return self._derive_python_code(node_inputs, mode="udf", source_type=config.source_type)


class TimeDeltaNode(BaseSeriesOutputNode):
    """TimeDeltaNode class"""

    class Parameters(FeatureByteBaseModel):
        """Parameters"""

        unit: TimedeltaSupportedUnitType

    type: Literal[NodeType.TIMEDELTA] = NodeType.TIMEDELTA
    parameters: Parameters

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.TIMEDELTA)

    def _derive_python_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        node_output_type: NodeOutputType,
        node_output_category: NodeOutputCategory,
        timedelta_func: Union[ClassEnum, str],
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name_expression = var_name_expressions[0]
        statements: List[StatementT] = []
        var_name = var_name_generator.generate_variable_name(
            node_output_type=node_output_type,
            node_output_category=node_output_category,
            node_name=self.name,
        )
        if isinstance(timedelta_func, ClassEnum):
            obj = timedelta_func(var_name_expression, unit=self.parameters.unit)
        else:
            obj = get_object_class_from_function_call(
                timedelta_func, var_name_expression, unit=self.parameters.unit
            )
        statements.append((var_name, obj))
        return statements, var_name

    def _derive_sdk_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = config, context
        return self._derive_python_code(
            node_inputs,
            var_name_generator,
            operation_structure.output_type,
            operation_structure.output_category,
            ClassEnum.TO_TIMEDELTA,
        )

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = config
        return self._derive_python_code(
            node_inputs,
            var_name_generator,
            NodeOutputType.SERIES,
            NodeOutputCategory.FEATURE,
            "pd.to_timedelta",
        )

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = config
        return self._derive_python_code(
            node_inputs,
            var_name_generator,
            NodeOutputType.SERIES,
            NodeOutputCategory.FEATURE,
            "pd.to_timedelta",
        )


class DateAddNode(BaseSeriesOutputNode):
    """DateAddNode class"""

    class Parameters(FeatureByteBaseModel):
        """Parameters"""

        value: Optional[int] = Field(default=None)
        left_timestamp_metadata: Optional[DBVarTypeMetadata] = Field(default=None)

        @property
        def left_timestamp_schema(self) -> Optional[TimestampSchema]:
            """
            Left timestamp schema

            Returns
            -------
            Optional[TimestampSchema]
                Left timestamp schema
            """
            if self.left_timestamp_metadata:
                return self.left_timestamp_metadata.timestamp_schema
            return None

    type: Literal[NodeType.DATE_ADD] = NodeType.DATE_ADD
    parameters: Parameters

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        if inputs[0].output_category == NodeOutputCategory.FEATURE:
            # when the inputs[0] is requested column, inputs[0].columns is empty.
            # in this case, we should derive the var type from inputs[0].aggregations
            return inputs[0].aggregations[0].dtype_info
        return inputs[0].columns[0].dtype_info

    def _derive_python_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        sdk_code: bool,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        if len(node_inputs) == 1:
            # we don't allow adding timestamp with a scalar timedelta through SDK
            raise RuntimeError("DateAddNode with only one input is not supported")

        var_name_expressions = self._assert_no_info_dict(node_inputs)
        left_operand: str = var_name_expressions[0].as_input()
        right_operand = var_name_expressions[1].as_input()
        statements: List[StatementT] = []
        if sdk_code:
            expr = ExpressionStr(f"{left_operand} + {right_operand}")
        else:
            expr = ExpressionStr(
                f"pd.to_datetime({left_operand}, utc=True) + pd.to_timedelta({right_operand})"
            )
        return statements, expr

    def _derive_sdk_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = var_name_generator, operation_structure, config, context
        return self._derive_python_code(node_inputs, sdk_code=True)

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = var_name_generator, config
        return self._derive_python_code(node_inputs, sdk_code=False)

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = var_name_generator, config
        return self._derive_python_code(node_inputs, sdk_code=False)


class ToTimestampFromEpochNode(BaseSeriesOutputNode):
    """ToTimestampFromEpochNode class"""

    class Parameters(FeatureByteBaseModel):
        """Parameters"""

    type: Literal[NodeType.TO_TIMESTAMP_FROM_EPOCH] = NodeType.TO_TIMESTAMP_FROM_EPOCH
    parameters: Parameters

    @property
    def max_input_count(self) -> int:
        return 1

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.TIMESTAMP)

    def _derive_sdk_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        statements: List[StatementT] = []
        var_name = var_name_generator.generate_variable_name(
            node_output_type=operation_structure.output_type,
            node_output_category=operation_structure.output_category,
            node_name=self.name,
        )
        obj = ClassEnum.TO_TIMESTAMP_FROM_EPOCH(var_name_expressions[0])
        statements.append((var_name, obj))
        return statements, var_name

    def _derive_on_demand_view_or_function_code_helper(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        statements: List[StatementT] = []
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        func_name = "to_datetime_from_epoch"
        if var_name_generator.should_insert_function(function_name=func_name):
            func_string = f"""
            def {func_name}(values):
                return pd.to_datetime(values, unit='s', utc=True)
            """
            statements.append(StatementStr(textwrap.dedent(func_string)))

        values = input_var_name_expressions[0]
        dist_expr = ExpressionStr(f"{func_name}({values})")
        return statements, dist_expr

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        return self._derive_on_demand_view_or_function_code_helper(
            node_inputs=node_inputs, var_name_generator=var_name_generator
        )

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        return self._derive_on_demand_view_or_function_code_helper(
            node_inputs=node_inputs, var_name_generator=var_name_generator
        )
