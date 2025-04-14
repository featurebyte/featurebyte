"""
This module contains datetime operation related node classes
"""

# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
import textwrap
from abc import ABC, abstractmethod
from typing import Callable, ClassVar, Dict, List, Optional, Sequence, Set, Tuple, Union

import numpy as np
from pydantic import Field
from typing_extensions import Annotated, Literal

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
from featurebyte.query_graph.node.agg_func import construct_agg_func
from featurebyte.query_graph.node.base import BaseSeriesOutputNode
from featurebyte.query_graph.node.metadata.config import (
    OnDemandFunctionCodeGenConfig,
    OnDemandViewCodeGenConfig,
    SDKCodeGenConfig,
)
from featurebyte.query_graph.node.metadata.operation import AggregationColumn, OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerationContext,
    ExpressionStr,
    NodeCodeGenOutput,
    ObjectClass,
    StatementStr,
    StatementT,
    ValueStr,
    VariableNameGenerator,
    VariableNameStr,
    VarNameExpressionInfo,
    get_object_class_from_function_call,
)
from featurebyte.query_graph.sql.common import MISSING_VALUE_REPLACEMENT
from featurebyte.typing import Scalar


class BaseCountDictOpNode(BaseSeriesOutputNode, ABC):
    """BaseCountDictOpNode class"""

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def _get_count_dict_and_mask_variables(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
    ) -> Tuple[List[StatementT], VariableNameStr, VariableNameStr]:
        statements: List[StatementT] = []
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name_expression = var_name_expressions[0].as_input()
        mask_expr = ExpressionStr(f"~{var_name_expression}.isnull()")
        mask_var = var_name_generator.convert_to_variable_name(
            variable_name_prefix="feat_mask", node_name=None
        )
        statements.append((mask_var, mask_expr))
        var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="feat_count_dict", node_name=None
        )
        statements.append((var_name, ExpressionStr(f"{var_name_expression}[{mask_var}]")))
        return statements, var_name, mask_var

    def _derive_python_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        generate_expression_func: Callable[..., str],
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name_expression = var_name_expressions[0].as_input()
        other_operands = [val.as_input() for val in var_name_expressions[1:]]
        expression = ExpressionStr(
            generate_expression_func(operand=var_name_expression, other_operands=other_operands)
        )
        return [], expression

    def _derive_sdk_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = var_name_generator, operation_structure, config, context
        return self._derive_python_code(node_inputs, self.generate_expression)

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = var_name_generator, config
        return self._derive_python_code(node_inputs, self.generate_odfv_expression)

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

    def generate_odfv_expression(self, operand: str, other_operands: List[str]) -> str:
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
        # noqa: DAR202

        Raises
        ------
        RuntimeError
            If on-demand view code generation is not supported
        """
        raise RuntimeError("On-demand view code generation is not supported for this node")


class CountDictTransformNode(BaseCountDictOpNode):
    """CountDictTransformNode class"""

    class Parameters(FeatureByteBaseModel):
        """Parameters"""

        transform_type: Literal[
            "entropy", "most_frequent", "key_with_highest_value", "key_with_lowest_value"
        ]

    class UniqueCountParameters(FeatureByteBaseModel):
        """UniqueCountParameters"""

        transform_type: Literal["unique_count"]
        include_missing: bool

    type: Literal[NodeType.COUNT_DICT_TRANSFORM] = NodeType.COUNT_DICT_TRANSFORM
    parameters: Annotated[
        Union[Parameters, UniqueCountParameters], Field(discriminator="transform_type")
    ]

    transform_types_with_varchar_output: ClassVar[Set[str]] = {
        "most_frequent",
        "key_with_highest_value",
        "key_with_lowest_value",
    }

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        if self.parameters.transform_type in self.transform_types_with_varchar_output:
            return DBVarTypeInfo(dtype=DBVarType.VARCHAR)
        return DBVarTypeInfo(dtype=DBVarType.FLOAT)

    def generate_expression(self, operand: str, other_operands: List[str]) -> str:
        params = ""
        if isinstance(self.parameters, self.UniqueCountParameters):
            params = f"include_missing={ValueStr.create(self.parameters.include_missing)}"
        return f"{operand}.cd.{self.parameters.transform_type}({params})"

    @staticmethod
    def _get_entropy(
        count_dict_var_name: str, var_name_generator: VariableNameGenerator
    ) -> Tuple[List[StatementT], ExpressionStr]:
        count_expr = get_object_class_from_function_call(
            f"{count_dict_var_name}.apply",
            ExpressionStr("lambda x: np.array(list(x.values()))"),
        )
        count_var = var_name_generator.convert_to_variable_name(
            variable_name_prefix="feat_count", node_name=None
        )
        statements: List[StatementT] = [(count_var, count_expr)]
        entropy_expr = get_object_class_from_function_call(
            f"{count_var}.apply",
            ExpressionStr("sp.stats.entropy"),
        )
        return statements, ExpressionStr(entropy_expr)

    @staticmethod
    def _get_extreme_value_func_name(
        var_name_generator: VariableNameGenerator,
        operation: Literal["max", "min"] = "max",
    ) -> Tuple[List[StatementT], str]:
        statements: List[StatementT] = []
        func_name = f"extract_extreme_value_{operation}"
        if var_name_generator.should_insert_function(function_name=func_name):
            func_string = f"""
            def {func_name}(input_dict):
                if pd.isna(input_dict) or len(input_dict) == 0:
                    return np.nan
                return min(
                    [key for key, value in input_dict.items() if value == {operation}(input_dict.values())]
                )
            """
            statements.append(StatementStr(textwrap.dedent(func_string)))
        return statements, func_name

    def _get_extreme_value_key(
        self,
        count_dict_var_name: str,
        var_name_generator: VariableNameGenerator,
        operation: Literal["max", "min"] = "max",
    ) -> Tuple[List[StatementT], ExpressionStr]:
        statements, func_name = self._get_extreme_value_func_name(var_name_generator, operation)
        extreme_value_key_expr = get_object_class_from_function_call(
            f"{count_dict_var_name}.apply",
            ExpressionStr(func_name),
        )
        return statements, ExpressionStr(extreme_value_key_expr)

    @staticmethod
    def _get_unique_count(
        count_dict_var_name: str,
        var_name_generator: VariableNameGenerator,
        include_missing: bool,
    ) -> Tuple[List[StatementT], ExpressionStr]:
        _ = var_name_generator
        if include_missing:
            unique_count_expr = get_object_class_from_function_call(
                f"{count_dict_var_name}.apply",
                ExpressionStr("len"),
            )
        else:
            unique_count_expr = get_object_class_from_function_call(
                f"{count_dict_var_name}.apply",
                ExpressionStr(
                    f"lambda x: len([key for key in x if key != '{MISSING_VALUE_REPLACEMENT}'])"
                ),
            )
        return [], ExpressionStr(unique_count_expr)

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        statements, cd_var_name, mask_var = self._get_count_dict_and_mask_variables(
            node_inputs, var_name_generator
        )
        include_missing = False
        if isinstance(self.parameters, self.UniqueCountParameters):
            include_missing = self.parameters.include_missing
        transform_type_to_func: Dict[str, Callable[..., Tuple[List[StatementT], ExpressionStr]]] = {
            "entropy": self._get_entropy,
            "most_frequent": lambda var, gen: self._get_extreme_value_key(var, gen, "max"),
            "key_with_highest_value": lambda var, gen: self._get_extreme_value_key(var, gen, "max"),
            "key_with_lowest_value": lambda var, gen: self._get_extreme_value_key(var, gen, "min"),
            "unique_count": lambda var, gen: self._get_unique_count(var, gen, include_missing),
        }
        transform_type = self.parameters.transform_type
        op_statements, op_expr = transform_type_to_func[transform_type](
            cd_var_name, var_name_generator
        )
        statements.extend(op_statements)
        var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix=f"feat_{transform_type}", node_name=None
        )
        statements.append((var_name, op_expr))
        return statements, ExpressionStr(f"{var_name}.reindex({mask_var}.index)")

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        operand = var_name_expressions[0].as_input()

        include_missing = False
        if isinstance(self.parameters, self.UniqueCountParameters):
            include_missing = self.parameters.include_missing

        transform_type = self.parameters.transform_type
        statements: List[StatementT] = []
        if transform_type == "entropy":
            expr = ExpressionStr(
                f"sp.stats.entropy(list(({operand}).values())) if not pd.isna({operand}) else np.nan"
            )
        elif transform_type in {"most_frequent", "key_with_highest_value"}:
            statements, func_name = self._get_extreme_value_func_name(var_name_generator, "max")
            expr = ExpressionStr(f"{func_name}({operand})")
        elif transform_type == "key_with_lowest_value":
            statements, func_name = self._get_extreme_value_func_name(var_name_generator, "min")
            expr = ExpressionStr(f"{func_name}({operand})")
        elif transform_type == "unique_count" and include_missing:
            expr = ExpressionStr(f"len({operand}) if not pd.isna({operand}) else np.nan")
        elif transform_type == "unique_count" and not include_missing:
            expr = ExpressionStr(
                f"len([key for key in {operand} if key != '{MISSING_VALUE_REPLACEMENT}']) "
                f"if not pd.isna({operand}) else np.nan"
            )
        else:
            raise ValueError(f"Unsupported transform type: {transform_type}")
        return statements, expr


class CosineSimilarityNode(BaseCountDictOpNode):
    """CosineSimilarityNode class"""

    type: Literal[NodeType.COSINE_SIMILARITY] = NodeType.COSINE_SIMILARITY

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.FLOAT)

    def generate_expression(self, operand: str, other_operands: List[str]) -> str:
        return f"{operand}.cd.cosine_similarity(other={other_operands[0]})"

    def generate_odfv_expression(self, operand: str, other_operands: List[str]) -> str:
        lambda_func = (
            "lambda d1, d2: np.nan if pd.isna(d1) or pd.isna(d2) else cosine_similarity(d1, d2)"
        )
        return f"{operand}.combine({other_operands[0]}, {lambda_func})"

    @staticmethod
    def _get_cosine_similarity_function_name(
        var_name_generator: VariableNameGenerator,
    ) -> Tuple[List[StatementT], str]:
        statements: List[StatementT] = []
        func_name = "cosine_similarity"
        if var_name_generator.should_insert_function(function_name=func_name):
            func_string = f"""
            def {func_name}(dict1, dict2):
                if pd.isna(dict1) or pd.isna(dict2):
                    return np.nan
                if len(dict1) == 0 or len(dict2) == 0:
                    return 0.0
                all_keys = set(dict1.keys()).union(dict2.keys())
                series1 = pd.Series(dict1).reindex(all_keys, fill_value=0)
                series2 = pd.Series(dict2).reindex(all_keys, fill_value=0)
                return 1 - sp.spatial.distance.cosine(series1, series2)
            """
            statements.append(StatementStr(textwrap.dedent(func_string)))
        return statements, func_name

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        statements, _ = self._get_cosine_similarity_function_name(
            var_name_generator=var_name_generator
        )

        # compute cosine similarity
        odfv_stats, output_var_name = super()._derive_on_demand_view_code(
            node_inputs, var_name_generator, config
        )
        statements.extend(odfv_stats)
        return statements, output_var_name

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        statements, func_name = self._get_cosine_similarity_function_name(
            var_name_generator=var_name_generator
        )
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        left_operand = input_var_name_expressions[0].as_input()
        right_operand = input_var_name_expressions[1].as_input()
        expr = ExpressionStr(f"{func_name}({left_operand}, {right_operand})")
        return statements, expr


class DictionaryKeysNode(BaseSeriesOutputNode):
    """Dictionary keys node class"""

    type: Literal[NodeType.DICTIONARY_KEYS] = NodeType.DICTIONARY_KEYS

    @property
    def max_input_count(self) -> int:
        return 1

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.ARRAY)

    def _derive_sdk_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        # This node is introduced when IS_IN is used on a dictionary feature. There is no need to
        # generate any code for this node and the actual code generation will be done by the IS_IN node.
        return [], node_inputs[0].var_name_or_expr

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        statements: List[StatementT] = []
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name: str = input_var_name_expressions[0].as_input()
        keys_expr = get_object_class_from_function_call(
            f"{var_name}.apply",
            ExpressionStr("lambda x: np.nan if pd.isna(x) else list(x.keys())"),
        )
        return statements, ExpressionStr(keys_expr)

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name: str = input_var_name_expressions[0].as_input()
        return [], ExpressionStr(f"np.nan if pd.isna({var_name}) else list({var_name}.keys())")


class BaseCountDictWithKeyOpNode(BaseCountDictOpNode, ABC):
    """Base class for count dictionary operation with key"""

    class Parameters(FeatureByteBaseModel):
        """Parameters"""

        value: Optional[Scalar] = Field(default=None)

    parameters: Parameters

    def get_key_value(self) -> ValueStr:
        """
        Get key value as ValueStr from node parameters

        Returns
        -------
        ValueStr
        """
        # Note that the key of the dictionary/map is always a string for all supported data warehouses.
        if isinstance(self.parameters.value, (int, float, np.integer, np.floating)):
            param = ValueStr.create(str(int(self.parameters.value)))
        else:
            # If it is a string, it is already quoted.
            # If it is other types, the dictionary lookup will return null.
            param = ValueStr.create(self.parameters.value)
        return param

    @staticmethod
    def get_key_value_func_name(
        var_name_generator: VariableNameGenerator,
    ) -> Tuple[List[StatementT], str]:
        """
        Create a function statement & get the function name for getting key value

        Parameters
        ----------
        var_name_generator: VariableNameGenerator
            Variable name generator

        Returns
        -------
        Tuple[List[StatementT], str]
        """
        statements: List[StatementT] = []
        func_name = "get_key_value"
        if var_name_generator.should_insert_function(function_name=func_name):
            func_string = f"""
            def {func_name}(key):
                if pd.isna(key):
                    return key
                if isinstance(key, (int, float, np.integer, np.floating)):
                    return str(int(key))
                return key
            """
            statements.append(StatementStr(textwrap.dedent(func_string)))
        return statements, func_name


class GetValueFromDictionaryNode(BaseCountDictWithKeyOpNode):
    """Get value from dictionary node class"""

    type: Literal[NodeType.GET_VALUE] = NodeType.GET_VALUE

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        aggregations = inputs[0].aggregations
        agg_column = aggregations[0]
        # This assumes that dictionary features are never post-processed.
        assert isinstance(agg_column, AggregationColumn)
        method = agg_column.method
        assert method is not None
        agg_func = construct_agg_func(method)
        # derive the output_dtype_info using aggregation's parent column without passing category parameter
        # as count method doesn't have any parent column, take the first input column as parent column
        parent_column = agg_column.column
        if parent_column is None:
            parent_column = inputs[0].columns[0]
        return agg_func.derive_output_dtype_info(parent_column.dtype_info, category=None)

    def generate_expression(self, operand: str, other_operands: List[str]) -> str:
        param = other_operands[0] if other_operands else self.get_key_value()
        return f"{operand}.cd.get_value(key={param})"

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name: str = input_var_name_expressions[0].as_input()
        value_expr: Union[str, ObjectClass]
        statements: List[StatementT] = []
        if len(node_inputs) == 1:
            value_expr = get_object_class_from_function_call(
                f"{var_name}.apply",
                ExpressionStr(f"lambda x: np.nan if pd.isna(x) else x.get({self.get_key_value()})"),
            )
        else:
            func_statements, key_func_name = self.get_key_value_func_name(var_name_generator)
            statements.extend(func_statements)
            operand: str = input_var_name_expressions[1].as_input()
            value_expr = (
                f"{var_name}.combine({operand}, "
                f"lambda x, y: np.nan if pd.isna(x) or pd.isna(y) else x.get({key_func_name}(y)))"
            )

        return statements, ExpressionStr(value_expr)

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name: str = input_var_name_expressions[0].as_input()
        statements: List[StatementT] = []
        if len(node_inputs) == 1:
            value_expr = ExpressionStr(
                f"np.nan if pd.isna({var_name}) else {var_name}.get({self.get_key_value()})"
            )
        else:
            func_statements, key_func_name = self.get_key_value_func_name(var_name_generator)
            statements.extend(func_statements)
            operand: str = input_var_name_expressions[1].as_input()
            value_expr = ExpressionStr(
                f"np.nan if pd.isna({var_name}) else {var_name}.get({key_func_name}({operand}))"
            )

        return statements, value_expr


class GetRankFromDictionaryNode(BaseCountDictWithKeyOpNode):
    """Get rank from dictionary node class"""

    class Parameters(BaseCountDictWithKeyOpNode.Parameters):
        """Parameters"""

        descending: bool = False

    type: Literal[NodeType.GET_RANK] = NodeType.GET_RANK
    parameters: Parameters

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.FLOAT)

    def generate_expression(self, operand: str, other_operands: List[str]) -> str:
        key = other_operands[0] if other_operands else self.get_key_value()
        descending = ValueStr.create(self.parameters.descending)
        params = f"key={key}, descending={descending}"
        return f"{operand}.cd.get_value(key={params})"

    @staticmethod
    def _get_rank_func_name(
        var_name_generator: VariableNameGenerator,
    ) -> Tuple[List[StatementT], str]:
        statements: List[StatementT] = []
        func_name = "get_rank"
        if var_name_generator.should_insert_function(function_name=func_name):
            func_string = f"""
            def {func_name}(input_dict, key, is_descending):
                if pd.isna(input_dict) or pd.isna(key):
                    return np.nan
                key = str(int(key)) if isinstance(key, (int, float, np.integer, np.floating)) else key
                if key not in input_dict:
                    return np.nan
                sorted_values = sorted(input_dict.values(), reverse=is_descending)
                return sorted_values.index(input_dict[key]) + 1
            """
            statements.append(StatementStr(textwrap.dedent(func_string)))
        return statements, func_name

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name: str = input_var_name_expressions[0].as_input()
        descending = ValueStr.create(self.parameters.descending)
        statements, func_name = self._get_rank_func_name(var_name_generator)
        if len(node_inputs) == 1:
            rank_expr = ExpressionStr(
                get_object_class_from_function_call(
                    f"{var_name}.apply",
                    ExpressionStr(
                        f"lambda dct: {func_name}(dct, key={self.get_key_value()}, is_descending={descending})"
                    ),
                )
            )
        else:
            func_statements, key_func_name = self.get_key_value_func_name(var_name_generator)
            statements.extend(func_statements)
            operand: str = input_var_name_expressions[1].as_input()
            rank_expr = ExpressionStr(
                f"{var_name}.combine({operand}, "
                f"lambda dct, key: {func_name}(dct, key={key_func_name}(key), is_descending={descending}))"
            )

        return statements, rank_expr

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name: str = input_var_name_expressions[0].as_input()
        descending = ValueStr.create(self.parameters.descending)
        statements, func_name = self._get_rank_func_name(var_name_generator)
        if len(node_inputs) == 1:
            rank_expr = ExpressionStr(
                f"{func_name}({var_name}, key={self.get_key_value()}, is_descending={descending})"
            )
        else:
            func_statements, key_func_name = self.get_key_value_func_name(var_name_generator)
            statements.extend(func_statements)
            operand: str = input_var_name_expressions[1].as_input()
            rank_expr = ExpressionStr(
                f"{func_name}({var_name}, key={key_func_name}({operand}), is_descending={descending})"
            )

        return statements, rank_expr


class GetRelativeFrequencyFromDictionaryNode(BaseCountDictWithKeyOpNode):
    """Get relative frequency from dictionary node class"""

    type: Literal[NodeType.GET_RELATIVE_FREQUENCY] = NodeType.GET_RELATIVE_FREQUENCY

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.FLOAT)

    def generate_expression(self, operand: str, other_operands: List[str]) -> str:
        param = other_operands[0] if other_operands else self.get_key_value()
        return f"{operand}.cd.get_relative_frequency(key={param})"

    @staticmethod
    def _get_relative_frequency_func_name(
        var_name_generator: VariableNameGenerator,
    ) -> Tuple[List[StatementT], str]:
        statements: List[StatementT] = []
        func_name = "get_relative_frequency"
        if var_name_generator.should_insert_function(function_name=func_name):
            func_string = f"""
            def {func_name}(input_dict, key):
                if pd.isna(input_dict) or pd.isna(key):
                    return np.nan
                key = str(int(key)) if isinstance(key, (int, float, np.integer, np.floating)) else key
                if key not in input_dict:
                    return np.nan
                total_count = sum(input_dict.values())
                if total_count == 0:
                    return 0
                key_frequency = input_dict.get(key, 0)
                return key_frequency / total_count
            """
            statements.append(StatementStr(textwrap.dedent(func_string)))
        return statements, func_name

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name: str = input_var_name_expressions[0].as_input()
        statements, func_name = self._get_relative_frequency_func_name(var_name_generator)
        if len(node_inputs) == 1:
            param = ValueStr.create(self.parameters.value)
            freq_expr = ExpressionStr(
                get_object_class_from_function_call(
                    f"{var_name}.apply",
                    ExpressionStr(f"lambda dct: {func_name}(dct, key={param})"),
                )
            )
        else:
            operand: str = input_var_name_expressions[1].as_input()
            freq_expr = ExpressionStr(
                f"{var_name}.combine({operand}, lambda dct, key: {func_name}(dct, key=key))"
            )

        return statements, freq_expr

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name: str = input_var_name_expressions[0].as_input()
        statements, func_name = self._get_relative_frequency_func_name(var_name_generator)
        if len(node_inputs) == 1:
            param = ValueStr.create(self.parameters.value)
            freq_expr = ExpressionStr(f"{func_name}({var_name}, key={param})")
        else:
            operand: str = input_var_name_expressions[1].as_input()
            freq_expr = ExpressionStr(f"{func_name}({var_name}, key={operand})")

        return statements, freq_expr
