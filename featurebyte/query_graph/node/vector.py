"""
Vector node module
"""

import textwrap
from typing import List, Sequence, Tuple

from typing_extensions import Literal

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
from featurebyte.query_graph.node.base import BaseSeriesOutputNode
from featurebyte.query_graph.node.metadata.config import (
    OnDemandFunctionCodeGenConfig,
    OnDemandViewCodeGenConfig,
    SDKCodeGenConfig,
)
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerationContext,
    ExpressionStr,
    NodeCodeGenOutput,
    StatementStr,
    StatementT,
    VariableNameGenerator,
    VarNameExpressionInfo,
)


class VectorCosineSimilarityNode(BaseSeriesOutputNode):
    """VectorCosineSimilarityNode class"""

    type: Literal[NodeType.VECTOR_COSINE_SIMILARITY] = NodeType.VECTOR_COSINE_SIMILARITY

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.FLOAT)

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
        var_name_expression = var_name_expressions[0].as_input()
        other_operands = [val.as_input() for val in var_name_expressions[1:]]
        expression = ExpressionStr(
            f"{var_name_expression}.vec.cosine_similarity(other={other_operands[0]})"
        )
        return [], expression

    @staticmethod
    def _get_vector_cosine_similarity_function_name(
        var_name_generator: VariableNameGenerator,
    ) -> Tuple[List[StatementT], str]:
        statements: List[StatementT] = []
        func_name = "vector_cosine_similarity"
        if var_name_generator.should_insert_function(function_name=func_name):
            func_string = f"""
            def {func_name}(vec1, vec2):
                if not isinstance(vec1, (np.ndarray, list)) and pd.isna(vec1):
                    return 0
                if not isinstance(vec2, (np.ndarray, list)) and pd.isna(vec2):
                    return 0
                if len(vec1) != len(vec2):
                    raise ValueError("Vector lengths must be equal")
                if len(vec1) == 0 or len(vec2) == 0:
                    return 0

                dot_product = np.dot(vec1, vec2)
                magnitude = np.linalg.norm(vec1) * np.linalg.norm(vec2)
                return dot_product / magnitude if magnitude != 0 else np.nan
            """
            statements.append(StatementStr(textwrap.dedent(func_string)))
        return statements, func_name

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        statements, func_name = self._get_vector_cosine_similarity_function_name(
            var_name_generator=var_name_generator
        )
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        left_operand = input_var_name_expressions[0].as_input()
        right_operand = input_var_name_expressions[1].as_input()
        expr = ExpressionStr(f"{left_operand}.combine({right_operand}, {func_name})")
        return statements, expr

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        statements, func_name = self._get_vector_cosine_similarity_function_name(
            var_name_generator=var_name_generator
        )
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        left_operand = input_var_name_expressions[0].as_input()
        right_operand = input_var_name_expressions[1].as_input()
        expr = ExpressionStr(f"{func_name}({left_operand}, {right_operand})")
        return statements, expr
