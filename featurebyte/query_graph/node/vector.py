"""
Vector node module
"""
from typing import List, Literal, Sequence, Tuple

from pydantic import Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import BaseSeriesOutputNode
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerationConfig,
    CodeGenerationContext,
    ExpressionStr,
    StatementT,
    VariableNameGenerator,
    VarNameExpressionInfo,
)


class VectorCosineSimilarityNode(BaseSeriesOutputNode):
    """VectorCosineSimilarityNode class"""

    type: Literal[NodeType.VECTOR_COSINE_SIMILARITY] = Field(
        NodeType.VECTOR_COSINE_SIMILARITY, const=True
    )

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT

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
            f"{var_name_expression}.vec.cosine_similarity(other={other_operands[0]})"
        )
        return [], expression
