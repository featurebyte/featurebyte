"""
Distance node module
"""
from typing import List, Literal, Sequence, Tuple

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import BaseSeriesOutputNode
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    CodeGenerationConfig,
    CodeGenerationContext,
    StatementT,
    VariableNameGenerator,
    VarNameExpressionInfo,
)


class Haversine(BaseSeriesOutputNode):
    """Haversine class"""

    class Parameters(BaseModel):
        """Parameters"""

    type: Literal[NodeType.HAVERSINE] = Field(NodeType.HAVERSINE, const=True)
    parameters: Parameters

    @property
    def max_input_count(self) -> int:
        return 4

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        statements: List[StatementT] = []
        var_name = var_name_generator.generate_variable_name(
            node_output_type=operation_structure.output_type,
            node_output_category=operation_structure.output_category,
            node_name=self.name,
        )
        obj = ClassEnum.HAVERSINE(
            lat_series_1=var_name_expressions[0],
            lon_series_1=var_name_expressions[1],
            lat_series_2=var_name_expressions[2],
            lon_series_2=var_name_expressions[3],
        )
        statements.append((var_name, obj))
        return statements, var_name
