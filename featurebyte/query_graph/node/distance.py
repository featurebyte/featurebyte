"""
Distance node module
"""
from typing import List, Literal, Sequence, Tuple

import textwrap

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import BaseSeriesOutputNode
from featurebyte.query_graph.node.metadata.config import OnDemandViewCodeGenConfig, SDKCodeGenConfig
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    CodeGenerationContext,
    ExpressionStr,
    StatementStr,
    StatementT,
    VariableNameGenerator,
    VarNameExpressionInfo,
)


class HaversineNode(BaseSeriesOutputNode):
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
        obj = ClassEnum.HAVERSINE(
            lat_series_1=var_name_expressions[0],
            lon_series_1=var_name_expressions[1],
            lat_series_2=var_name_expressions[2],
            lon_series_2=var_name_expressions[3],
        )
        statements.append((var_name, obj))
        return statements, var_name

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        statements: List[StatementT] = []
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        func_name = "haversine_distance"
        if func_name not in var_name_generator.var_name_counter:
            # add custom function if it doesn't exist
            func_name = var_name_generator.convert_to_variable_name(
                variable_name_prefix=func_name, node_name=None
            )
            func_string = f"""
            def {func_name}(lat1, lon1, lat2, lon2):
                R = 6371.0
                lat1_rad, lon1_rad = np.radians(lat1), np.radians(lon1)
                lat2_rad, lon2_rad = np.radians(lat2), np.radians(lon2)
                dlat = lat2_rad - lat1_rad
                dlon = lon2_rad - lon1_rad
                a = np.sin(dlat / 2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2)**2
                c = 2 * np.arcsin(np.sqrt(a))
                distance = R * c
                return distance
            """
            statements.append(StatementStr(textwrap.dedent(func_string)))

        lat_1 = input_var_name_expressions[0]
        lon_1 = input_var_name_expressions[1]
        lat_2 = input_var_name_expressions[2]
        lon_2 = input_var_name_expressions[3]
        dist_expr = ExpressionStr(f"haversine_distance({lat_1}, {lon_1}, {lat_2}, {lon_2})")
        return statements, dist_expr
