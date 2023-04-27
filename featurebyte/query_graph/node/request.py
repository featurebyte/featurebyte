"""
Request data related node classes
"""
from typing import List, Literal, Sequence, Tuple

from pydantic import BaseModel, Field, StrictStr

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.base import BaseNode
from featurebyte.query_graph.node.metadata.operation import (
    AggregationColumn,
    FeatureDataColumnType,
    NodeOutputCategory,
    OperationStructure,
    OperationStructureBranchState,
    OperationStructureInfo,
)
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    CodeGenerationConfig,
    StatementT,
    VariableNameGenerator,
    VarNameExpressionStr,
    get_object_class_from_function_call,
)


class RequestColumnNode(BaseNode):
    """Request column node used by on-demand features"""

    class RequestColumnNodeParameters(BaseModel):
        """Node parameters"""

        column_name: StrictStr
        dtype: DBVarType

    type: Literal[NodeType.REQUEST_COLUMN] = Field(NodeType.REQUEST_COLUMN, const=True)
    output_type: NodeOutputType
    parameters: RequestColumnNodeParameters

    @property
    def max_input_count(self) -> int:
        return 0

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
        return OperationStructure(
            columns=[],
            aggregations=[
                AggregationColumn(
                    name=self.parameters.column_name,
                    dtype=self.parameters.dtype,
                    filter=False,
                    node_names={self.name},
                    node_name=self.name,
                    method=None,
                    keys=[],
                    window=None,
                    category=None,
                    type=FeatureDataColumnType.AGGREGATION,
                    column=None,
                    aggregation_type=NodeType.REQUEST_COLUMN,
                ),
            ],
            output_type=NodeOutputType.SERIES,
            output_category=NodeOutputCategory.FEATURE,
            row_index_lineage=(self.name,),
        )

    def _derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionStr],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionStr]:
        statements: List[StatementT] = []
        var_name = var_name_generator.generate_variable_name(
            node_output_type=operation_structure.output_type,
            node_output_category=operation_structure.output_category,
        )
        obj = ClassEnum.REQUEST_COLUMN(
            self.parameters.column_name,
            self.parameters.dtype,
            _method_name="create_request_column",
        )
        statements.append((var_name, obj))
        return statements, var_name
