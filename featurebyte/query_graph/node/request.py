"""
Request data related node classes
"""
from typing import List, Literal, Sequence

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
        raise RuntimeError("Request column node should not be used to derive input columns.")

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
