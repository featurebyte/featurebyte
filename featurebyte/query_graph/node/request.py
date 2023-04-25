"""
Request data related node classes
"""
from typing import List, Literal, Sequence

from pydantic import BaseModel, Field, StrictStr

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.base import BaseNode
from featurebyte.query_graph.node.metadata.operation import (
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

    type: Literal[NodeType.REQUEST_COLUMN] = Field(NodeType.REQUEST_COLUMN, const=True)
    output_type: NodeOutputType
    parameters: RequestColumnNodeParameters

    @property
    def max_input_count(self) -> int:
        return 0

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        raise RuntimeError("Proxy input node should not be used to derive input columns.")

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        return OperationStructure(
            columns=[],
            aggregations=[],
            output_type=NodeOutputType.SERIES,
            output_category=NodeOutputCategory.FEATURE,
            row_index_lineage=(self.name,),
        )
