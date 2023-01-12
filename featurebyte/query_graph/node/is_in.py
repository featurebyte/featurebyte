"""
Is in dictionary
"""
from typing import Any, Dict, List, Literal

from pydantic import Field

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node import BaseNode
from featurebyte.query_graph.node.metadata.operation import (
    NodeOutputCategory,
    OperationStructure,
    OperationStructureBranchState,
    OperationStructureInfo,
)


class IsInDictionaryNode(BaseNode):
    """
    Is in dictionary node
    """

    # class Parameters(BaseModel):
    #     """Parameters"""
    # columns: List[InColumnStr]

    type: Literal[NodeType.IS_IN_DICT] = Field(NodeType.IS_IN_DICT, const=True)
    # parameters: Parameters

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        _ = branch_state, global_state
        input_operation_info = inputs[0]
        output_category = input_operation_info.output_category
        names = set(self.get_required_input_columns())
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
