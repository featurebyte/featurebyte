"""
Unit tests for featurebyte/query_graph/node/base.py
"""
from typing import Any, List, Literal

import pytest
from pydantic import BaseModel, Field

from featurebyte.query_graph.enum import NodeOutputType
from featurebyte.query_graph.node.base import BaseNode
from featurebyte.query_graph.node.count_dict import CountDictTransformNode
from featurebyte.query_graph.node.metadata.column import InColumnStr, OutColumnStr
from featurebyte.query_graph.node.metadata.operation import NodeOutputCategory, OperationStructure


@pytest.fixture(name="node")
def node_fixture():
    """Node fixture"""

    class NestedNodeParams(BaseModel):
        in_name: InColumnStr
        in_names: List[InColumnStr]
        out_name: OutColumnStr
        out_names: List[OutColumnStr]

    class Node(BaseNode):
        class NodeParams(BaseModel):
            in_name: InColumnStr
            in_names: List[InColumnStr]
            out_name: OutColumnStr
            out_names: List[OutColumnStr]
            nested_params: NestedNodeParams
            anything: Any

        type: Literal["type"] = Field("node_type", const=True)
        parameters: NodeParams

        def max_input_count(self) -> int:
            return 4

        def _get_required_input_columns(self, input_index: int):
            _ = input_index
            return self._extract_column_str_values(self.parameters.dict(), InColumnStr)

        def _derive_node_operation_info(self, inputs, branch_state, global_state):
            _ = inputs, branch_state, global_state
            return OperationStructure(
                output_type=NodeOutputType.FRAME, output_category=NodeOutputCategory.VIEW
            )

    return Node(
        name="node_name",
        output_type=NodeOutputType.SERIES,
        parameters={
            "in_name": "required_column",
            "in_names": ["required_columns"],
            "out_name": "new_output_column",
            "out_names": ["new_output_columns"],
            "nested_params": {
                "in_name": "nested_required_column",
                "in_names": ["nested_required_columns"],
                "out_name": "nested_new_output_column",
                "out_names": ["nested_new_output_columns"],
            },
            "anything": "anything",
        },
    )


def test_get_required_input_columns(node):
    """Test get_required_input_columns"""
    required_in_cols = node._get_required_input_columns(0)
    assert set(required_in_cols) == {
        "required_column",
        "required_columns",
        "nested_required_column",
        "nested_required_columns",
    }


@pytest.mark.parametrize(
    "parameters",
    [
        {
            "name": "count_dict_node",
            "parameters": {"transform_type": "entropy"},
            "output_type": "series",
        },
        {
            "name": "count_dict_node",
            "parameters": {"transform_type": "unique_count", "include_missing": True},
            "output_type": "series",
        },
    ],
)
def test_count_dict_transform_node(parameters):
    """Test CountDitTransformNode not introducing additional param for unique_count transform type"""
    assert CountDictTransformNode(**parameters).dict(exclude={"type": True}) == parameters
