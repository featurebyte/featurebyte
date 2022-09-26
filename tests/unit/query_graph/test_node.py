"""
Unit tests for featurebyte/query_graph/node/base.py
"""
from typing import Any, List, Literal

import pytest
from pydantic import BaseModel, Field

from featurebyte.query_graph.enum import NodeOutputType
from featurebyte.query_graph.node.base import BaseNode, InColumnStr, OutColumnStr


@pytest.fixture(name="node")
def node_fixture():
    """Node fixture"""

    class Node(BaseNode):
        class NodeParams(BaseModel):
            in_name: InColumnStr
            in_names: List[InColumnStr]
            out_name: OutColumnStr
            out_names: List[OutColumnStr]
            anything: Any

        type: Literal["type"] = Field("node_type", const=True)
        parameters: NodeParams

    return Node(
        name="node_name",
        output_type=NodeOutputType.SERIES,
        parameters={
            "in_name": "required_column",
            "in_names": ["required_columns"],
            "out_name": "new_output_column",
            "out_names": ["new_output_columns"],
            "anything": "anything",
        },
    )


def test_get_required_input_columns(node):
    """Test get_required_input_columns"""
    required_in_cols = node.get_required_input_columns()
    assert set(required_in_cols) == {"required_column", "required_columns"}


def test_get_new_output_columns(node):
    """Test get_new_output_columns"""
    required_in_cols = node.get_new_output_columns()
    assert set(required_in_cols) == {"new_output_column", "new_output_columns"}
