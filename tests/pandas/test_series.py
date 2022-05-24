"""
Unit test for Series
"""
import pytest

from featurebyte.execution_graph.enum import NodeOutputType, NodeType
from featurebyte.execution_graph.graph import Node
from featurebyte.pandas.series import Series


@pytest.mark.parametrize(
    "statement,expected_node_type",
    [
        ("feat1 & feat2", NodeType.AND),
        ("feat1 | feat2", NodeType.OR),
        ("feat1 == feat2", NodeType.EQ),
        ("feat1 != feat2", NodeType.NE),
        ("feat1 < feat2", NodeType.LT),
        ("feat1 <= feat2", NodeType.LE),
        ("feat1 > feat2", NodeType.GT),
        ("feat1 >= feat2", NodeType.GE),
        ("feat1 + feat2", NodeType.ADD),
        ("feat1 - feat2", NodeType.SUB),
        ("feat1 * feat2", NodeType.MUL),
        ("feat1 / feat2", NodeType.DIV),
        ("feat1 % feat2", NodeType.MOD),
    ],
)
def test_binary_operation__homogenous_type(source_df, statement, expected_node_type):
    feat1, feat2 = source_df["feat1"], source_df["feat2"]
    res = eval(statement)
    expected_node = Node(
        id=f"{expected_node_type}_1",
        type=expected_node_type,
        parameters={},
        output_type=NodeOutputType.Series,
    )
    assert type(res) is Series
    assert res.node == expected_node
