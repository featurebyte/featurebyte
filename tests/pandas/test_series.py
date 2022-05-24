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
    node_id = f"{expected_node_type}_1"
    expected_node = Node(
        id=node_id,
        type=expected_node_type,
        parameters={},
        output_type=NodeOutputType.Series,
    )
    assert type(res) is Series
    assert res.node == expected_node
    assert len(source_df.execution_graph.backward_edges[node_id]) == 2


@pytest.mark.parametrize(
    "statement,expected_node_type,expected_value",
    [
        ("feat & True", NodeType.AND, True),
        ("feat | False", NodeType.OR, False),
    ],
)
def test_binary_logical_expressions__heterogeneous_type(
    source_df, statement, expected_node_type, expected_value
):
    feat = source_df["feat"]
    res = eval(statement)
    node_id = f"{expected_node_type}_1"
    expected_node = Node(
        id=node_id,
        type=expected_node_type,
        parameters={"value": expected_value},
        output_type=NodeOutputType.Series,
    )
    assert type(res) is Series
    assert res.node == expected_node
    assert len(source_df.execution_graph.backward_edges[node_id]) == 1


@pytest.mark.parametrize("other", [1, 0.123, "string"])
def test_binary_logical_expressions__heterogeneous_type_not_implemented(source_df, other):
    feat = source_df["feat"]
    with pytest.raises(TypeError):
        _ = feat & other
