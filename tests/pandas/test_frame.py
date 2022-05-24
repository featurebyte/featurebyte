"""
Unit test for DataFrame
"""
import pytest

from featurebyte.execution_graph.enum import NodeOutputType, NodeType
from featurebyte.execution_graph.graph import Node
from featurebyte.pandas.frame import DataFrame
from featurebyte.pandas.series import Series


def test_getitem__string_input(source_df):
    feat = source_df["feat"]
    assert isinstance(feat, Series)
    assert feat.node == Node(
        id="PROJECT_1",
        type=NodeType.PROJECT,
        parameters={"columns": ["feat"]},
        output_type=NodeOutputType.Series,
    )


def test_getitem__list_of_string_input(source_df):
    feat_df = source_df[["feat"]]
    assert isinstance(feat_df, DataFrame)
    assert feat_df.node == Node(
        id="PROJECT_1",
        type=NodeType.PROJECT,
        parameters={"columns": ["feat"]},
        output_type=NodeOutputType.DataFrame,
    )


def test_getitem__series_input(source_df):
    cond = source_df["cond"]
    feat_df = source_df[cond]
    assert isinstance(feat_df, DataFrame)
    assert feat_df.node == Node(
        id="FILTER_1", type=NodeType.FILTER, parameters={}, output_type=NodeOutputType.DataFrame
    )


@pytest.mark.parametrize("item", [1, ["feat", 2]])
def test_getitem__not_implemented(source_df, item):
    with pytest.raises(TypeError):
        _ = source_df[item]


def test_setitem(source_df):
    source_df["amount"] = source_df["feat"]
    assert source_df.node == Node(
        id="ASSIGN_1", type=NodeType.ASSIGN, parameters={}, output_type=NodeOutputType.DataFrame
    )
    assert source_df.execution_graph.backward_edges["ASSIGN_1"] == ["INPUT_1", "PROJECT_1"]


@pytest.mark.parametrize("item", [1, "string", True])
def test_setitem__not_implemented(source_df, item):
    with pytest.raises(TypeError):
        source_df["amount"] = item
