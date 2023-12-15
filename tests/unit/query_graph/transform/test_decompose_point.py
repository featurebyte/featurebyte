"""Test related to offline store ingest graph decomposition."""
import pytest
from bson import ObjectId

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.transform.decompose_point import (
    AggregationInfo,
    DecomposePointGlobalState,
)


@pytest.fixture(name="decompose_point_global_state")
def decompose_point_global_state_fixture():
    """Fixture for DecomposePointGlobalState."""
    return DecomposePointGlobalState.create(relationships_info=[], aggregation_node_names=set())


@pytest.mark.parametrize(
    "input1_has_req_col,input1_has_graph," "input2_has_req_col,input2_has_graph," "expected",
    [
        # do not split
        (False, False, False, False, False),
        # split so that input 1 can be a new graph node
        (False, False, False, True, True),
        (False, False, True, False, True),
        (False, False, True, True, True),
        # split so that input 2 can be a new graph node
        (False, True, False, False, True),
        (True, False, False, False, True),
        (True, True, False, False, True),
        # could not split as both inputs either have request column or ingest graph node
        (False, True, False, True, False),
        (False, True, True, False, False),
        (False, True, True, True, False),
        (True, False, False, True, False),
        (True, False, True, False, False),
        (True, False, True, True, False),
        (True, True, False, True, False),
        (True, True, True, False, False),
        (True, True, True, True, False),
    ],
)
def test_check_input_aggregations(
    decompose_point_global_state,
    input1_has_req_col,
    input1_has_graph,
    input2_has_req_col,
    input2_has_graph,
    expected,
):
    """Test check_input_aggregations."""
    input_node_names = ["input_1", "input_2"]
    input1_agg_info = AggregationInfo()
    input1_agg_info.has_request_column = input1_has_req_col
    input1_agg_info.has_ingest_graph_node = input1_has_graph
    input2_agg_info = AggregationInfo()
    input2_agg_info.has_request_column = input2_has_req_col
    input2_agg_info.has_ingest_graph_node = input2_has_graph
    decompose_point_global_state.node_name_to_aggregation_info["input_1"] = input1_agg_info
    decompose_point_global_state.node_name_to_aggregation_info["input_2"] = input2_agg_info
    output = decompose_point_global_state.check_input_aggregations(
        agg_info=AggregationInfo(),
        input_node_names=input_node_names,
    )
    assert output == expected
