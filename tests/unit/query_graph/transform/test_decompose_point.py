"""Test related to offline store ingest graph decomposition."""

import pytest

import featurebyte as fb
from featurebyte.query_graph.transform.decompose_point import (
    AggregationInfo,
    DecomposePointExtractor,
    DecomposePointState,
)


@pytest.fixture(name="decompose_point_global_state")
def decompose_point_global_state_fixture():
    """Fixture for DecomposePointState."""
    return DecomposePointState.create(
        relationships_info=[],
        aggregation_node_names=set(),
        operation_structure_map={},
        extract_primary_entity_ids_only=False,
    )


@pytest.mark.parametrize(
    "input1_has_req_col,input1_has_graph,input2_has_req_col,input2_has_graph,expected",
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
    input1_agg_info = AggregationInfo(extract_primary_entity_ids_only=False)
    input1_agg_info.has_request_column = input1_has_req_col
    input1_agg_info.has_ingest_graph_node = input1_has_graph
    input2_agg_info = AggregationInfo(extract_primary_entity_ids_only=False)
    input2_agg_info.has_request_column = input2_has_req_col
    input2_agg_info.has_ingest_graph_node = input2_has_graph
    decompose_point_global_state.node_name_to_aggregation_info["input_1"] = input1_agg_info
    decompose_point_global_state.node_name_to_aggregation_info["input_2"] = input2_agg_info
    output = decompose_point_global_state.check_input_aggregations(
        agg_info=AggregationInfo(extract_primary_entity_ids_only=False),
        input_node_names=input_node_names,
    )
    assert output == expected


def test_decompose_point_extractor(
    global_graph,
    time_series_window_aggregate_feature_node,
):
    """Test decompose_point_extractor for the case where the query graph has cron feature job setting."""
    extractor = DecomposePointExtractor(global_graph)
    output = extractor.extract(node=time_series_window_aggregate_feature_node)
    assert output.decompose_node_names == set()
    assert output.ingest_graph_output_node_names == set()
    assert output.aggregation_node_names == {"time_series_window_aggregate_1"}


def test_cyclic_phase_mixed_fjs_deployment_sql(snowflake_event_view_with_entity):
    """
    Regression test: deployment SQL generation must NOT trigger decomposition when inputs
    have different feature job settings.

    In deployment SQL generation, FJS is irrelevant (no tile scheduling), so a feature whose
    sub-expressions happen to use different FJS should still be treated as a single graph.

    Previously, check_input_aggregations compared feature_job_settings even during deployment
    SQL generation, causing features with mixed FJS to be incorrectly flagged for decomposition.
    """
    view = snowflake_event_view_with_entity

    fjs_a = fb.FeatureJobSetting(blind_spot="1h", period="24h", offset="1h")
    fjs_b = fb.FeatureJobSetting(blind_spot="2h", period="24h", offset="2h")

    view["sin_col"] = view["col_float"].sin()
    view["cos_col"] = view["col_float"].cos()

    grouped = view.groupby("cust_id")

    # Two aggregations with deliberately different FJS
    sum_sin = grouped.aggregate_over(
        "sin_col",
        method="sum",
        feature_names=["sum_sin_fjs_a"],
        windows=["7d"],
        feature_job_setting=fjs_a,
    )["sum_sin_fjs_a"]
    sum_cos = grouped.aggregate_over(
        "cos_col",
        method="sum",
        feature_names=["sum_cos_fjs_b"],
        windows=["7d"],
        feature_job_setting=fjs_b,
    )["sum_cos_fjs_b"]

    phase = fb.atan2(sum_sin, sum_cos)
    phase.name = "phase_mixed_fjs"

    # Use separate extractor instances — BaseGraphExtractor._input_node_map_cache is per-instance
    # and would cause the second extract() call to skip processing intermediate nodes if reused.

    # Without deployment_sql_generation, decomposition IS expected (different FJS means split)
    result_normal = DecomposePointExtractor(phase.graph).extract(
        node=phase.node, deployment_sql_generation=False
    )
    assert result_normal.decompose_node_names != set(), (
        "Expected decomposition in normal mode with mixed FJS"
    )

    # With deployment_sql_generation, FJS differences should be ignored — no decomposition
    result_deployment = DecomposePointExtractor(phase.graph).extract(
        node=phase.node, deployment_sql_generation=True
    )
    assert result_deployment.decompose_node_names == set(), (
        f"Deployment SQL generation incorrectly triggered decomposition for mixed-FJS feature: "
        f"{result_deployment.decompose_node_names}"
    )
