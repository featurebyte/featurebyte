"""
This module contains tests for the offline ingest query graph.
"""
from featurebyte import FeatureJobSetting, RequestColumn
from tests.util.helper import check_decomposed_graph_output_node_hash


def test_feature_contains_ttl_and_non_ttl_components(float_feature, non_time_based_feature):
    """Test that a feature contains both ttl and non-ttl components."""
    ttl_component = 2 * (float_feature + 100)
    non_ttl_component = 3 - (non_time_based_feature + 100)
    feature = ttl_component + non_ttl_component
    feature.name = "feature"
    feature.save()

    # check offline ingest query graph
    ingest_query_graphs = feature.cached_model.extract_offline_store_ingest_query_graphs()
    assert len(ingest_query_graphs) == 2
    if ingest_query_graphs[0].feature_job_setting:
        ttl_component_graph = ingest_query_graphs[0]
        non_ttl_component_graph = ingest_query_graphs[1]
    else:
        ttl_component_graph = ingest_query_graphs[1]
        non_ttl_component_graph = ingest_query_graphs[0]

    expected_feature_job_setting = FeatureJobSetting(
        blind_spot="600s", frequency="1800s", time_modulo_frequency="300s"
    )
    assert ttl_component_graph.feature_job_setting == expected_feature_job_setting
    assert ttl_component_graph.node_name == "mul_1"
    assert ttl_component_graph.has_ttl is True

    assert non_ttl_component_graph.feature_job_setting is None
    assert non_ttl_component_graph.node_name == "sub_1"
    assert non_ttl_component_graph.has_ttl is False

    # check consistency of decomposed graph
    check_decomposed_graph_output_node_hash(feature_model=feature.cached_model)


def test_feature_request_column_and_non_ttl_components(
    non_time_based_feature, latest_event_timestamp_feature
):
    """Test that a feature contains both request column and non-ttl components."""
    request_and_ttl_component = (
        # request component part
        RequestColumn.point_in_time()
        + (RequestColumn.point_in_time() - RequestColumn.point_in_time())
        # ttl component part
        - latest_event_timestamp_feature
    ).dt.day
    non_ttl_component = non_time_based_feature
    feature = request_and_ttl_component + non_ttl_component
    feature.name = "feature"
    feature.save()

    # check offline ingest query graph (note that the request column part should be removed)
    ingest_query_graphs = feature.cached_model.extract_offline_store_ingest_query_graphs()
    assert len(ingest_query_graphs) == 2
    if ingest_query_graphs[0].feature_job_setting:
        ttl_component_graph = ingest_query_graphs[0]
        non_ttl_component_graph = ingest_query_graphs[1]
    else:
        ttl_component_graph = ingest_query_graphs[1]
        non_ttl_component_graph = ingest_query_graphs[0]

    expected_feature_job_setting = FeatureJobSetting(
        blind_spot="3600s", frequency="3600s", time_modulo_frequency="1800s"
    )
    assert ttl_component_graph.feature_job_setting == expected_feature_job_setting
    assert ttl_component_graph.node_name == "project_1"
    assert ttl_component_graph.has_ttl is True

    assert non_ttl_component_graph.feature_job_setting is None
    assert non_ttl_component_graph.node_name == "project_1"
    assert non_ttl_component_graph.has_ttl is False

    # check consistency of decomposed graph
    check_decomposed_graph_output_node_hash(feature_model=feature.cached_model)
