"""
This module contains tests for the offline ingest query graph.
"""
import os
import textwrap

import pytest
from bson import json_util

from featurebyte import FeatureJobSetting, RequestColumn
from featurebyte.models.feature import FeatureModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.transform.offline_store_ingest import AggregationNodeInfo
from tests.util.helper import (
    check_decomposed_graph_output_node_hash,
    check_on_demand_feature_view_code_generation,
)


@pytest.fixture(name="always_enable_feast_integration", autouse=True)
def always_enable_feast_integration_fixture(enable_feast_integration):
    """Enable feast integration for all tests in this module"""
    _ = enable_feast_integration
    yield


@pytest.fixture(name="latest_event_timestamp_feature")
def latest_event_timestamp_feature_fixture(
    snowflake_event_view_with_entity, feature_group_feature_job_setting
):
    """
    Fixture for a timestamp feature
    """
    feature = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="event_timestamp",
        method="latest",
        windows=["90d"],
        feature_names=["latest_event_timestamp_90d"],
        feature_job_setting=feature_group_feature_job_setting,
    )["latest_event_timestamp_90d"]
    return feature


@pytest.fixture(name="entity_id_to_serving_name")
def entity_id_to_serving_name_fixture(cust_id_entity, transaction_entity):
    """Fixture for entity id to serving name"""
    return {
        cust_id_entity.id: cust_id_entity.serving_names[0],
        transaction_entity.id: transaction_entity.serving_names[0],
    }


def check_ingest_query_graph(ingest_query_graph):
    """Check the ingest query graph."""
    graph = ingest_query_graph.graph
    for aggregation_node_info in ingest_query_graph.aggregation_nodes_info:
        agg_node = graph.get_node_by_name(aggregation_node_info.node_name)
        assert agg_node.type == aggregation_node_info.node_type
        input_node_names = graph.get_input_node_names(agg_node)
        assert len(input_node_names) == 1
        assert input_node_names[0] == aggregation_node_info.input_node_name


def test_feature__ttl_and_non_ttl_components(float_feature, non_time_based_feature):
    """Test that a feature contains both ttl and non-ttl components."""
    ttl_component = 2 * (float_feature + 100)
    non_ttl_component = 3 - (non_time_based_feature + 100)
    feature = ttl_component + non_ttl_component
    feature.name = "feature"
    feature.save()

    # check offline ingest query graph
    feature_model = feature.cached_model
    offline_store_info = feature_model.offline_store_info
    ingest_query_graphs = offline_store_info.extract_offline_store_ingest_query_graphs()
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
    assert ttl_component_graph.aggregation_nodes_info == [
        AggregationNodeInfo(
            node_type=NodeType.GROUPBY, node_name="groupby_1", input_node_name="graph_1"
        ),
    ]
    check_ingest_query_graph(ttl_component_graph)

    assert non_ttl_component_graph.feature_job_setting is None
    assert non_ttl_component_graph.node_name == "sub_1"
    assert non_ttl_component_graph.has_ttl is False
    assert non_ttl_component_graph.aggregation_nodes_info == [
        AggregationNodeInfo(
            node_type=NodeType.ITEM_GROUPBY, node_name="item_groupby_1", input_node_name="graph_2"
        ),
    ]
    check_ingest_query_graph(non_ttl_component_graph)

    # check consistency of decomposed graph
    check_decomposed_graph_output_node_hash(feature_model=feature_model)
    check_on_demand_feature_view_code_generation(feature_model=feature_model)

    # check on-demand view code
    fv_global_state = offline_store_info.extract_on_demand_feature_view_code_generation()
    on_demand_feature_view_codes = fv_global_state.generate_code()
    expected = """
    import numpy as np
    import pandas as pd


    def on_demand_feature_view(inputs: pd.DataFrame) -> pd.DataFrame:
        df = pd.DataFrame()
        feat = inputs["__feature__part0"] + inputs["__feature__part1"]
        df["feature"] = feat
        return df
    """
    assert on_demand_feature_view_codes.strip() == textwrap.dedent(expected).strip()


def test_feature__request_column_ttl_and_non_ttl_components(
    non_time_based_feature,
    latest_event_timestamp_feature,
    feature_group_feature_job_setting,
):
    """Test that a feature contains request column, ttl and non-ttl components."""
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
    feature_model = feature.cached_model
    offline_store_info = feature_model.offline_store_info
    ingest_query_graphs = offline_store_info.extract_offline_store_ingest_query_graphs()
    assert len(ingest_query_graphs) == 2
    if ingest_query_graphs[0].feature_job_setting:
        ttl_component_graph = ingest_query_graphs[0]
        non_ttl_component_graph = ingest_query_graphs[1]
    else:
        ttl_component_graph = ingest_query_graphs[1]
        non_ttl_component_graph = ingest_query_graphs[0]

    assert ttl_component_graph.feature_job_setting == feature_group_feature_job_setting
    assert ttl_component_graph.node_name == "project_1"
    assert ttl_component_graph.has_ttl is True
    assert ttl_component_graph.aggregation_nodes_info == [
        AggregationNodeInfo(
            node_type=NodeType.GROUPBY, node_name="groupby_1", input_node_name="graph_1"
        ),
    ]
    check_ingest_query_graph(ttl_component_graph)

    assert non_ttl_component_graph.feature_job_setting is None
    assert non_ttl_component_graph.node_name == "project_1"
    assert non_ttl_component_graph.has_ttl is False
    assert non_ttl_component_graph.aggregation_nodes_info == [
        AggregationNodeInfo(
            node_type=NodeType.ITEM_GROUPBY, node_name="item_groupby_1", input_node_name="graph_2"
        ),
    ]
    check_ingest_query_graph(non_ttl_component_graph)

    # check consistency of decomposed graph
    check_decomposed_graph_output_node_hash(feature_model=feature_model)
    check_on_demand_feature_view_code_generation(feature_model=feature_model)

    # check on-demand view code
    fv_global_state = offline_store_info.extract_on_demand_feature_view_code_generation()
    on_demand_feature_view_codes = fv_global_state.generate_code()
    expected = """
    import numpy as np
    import pandas as pd


    def on_demand_feature_view(inputs: pd.DataFrame) -> pd.DataFrame:
        df = pd.DataFrame()
        request_col = pd.to_datetime(inputs["POINT_IN_TIME"])
        feat = request_col + (request_col - request_col)
        feat_1 = pd.to_datetime(inputs["__feature__part0"])
        feat_2 = ((feat - feat_1).dt.seconds // 86400) + inputs["__feature__part1"]
        df["feature"] = feat_2
        return df
    """
    assert on_demand_feature_view_codes.strip() == textwrap.dedent(expected).strip()


def test_feature__multiple_non_ttl_components(
    snowflake_scd_table, snowflake_dimension_table, cust_id_entity
):
    """Test that a feature contains multiple non-ttl components."""
    snowflake_scd_table["col_text"].as_entity(cust_id_entity.name)
    scd_view = snowflake_scd_table.get_view()
    lookup_feature = scd_view["col_float"].as_feature("FloatFeature", offset="7d")

    snowflake_dimension_table["col_int"].as_entity(cust_id_entity.name)
    dimension_view = snowflake_dimension_table.get_view()
    feature_a = dimension_view["col_float"].as_feature("FloatFeatureDimensionView")
    feature = lookup_feature + feature_a
    feature.name = "feature"
    feature.save()

    # check offline ingest query graph (note that the request column part should be removed)
    feature_model = feature.cached_model
    offline_store_info = feature_model.offline_store_info
    ingest_query_graphs = offline_store_info.extract_offline_store_ingest_query_graphs()
    assert len(ingest_query_graphs) == 1
    non_ttl_component_graph = ingest_query_graphs[0]
    assert non_ttl_component_graph.feature_job_setting is None
    assert non_ttl_component_graph.node_name == "alias_1"
    assert non_ttl_component_graph.has_ttl is False
    assert non_ttl_component_graph.aggregation_nodes_info == [
        AggregationNodeInfo(
            node_type=NodeType.LOOKUP, node_name="lookup_1", input_node_name="graph_1"
        ),
        AggregationNodeInfo(
            node_type=NodeType.LOOKUP, node_name="lookup_2", input_node_name="graph_2"
        ),
    ]
    check_ingest_query_graph(non_ttl_component_graph)

    # check consistency of decomposed graph
    check_decomposed_graph_output_node_hash(feature_model=feature.cached_model)

    # check on-demand view code
    assert offline_store_info.is_decomposed is False
    expected_error = "On demand view can only be extracted from decomposed query graph"
    with pytest.raises(ValueError, match=expected_error):
        offline_store_info.extract_on_demand_feature_view_code_generation()


def test_feature__ttl_item_aggregate_request_column(
    float_feature, non_time_based_feature, latest_event_timestamp_feature
):
    """Test that a feature contains ttl item aggregate and request column components."""
    request_feature = (RequestColumn.point_in_time() - latest_event_timestamp_feature).dt.day
    composite_feature = float_feature + non_time_based_feature + request_feature
    composite_feature.name = "composite_feature"
    composite_feature.save()

    # check offline ingest query graph
    feature_model = composite_feature.cached_model
    check_decomposed_graph_output_node_hash(feature_model=feature_model)
    check_on_demand_feature_view_code_generation(feature_model=feature_model)

    # check on-demand view code
    offline_store_info = feature_model.offline_store_info
    fv_global_state = offline_store_info.extract_on_demand_feature_view_code_generation()
    on_demand_feature_view_codes = fv_global_state.generate_code()
    expected = """
    import numpy as np
    import pandas as pd


    def on_demand_feature_view(inputs: pd.DataFrame) -> pd.DataFrame:
        df = pd.DataFrame()
        feat = (
            inputs["__composite_feature__part0"]
            + inputs["__composite_feature__part1"]
        )
        request_col = pd.to_datetime(inputs["POINT_IN_TIME"])
        feat_1 = pd.to_datetime(inputs["__composite_feature__part2"])
        feat_2 = (request_col - feat_1).dt.seconds // 86400
        df["composite_feature"] = feat + feat_2
        return df
    """
    assert on_demand_feature_view_codes.strip() == textwrap.dedent(expected).strip()


def test_feature__input_has_mixed_ingest_graph_node_flags(
    snowflake_event_table_with_entity,
    feature_group_feature_job_setting,
):
    """Test that a feature with mixed ingest graph node flags input nodes."""
    snowflake_event_table_with_entity.update_default_feature_job_setting(
        feature_job_setting=feature_group_feature_job_setting,
    )
    event_view = snowflake_event_table_with_entity.get_view()
    grouped_event_view = event_view.groupby("cust_id")
    feature_raw = event_view.as_features(column_names=["col_float"], feature_names=["col_float"])[
        "col_float"
    ]
    feature_avg = grouped_event_view.aggregate_over(
        value_column="col_float",
        method="avg",
        windows=["90d"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["avg_col_float_90d"],
    )["avg_col_float_90d"]
    feature_std = grouped_event_view.aggregate_over(
        value_column="col_float",
        method="std",
        windows=["90d"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["std_col_float_90d"],
    )["std_col_float_90d"]
    feature_zscore = (feature_raw - feature_avg) / feature_std
    feature_zscore.name = "feature_zscore"
    feature_zscore.save()

    # check offline ingest query graph
    feature_model = feature_zscore.cached_model
    check_decomposed_graph_output_node_hash(feature_model=feature_model)
    check_on_demand_feature_view_code_generation(feature_model=feature_model)

    # check on-demand view code
    offline_store_info = feature_model.offline_store_info
    fv_global_state = offline_store_info.extract_on_demand_feature_view_code_generation()
    on_demand_feature_view_codes = fv_global_state.generate_code()
    expected = """
    import numpy as np
    import pandas as pd


    def on_demand_feature_view(inputs: pd.DataFrame) -> pd.DataFrame:
        df = pd.DataFrame()
        feat = inputs["__feature_zscore__part0"] - inputs["__feature_zscore__part1"]
        df["feature_zscore"] = feat / inputs["__feature_zscore__part2"]
        return df
    """
    assert on_demand_feature_view_codes.strip() == textwrap.dedent(expected).strip()


def test_feature__input_has_ingest_query_graph_node(test_dir):
    """
    Test a complex feature when there exist a node fulfills the split condition except all the input nodes
    have already contained ingest query graph nodes.
    """
    fixture_path = os.path.join(
        test_dir,
        "fixtures/graph/TXN_IsWeekend_Representation_in_CARD_Cash_advance_Txn_Amount_90d.json",
    )
    with open(fixture_path, "r") as file_handle:
        feature_dict = json_util.loads(file_handle.read())

    feature_model = FeatureModel(**feature_dict)
    check_decomposed_graph_output_node_hash(feature_model=feature_model)
