"""
Tests for tile_compute_combine.py
"""

import pytest

from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.sql.tile_compute_combine import combine_tile_compute_specs


@pytest.fixture(name="feature_1")
def feature_1_fixture(snowflake_event_view_with_entity, feature_group_feature_job_setting):
    """
    Fixture for a feature
    """
    view = snowflake_event_view_with_entity
    feature = view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["my_feature_1"],
    )["my_feature_1"]
    return feature


@pytest.fixture(name="feature_2")
def feature_2_fixture(snowflake_event_view_with_entity, feature_group_feature_job_setting):
    """
    Fixture for a feature
    """
    view = snowflake_event_view_with_entity
    feature = view.groupby("cust_id").aggregate_over(
        value_column="col_int",
        method="avg",
        windows=["30m"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["my_feature_2"],
    )["my_feature_2"]
    return feature


@pytest.fixture(name="feature_3")
def feature_3_fixture(snowflake_event_view_with_entity, feature_group_feature_job_setting):
    """
    Fixture for a feature (different job settiing
    """
    view = snowflake_event_view_with_entity
    feature_job_setting = feature_group_feature_job_setting.copy()
    feature_job_setting.offset = f"{feature_job_setting.offset_seconds + 10}s"
    feature = view.groupby("cust_id").aggregate_over(
        value_column="col_int",
        method="avg",
        windows=["30m"],
        feature_job_setting=feature_job_setting,
        feature_names=["my_feature_2"],
    )["my_feature_2"]
    return feature


def get_tile_info(feature, source_info):
    """
    Helper function to get tile info from a feature
    """
    graph = feature.graph
    node = feature.node
    interpreter = GraphInterpreter(graph, source_info)
    tile_gen_sqls = interpreter.construct_tile_gen_sql(node, is_on_demand=True)
    assert len(tile_gen_sqls) == 1
    return tile_gen_sqls[0]


def get_tile_info_for_features(features, source_info):
    """
    Helper function to get tile info for multiple features
    """
    return [get_tile_info(feature, source_info) for feature in features]


def test_tile_compute_combine(feature_1, feature_2, source_info):
    """
    Test when combination is possible
    """
    result = combine_tile_compute_specs(
        get_tile_info_for_features([feature_1, feature_2], source_info)
    )
    assert len(result) == 1
    combined = result[0]
    assert len(combined.tile_table_groupings) == 2


def test_tile_compute_no_combine(feature_1, feature_3, source_info):
    """
    Test when combination is not possible
    """
    result = combine_tile_compute_specs(
        get_tile_info_for_features([feature_1, feature_3], source_info)
    )
    assert len(result) == 2
    for result in result:
        assert len(result.tile_table_groupings) == 1
