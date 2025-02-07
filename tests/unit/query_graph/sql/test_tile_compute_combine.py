"""
Tests for tile_compute_combine.py
"""

import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.sql.interpreter import GraphInterpreter, TileGenSql
from featurebyte.query_graph.sql.tile_compute_combine import (
    TileTableGrouping,
    combine_tile_compute_specs,
)
from tests.util.helper import assert_equal_with_expected_fixture


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
    Fixture for a feature (different job setting)
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


@pytest.fixture(name="feature_many_tiles")
def feature_many_tiles_fixture(snowflake_event_view_with_entity, feature_group_feature_job_setting):
    """
    Fixture for a feature with many tiles
    """
    view = snowflake_event_view_with_entity
    final_feature = None
    for i in range(20):
        view["transformed_col"] = view["col_int"] + (i * 123.0)
        feature_name = f"my_feature_{i}"
        feature = view.groupby("cust_id").aggregate_over(
            value_column="transformed_col",
            method="max",
            windows=["30m"],
            feature_job_setting=feature_group_feature_job_setting,
            feature_names=[feature_name],
        )[feature_name]
        if final_feature is None:
            final_feature = feature
        else:
            final_feature += feature
    final_feature.name = "complex_feature"
    return final_feature


@pytest.fixture(name="cross_aggregate_features")
def cross_aggregate_features_fixture(
    snowflake_event_view_with_entity, feature_group_feature_job_setting
):
    """
    Fixture for cross aggregate features
    """
    view = snowflake_event_view_with_entity
    feature_job_setting = feature_group_feature_job_setting
    cross_aggregate_feature_1 = view.groupby("cust_id", category="col_int").aggregate_over(
        value_column="col_float",
        method="min",
        windows=["30m"],
        feature_job_setting=feature_job_setting,
        feature_names=["cross_aggregate_feature_1"],
    )["cross_aggregation_feature_1"]
    cross_aggregate_feature_2 = view.groupby("cust_id", category="col_int").aggregate_over(
        value_column="col_float",
        method="max",
        windows=["30m"],
        feature_job_setting=feature_job_setting,
        feature_names=["cross_aggregate_feature_2"],
    )["cross_aggregation_feature_2"]
    return [cross_aggregate_feature_1, cross_aggregate_feature_2]


def get_tile_infos(feature, source_info) -> list[TileGenSql]:
    """
    Helper function to get tile info from a feature
    """
    graph = feature.graph
    node = feature.node
    interpreter = GraphInterpreter(graph, source_info)
    tile_gen_sqls = interpreter.construct_tile_gen_sql(node, is_on_demand=True)
    return tile_gen_sqls


def get_tile_infos_for_features(features, source_info) -> list[TileGenSql]:
    """
    Helper function to get tile info for multiple features
    """
    tile_infos = []
    for feature in features:
        tile_infos.extend(get_tile_infos(feature, source_info))
    return tile_infos


@pytest.fixture(name="expected_combined_tile_table_groupings")
def expected_combined_tile_table_groupings_fixture():
    """
    Expected combined tile table groupings
    """
    return [
        TileTableGrouping(
            value_column_names=["value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"],
            value_column_types=[DBVarType.FLOAT],
            tile_id="TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295",
            aggregation_id="sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
        ),
        TileTableGrouping(
            value_column_names=[
                "sum_value_avg_7878f6dd82c857a14e65c8c50286995e4ca267ec",
                "count_value_avg_7878f6dd82c857a14e65c8c50286995e4ca267ec",
            ],
            value_column_types=[DBVarType.FLOAT, DBVarType.FLOAT],
            tile_id="TILE_AVG_7878F6DD82C857A14E65C8C50286995E4CA267EC",
            aggregation_id="avg_7878f6dd82c857a14e65c8c50286995e4ca267ec",
        ),
    ]


def test_tile_compute_combine(
    feature_1, feature_2, source_info, expected_combined_tile_table_groupings, update_fixtures
):
    """
    Test when combination is possible
    """
    result = combine_tile_compute_specs(
        get_tile_infos_for_features([feature_1, feature_2], source_info)
    )
    assert len(result) == 1
    combined = result[0]
    combined_tile_query = (
        combined.tile_info.tile_compute_spec.get_tile_compute_query().get_combined_query_string()
    )
    assert_equal_with_expected_fixture(
        combined_tile_query,
        "tests/fixtures/query_graph/expected_combined_tile_query.sql",
        update_fixtures,
    )
    assert combined.tile_table_groupings == expected_combined_tile_table_groupings


def test_tile_compute_no_combine(feature_1, feature_3, source_info):
    """
    Test when combination is not possible
    """
    result = combine_tile_compute_specs(
        get_tile_infos_for_features([feature_1, feature_3], source_info)
    )
    assert len(result) == 2
    assert result[0].tile_info == get_tile_infos(feature_1, source_info)[0]
    assert result[0].tile_table_groupings == [
        TileTableGrouping(
            value_column_names=["value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"],
            value_column_types=[DBVarType.FLOAT],
            tile_id="TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295",
            aggregation_id="sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
        )
    ]
    assert result[1].tile_info == get_tile_infos(feature_3, source_info)[0]
    assert result[1].tile_table_groupings == [
        TileTableGrouping(
            value_column_names=[
                "sum_value_avg_3e4c470d4a020c9ecbac9525189b19d6a01e5b47",
                "count_value_avg_3e4c470d4a020c9ecbac9525189b19d6a01e5b47",
            ],
            value_column_types=[DBVarType.FLOAT, DBVarType.FLOAT],
            tile_id="TILE_AVG_3E4C470D4A020C9ECBAC9525189B19D6A01E5B47",
            aggregation_id="avg_3e4c470d4a020c9ecbac9525189b19d6a01e5b47",
        )
    ]


def test_tile_compute_partial_combine(
    feature_1,
    feature_2,
    feature_3,
    source_info,
    expected_combined_tile_table_groupings,
    update_fixtures,
):
    """
    Test when combination is partially possible
    """
    result = combine_tile_compute_specs(
        get_tile_infos_for_features([feature_1, feature_2, feature_3], source_info),
    )

    assert len(result) == 2

    # feature_1 and feature_2 combined
    combined_tile_query = (
        result[0].tile_info.tile_compute_spec.get_tile_compute_query().get_combined_query_string()
    )
    assert_equal_with_expected_fixture(
        combined_tile_query,
        "tests/fixtures/query_graph/expected_combined_tile_query.sql",
        update_fixtures,
    )

    # feature_3 not combined
    assert result[1].tile_info == get_tile_infos(feature_3, source_info)[0]
    assert result[1].tile_table_groupings == [
        TileTableGrouping(
            value_column_names=[
                "sum_value_avg_3e4c470d4a020c9ecbac9525189b19d6a01e5b47",
                "count_value_avg_3e4c470d4a020c9ecbac9525189b19d6a01e5b47",
            ],
            value_column_types=[DBVarType.FLOAT, DBVarType.FLOAT],
            tile_id="TILE_AVG_3E4C470D4A020C9ECBAC9525189B19D6A01E5B47",
            aggregation_id="avg_3e4c470d4a020c9ecbac9525189b19d6a01e5b47",
        )
    ]


def test_tile_compute_combine_output_num_columns_limit(
    feature_many_tiles, source_info, update_fixtures
):
    """
    Test when combination is possible
    """
    result = combine_tile_compute_specs(
        get_tile_infos_for_features([feature_many_tiles], source_info),
        max_num_columns_per_query=10,
    )
    assert len(result) == 2

    for i in range(2):
        combined = result[i]
        combined_tile_query = combined.tile_info.tile_compute_spec.get_tile_compute_query().get_combined_query_string()
        assert_equal_with_expected_fixture(
            combined_tile_query,
            f"tests/fixtures/query_graph/expected_combined_tile_query_complex_{i}.sql",
            update_fixtures,
        )


def test_cross_aggregate_featurse_should_not_combine():
    """ """
