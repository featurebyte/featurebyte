"""
Tests for featurebyte.query_graph.tiling
"""
import copy

import pytest

from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.tiling import (
    AggFunc,
    TileSpec,
    get_aggregator,
    get_tile_table_identifier,
)


@pytest.mark.parametrize(
    "agg_func,expected_tile_specs,expected_merge_expr",
    [
        (
            AggFunc.SUM,
            [TileSpec(tile_expr='SUM("a_column")', tile_column_name="value")],
            "SUM(value)",
        ),
        (
            AggFunc.AVG,
            [
                TileSpec(tile_expr='SUM("a_column")', tile_column_name="sum_value"),
                TileSpec(tile_expr="COUNT(*)", tile_column_name="count_value"),
            ],
            "SUM(sum_value) / SUM(count_value)",
        ),
        (
            AggFunc.MIN,
            [TileSpec(tile_expr='MIN("a_column")', tile_column_name="value")],
            "MIN(value)",
        ),
        (
            AggFunc.MAX,
            [TileSpec(tile_expr='MAX("a_column")', tile_column_name="value")],
            "MAX(value)",
        ),
        (AggFunc.COUNT, [TileSpec(tile_expr="COUNT(*)", tile_column_name="value")], "SUM(value)"),
        (
            AggFunc.NA_COUNT,
            [
                TileSpec(
                    tile_expr='SUM(CAST("a_column" IS NULL AS INTEGER))', tile_column_name="value"
                )
            ],
            "SUM(value)",
        ),
    ],
)
def test_tiling_aggregators(agg_func, expected_tile_specs, expected_merge_expr):
    """Test tiling aggregators produces expected expressions"""
    agg = get_aggregator(agg_func)
    tile_specs = agg.tile("a_column")
    merge_expr = agg.merge()
    assert tile_specs == expected_tile_specs
    assert merge_expr == expected_merge_expr


def test_tile_table_id__agg_parameters(event_view):

    feature_names = ["sum_30m", "sum_2h", "sum_1d"]
    base_kwargs = dict(
        value_column="value",
        method="sum",
        windows=["30m", "2h", "1d"],
        blind_spot="10m",
        frequency="30m",
        time_modulo_frequency="5m",
        feature_names=feature_names,
    )

    kwargs = copy.deepcopy(base_kwargs)
    features = event_view.groupby(["cust_id"]).aggregate(**kwargs)
    graph, node = GlobalQueryGraph().prune(features.node, set(feature_names))
    tile_id = get_tile_table_identifier(graph, node)
    assert tile_id == "sum_f1800_m300_b600_73745c67c6aadbb5bec07085c9df5601e25dd7a0"

    features = event_view.groupby(["event_type"]).aggregate(**kwargs)
    graph, node = GlobalQueryGraph().prune(features.node, set(feature_names))
    tile_id = get_tile_table_identifier(graph, node)
    assert tile_id == "sum_f1800_m300_b600_3c7e197e40e46d702c2fb2f042ac09f8ccc9359b"

    kwargs = copy.deepcopy(base_kwargs)
    features = event_view.groupby(["event_type"]).aggregate(**kwargs)
    graph, node = GlobalQueryGraph().prune(features.node, set(feature_names))
    tile_id = get_tile_table_identifier(graph, node)
    assert tile_id == "sum_f1800_m300_b600_3c7e197e40e46d702c2fb2f042ac09f8ccc9359b"

    kwargs = copy.deepcopy(base_kwargs)
    kwargs["method"] = "max"
    features = event_view.groupby(["cust_id"]).aggregate(**kwargs)
    graph, node = GlobalQueryGraph().prune(features.node, set(feature_names))
    tile_id = get_tile_table_identifier(graph, node)
    assert tile_id == "max_f1800_m300_b600_fb8eca2ad73a377833efcd8e22bb36e4fc0a98b2"


def test_tile_table_id__job_setting(event_view):

    feature_names = ["sum_30m", "sum_2h", "sum_1d"]
    base_kwargs = dict(
        value_column="value",
        method="sum",
        windows=["30m", "2h", "1d"],
        blind_spot="10m",
        frequency="30m",
        time_modulo_frequency="5m",
        feature_names=feature_names,
    )

    kwargs = copy.deepcopy(base_kwargs)
    kwargs["frequency"] = "10m"
    features = event_view.groupby(["cust_id"]).aggregate(**kwargs)
    graph, node = GlobalQueryGraph().prune(features.node, set(feature_names))
    tile_id = get_tile_table_identifier(graph, node)
    assert tile_id == "sum_f600_m300_b600_8b10bcaa2752efbf46a902c716717442ae6b54f8"

    kwargs = copy.deepcopy(base_kwargs)
    kwargs["time_modulo_frequency"] = "10m"
    features = event_view.groupby(["cust_id"]).aggregate(**kwargs)
    graph, node = GlobalQueryGraph().prune(features.node, set(feature_names))
    tile_id = get_tile_table_identifier(graph, node)
    assert tile_id == "sum_f1800_m600_b600_75ee56cfcf23b8a9c4f6c054bafa1ecd76774d97"

    kwargs = copy.deepcopy(base_kwargs)
    kwargs["blind_spot"] = "20m"
    features = event_view.groupby(["cust_id"]).aggregate(**kwargs)
    graph, node = GlobalQueryGraph().prune(features.node, set(feature_names))
    tile_id = get_tile_table_identifier(graph, node)
    assert tile_id == "sum_f1800_m300_b1200_bab672a8bd456b382654cd6dc018f89e16b63922"


def test_tile_table_id__transformations(event_view):

    feature_names = ["sum_30m", "sum_2h", "sum_1d"]
    base_kwargs = dict(
        value_column="value",
        method="sum",
        windows=["30m", "2h", "1d"],
        blind_spot="10m",
        frequency="30m",
        time_modulo_frequency="5m",
        feature_names=feature_names,
    )

    event_view["value_10"] = event_view["value"] * 10
    event_view["value_100"] = event_view["value"] * 100

    kwargs = copy.deepcopy(base_kwargs)
    kwargs["value_column"] = "value_10"
    features = event_view.groupby(["cust_id"]).aggregate(**kwargs)
    graph, node = GlobalQueryGraph().prune(features.node, set(feature_names))
    tile_id = get_tile_table_identifier(graph, node)
    assert tile_id == "sum_f1800_m300_b600_d1718c9dd7b88997626cfbbfe491bc9af9b4d064"

    kwargs = copy.deepcopy(base_kwargs)
    kwargs["value_column"] = "value_100"
    features = event_view.groupby(["cust_id"]).aggregate(**kwargs)
    graph, node = GlobalQueryGraph().prune(features.node, set(feature_names))
    tile_id = get_tile_table_identifier(graph, node)
    assert tile_id == "sum_f1800_m300_b600_a2725c23f0cb003d5391ea04b1e1d99f4a61e3e2"
