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


@pytest.fixture(name="aggregate_kwargs")
def aggregate_kwargs_fixture():
    """Fixture for a valid kwargs that can be used in aggregate()"""
    aggregate_kwargs = dict(
        value_column="value",
        method="sum",
        windows=["30m", "2h", "1d"],
        blind_spot="10m",
        frequency="30m",
        time_modulo_frequency="5m",
        feature_names=["sum_30m", "sum_2h", "sum_1d"],
    )
    return aggregate_kwargs


def run_groupby_and_get_tile_table_identifier(event_view, aggregate_kwargs, groupby_kwargs=None):
    """Helper function to run groupby().aggregate() on an EventView and retrieve the tile ID

    A prune step is included to simulate the actual workflow.
    """
    if groupby_kwargs is None:
        groupby_kwargs = {"by_keys": ["cust_id"]}
    feature_names = set(aggregate_kwargs["feature_names"])
    features = event_view.groupby(**groupby_kwargs).aggregate(**aggregate_kwargs)
    graph, node = GlobalQueryGraph().prune(features.node, feature_names)
    tile_id = get_tile_table_identifier(graph, node)
    return tile_id


@pytest.mark.parametrize(
    "overrides,expected_tile_id",
    [
        ({}, "sum_f1800_m300_b600_73745c67c6aadbb5bec07085c9df5601e25dd7a0"),
        ({"method": "max"}, "max_f1800_m300_b600_fb8eca2ad73a377833efcd8e22bb36e4fc0a98b2"),
        (
            {"value_column": "event_type"},
            "sum_f1800_m300_b600_75cbef54c369e56c0eaf76988e6d882b15864481",
        ),
        ({"frequency": "10m"}, "sum_f600_m300_b600_8b10bcaa2752efbf46a902c716717442ae6b54f8"),
        (
            {"time_modulo_frequency": "10m"},
            "sum_f1800_m600_b600_75ee56cfcf23b8a9c4f6c054bafa1ecd76774d97",
        ),
        ({"blind_spot": "20m"}, "sum_f1800_m300_b1200_bab672a8bd456b382654cd6dc018f89e16b63922"),
    ],
)
def test_tile_table_id__agg_parameters(event_view, aggregate_kwargs, overrides, expected_tile_id):
    """Test tile table IDs are expected given different aggregate() parameters"""
    aggregate_kwargs.update(overrides)
    tile_id = run_groupby_and_get_tile_table_identifier(event_view, aggregate_kwargs)
    assert tile_id == expected_tile_id


@pytest.mark.parametrize(
    "groupby_kwargs,expected_tile_id",
    [
        ({"by_keys": "cust_id"}, "sum_f1800_m300_b600_73745c67c6aadbb5bec07085c9df5601e25dd7a0"),
        # Single groupby key specified as a list should give the same result
        ({"by_keys": ["cust_id"]}, "sum_f1800_m300_b600_73745c67c6aadbb5bec07085c9df5601e25dd7a0"),
        # Changing the by_keys changes the tile ID
        ({"by_keys": "event_type"}, "sum_f1800_m300_b600_3c7e197e40e46d702c2fb2f042ac09f8ccc9359b"),
    ],
)
def test_tile_table_id__groupby_parameters(
    event_view, aggregate_kwargs, groupby_kwargs, expected_tile_id
):
    """Test tile table IDs are expected given different groupby() parameters"""
    tile_id = run_groupby_and_get_tile_table_identifier(
        event_view, aggregate_kwargs, groupby_kwargs
    )
    assert tile_id == expected_tile_id


def test_tile_table_id__transformations(event_view, aggregate_kwargs):
    """Test different transformations produce different tile table IDs"""

    event_view["value_10"] = event_view["value"] * 10
    event_view["value_100"] = event_view["value"] * 100

    # Tile table id based on value_10
    kwargs = copy.deepcopy(aggregate_kwargs)
    kwargs["value_column"] = "value_10"
    tile_id = run_groupby_and_get_tile_table_identifier(event_view, kwargs)
    assert tile_id == "sum_f1800_m300_b600_d1718c9dd7b88997626cfbbfe491bc9af9b4d064"

    # Note that this is different from above
    kwargs = copy.deepcopy(aggregate_kwargs)
    kwargs["value_column"] = "value_100"
    tile_id = run_groupby_and_get_tile_table_identifier(event_view, kwargs)
    assert tile_id == "sum_f1800_m300_b600_a2725c23f0cb003d5391ea04b1e1d99f4a61e3e2"
