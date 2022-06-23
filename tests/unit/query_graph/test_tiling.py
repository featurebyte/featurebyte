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
        ({}, "sum_f1800_m300_b600_660bb2c3146819a24651769312ba61a9c96465d1"),
        # Features with different windows can share the same tile table
        ({"windows": ["2d"]}, "sum_f1800_m300_b600_660bb2c3146819a24651769312ba61a9c96465d1"),
        ({"method": "max"}, "max_f1800_m300_b600_eacc788e0533df806f3539628ec37525175e8b8c"),
        (
            {"value_column": "event_type"},
            "sum_f1800_m300_b600_1e458330b6c20fc0a9ed9af399ee4444da1ba272",
        ),
        ({"frequency": "10m"}, "sum_f600_m300_b600_b039cedc9dd8a370aaf4bac8f6a519d20b17fe19"),
        (
            {"time_modulo_frequency": "10m"},
            "sum_f1800_m600_b600_16986f600ecc3c4c9895bd4e6cc945ca7f168c77",
        ),
        ({"blind_spot": "20m"}, "sum_f1800_m300_b1200_991a80d487ba89c3c7b2a48f1a8ac047a6b370e7"),
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
        ({"by_keys": "cust_id"}, "sum_f1800_m300_b600_660bb2c3146819a24651769312ba61a9c96465d1"),
        # Single groupby key specified as a list should give the same result
        ({"by_keys": ["cust_id"]}, "sum_f1800_m300_b600_660bb2c3146819a24651769312ba61a9c96465d1"),
        # Changing the by_keys changes the tile ID
        ({"by_keys": "event_type"}, "sum_f1800_m300_b600_b0b10aa55ae0ea91a1db9bc299f0dd5267723f16"),
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
    assert tile_id == "sum_f1800_m300_b600_06fedbc59011280762994b6938b37c263ba25032"

    # Note that this is different from above
    kwargs = copy.deepcopy(aggregate_kwargs)
    kwargs["value_column"] = "value_100"
    tile_id = run_groupby_and_get_tile_table_identifier(event_view, kwargs)
    assert tile_id == "sum_f1800_m300_b600_266a158991f9ff00ae7b0e20c4dd5cb96aa12a5a"
