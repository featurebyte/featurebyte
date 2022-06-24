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
        value_column="col_float",
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
        ({}, "sum_f1800_m300_b600_9ae7bc18fa2359bc2365a62c32ede56123b52bba"),
        # Features with different windows can share the same tile table
        ({"windows": ["2d"]}, "sum_f1800_m300_b600_9ae7bc18fa2359bc2365a62c32ede56123b52bba"),
        ({"method": "max"}, "max_f1800_m300_b600_83abe49b7883ec71605569439b6fab89c6dc527a"),
        (
            {"value_column": "col_text"},
            "sum_f1800_m300_b600_5e9b9d763e1cef8bb43fa65c8c6f1ff669be6041",
        ),
        ({"frequency": "10m"}, "sum_f600_m300_b600_edba425e56315b6e5851b64ea2152301fede2bc9"),
        (
            {"time_modulo_frequency": "10m"},
            "sum_f1800_m600_b600_57031d2b70c52792a7d2b186ce545a556fde2e92",
        ),
        ({"blind_spot": "20m"}, "sum_f1800_m300_b1200_a00be7bb183c554c4e93ccbcda69018b65f4a726"),
    ],
)
def test_tile_table_id__agg_parameters(
    snowflake_event_view, aggregate_kwargs, overrides, expected_tile_id
):
    """Test tile table IDs are expected given different aggregate() parameters"""
    aggregate_kwargs.update(overrides)
    tile_id = run_groupby_and_get_tile_table_identifier(snowflake_event_view, aggregate_kwargs)
    assert tile_id == expected_tile_id


@pytest.mark.parametrize(
    "groupby_kwargs,expected_tile_id",
    [
        ({"by_keys": "cust_id"}, "sum_f1800_m300_b600_9ae7bc18fa2359bc2365a62c32ede56123b52bba"),
        # Single groupby key specified as a list should give the same result
        ({"by_keys": ["cust_id"]}, "sum_f1800_m300_b600_9ae7bc18fa2359bc2365a62c32ede56123b52bba"),
        # Changing the by_keys changes the tile ID
        ({"by_keys": "col_text"}, "sum_f1800_m300_b600_3b7b12b81d4071a9a47780483549741e7e8fd9ae"),
    ],
)
def test_tile_table_id__groupby_parameters(
    snowflake_event_view, aggregate_kwargs, groupby_kwargs, expected_tile_id
):
    """Test tile table IDs are expected given different groupby() parameters"""
    tile_id = run_groupby_and_get_tile_table_identifier(
        snowflake_event_view, aggregate_kwargs, groupby_kwargs
    )
    assert tile_id == expected_tile_id


def test_tile_table_id__transformations(snowflake_event_view, aggregate_kwargs):
    """Test different transformations produce different tile table IDs"""

    snowflake_event_view["value_10"] = snowflake_event_view["col_float"] * 10
    snowflake_event_view["value_100"] = snowflake_event_view["col_float"] * 100

    # Tile table id based on value_10
    kwargs = copy.deepcopy(aggregate_kwargs)
    kwargs["value_column"] = "value_10"
    tile_id = run_groupby_and_get_tile_table_identifier(snowflake_event_view, kwargs)
    assert tile_id == "sum_f1800_m300_b600_426b82f0d71c320120f0aa9999a105eb67cac2f4"

    # Note that this is different from above
    kwargs = copy.deepcopy(aggregate_kwargs)
    kwargs["value_column"] = "value_100"
    tile_id = run_groupby_and_get_tile_table_identifier(snowflake_event_view, kwargs)
    assert tile_id == "sum_f1800_m300_b600_ba0b2dd2b50a20480ccc92b940d37ca0e7afffee"
