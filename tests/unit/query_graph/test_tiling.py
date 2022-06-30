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
    by_keys = (
        [groupby_kwargs["by_keys"]]
        if isinstance(groupby_kwargs["by_keys"], str)
        else groupby_kwargs["by_keys"]
    )
    for by_key in by_keys:
        event_view[by_key].as_entity(by_key)
    feature_names = set(aggregate_kwargs["feature_names"])
    features = event_view.groupby(**groupby_kwargs).aggregate(**aggregate_kwargs)
    graph, node = GlobalQueryGraph().prune(features.node, feature_names)
    tile_id = get_tile_table_identifier(graph, node)
    return tile_id


@pytest.mark.parametrize(
    "overrides,expected_tile_id",
    [
        ({}, "sum_f1800_m300_b600_44113074bc882e9d8affb2dff7bd111237d66e3a"),
        # Features with different windows can share the same tile table
        ({"windows": ["2d"]}, "sum_f1800_m300_b600_44113074bc882e9d8affb2dff7bd111237d66e3a"),
        ({"method": "max"}, "max_f1800_m300_b600_5b549d30fadbd81c5958d7a676f10b99dbd65c4f"),
        (
            {"value_column": "col_text"},
            "sum_f1800_m300_b600_cb4376f330e3d5f4c7db753e020d14ff79875091",
        ),
        ({"frequency": "10m"}, "sum_f600_m300_b600_d5f4f3f064b7de163959c8c2c8be5dbf0cca1828"),
        (
            {"time_modulo_frequency": "10m"},
            "sum_f1800_m600_b600_2432581346ad7cab96f50fa9ccc163e626fff884",
        ),
        ({"blind_spot": "20m"}, "sum_f1800_m300_b1200_0378c8e11034e509943bbbf6bcf49bac52699676"),
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
        ({"by_keys": "cust_id"}, "sum_f1800_m300_b600_44113074bc882e9d8affb2dff7bd111237d66e3a"),
        # Single groupby key specified as a list should give the same result
        ({"by_keys": ["cust_id"]}, "sum_f1800_m300_b600_44113074bc882e9d8affb2dff7bd111237d66e3a"),
        # Changing the by_keys changes the tile ID
        ({"by_keys": "col_text"}, "sum_f1800_m300_b600_4c3a9f6f9ca1ece42d246a8e7e0c2bff268794e7"),
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
    assert tile_id == "sum_f1800_m300_b600_36db745e2292597f69ecfa050d0b24f29d54b15b"

    # Note that this is different from above
    kwargs = copy.deepcopy(aggregate_kwargs)
    kwargs["value_column"] = "value_100"
    tile_id = run_groupby_and_get_tile_table_identifier(snowflake_event_view, kwargs)
    assert tile_id == "sum_f1800_m300_b600_67002506b482cef59abf1e55f8fc50346919839a"
