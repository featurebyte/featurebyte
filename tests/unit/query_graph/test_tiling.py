"""
Tests for featurebyte.query_graph.tiling
"""
import copy

import pytest

from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.tiling import AggFunc, TileSpec, get_aggregator


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
    tile_id = features.node.parameters["tile_id"]
    _, node = GlobalQueryGraph().prune(features.node, feature_names, to_update_node_params=True)
    tile_id_pruned = node.parameters["tile_id"]
    return tile_id, tile_id_pruned


@pytest.mark.parametrize(
    "overrides,expected_tile_id",
    [
        ({}, "sum_f1800_m300_b600_2ffe099df53ee760d5a551c17707fedd0cf861f9"),
        # Features with different windows can share the same tile table
        ({"windows": ["2d"]}, "sum_f1800_m300_b600_2ffe099df53ee760d5a551c17707fedd0cf861f9"),
        ({"method": "max"}, "max_f1800_m300_b600_efa652c9b9058f625c8b5e131895167a5eb7d52b"),
        (
            {"value_column": "col_text"},
            "sum_f1800_m300_b600_4e6dbc6a82b5bae0feeea4c8573dd68aaad85303",
        ),
        ({"frequency": "10m"}, "sum_f600_m300_b600_637ddf299c88f16c8f6c42ea764af5f62ba7b337"),
        (
            {"time_modulo_frequency": "10m"},
            "sum_f1800_m600_b600_5395dd7f5a8d2e00e971f0d704143cf37e0e2482",
        ),
        ({"blind_spot": "20m"}, "sum_f1800_m300_b1200_0fa45f82d91add27c199a9db097890ddb5fc3dea"),
    ],
)
def test_tile_table_id__agg_parameters(
    snowflake_event_view, aggregate_kwargs, overrides, expected_tile_id
):
    """Test tile table IDs are expected given different aggregate() parameters"""
    aggregate_kwargs.update(overrides)
    _, tile_id = run_groupby_and_get_tile_table_identifier(snowflake_event_view, aggregate_kwargs)
    assert tile_id == expected_tile_id


@pytest.mark.parametrize(
    "groupby_kwargs,expected_tile_id",
    [
        ({"by_keys": "cust_id"}, "sum_f1800_m300_b600_2ffe099df53ee760d5a551c17707fedd0cf861f9"),
        # Single groupby key specified as a list should give the same result
        ({"by_keys": ["cust_id"]}, "sum_f1800_m300_b600_2ffe099df53ee760d5a551c17707fedd0cf861f9"),
        # Changing the by_keys changes the tile ID
        ({"by_keys": "col_text"}, "sum_f1800_m300_b600_2186338d961f41e2b2f2eca9d647ea7bfe2eb9ba"),
    ],
)
def test_tile_table_id__groupby_parameters(
    snowflake_event_view, aggregate_kwargs, groupby_kwargs, expected_tile_id
):
    """Test tile table IDs are expected given different groupby() parameters"""
    _, tile_id = run_groupby_and_get_tile_table_identifier(
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
    tile_id, tile_id_pruned = run_groupby_and_get_tile_table_identifier(
        snowflake_event_view, kwargs
    )
    assert tile_id == "sum_f1800_m300_b600_c45b692455d2664b355c3db5f1aac1534225a5fc"
    assert tile_id_pruned == "sum_f1800_m300_b600_7484940ec1348c0c173b652a0ea05d74cd969184"

    # Note that this is different from above
    kwargs = copy.deepcopy(aggregate_kwargs)
    kwargs["value_column"] = "value_100"
    tile_id, tile_id_pruned = run_groupby_and_get_tile_table_identifier(
        snowflake_event_view, kwargs
    )
    assert tile_id == "sum_f1800_m300_b600_c6c6a44b3e4982adfc39a1b1190c86b888b2e58f"
    assert tile_id_pruned == "sum_f1800_m300_b600_7c9083ba3613ce5645aee27409a65d6a8650bde2"
