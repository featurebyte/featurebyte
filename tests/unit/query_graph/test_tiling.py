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
    pruned_graph, node_name_map = GlobalQueryGraph().prune(
        features.node, feature_names, to_update_node_params=True
    )
    mapped_node = pruned_graph.get_node_by_name(node_name_map[features.node.name])
    tile_id_pruned = mapped_node.parameters["tile_id"]
    return tile_id, tile_id_pruned


@pytest.mark.parametrize(
    "overrides,expected_tile_id",
    [
        ({}, "sum_f1800_m300_b600_3cb3b2b28a359956be02abe635c4446cb50710d7"),
        # Features with different windows can share the same tile table
        ({"windows": ["2d"]}, "sum_f1800_m300_b600_3cb3b2b28a359956be02abe635c4446cb50710d7"),
        ({"method": "max"}, "max_f1800_m300_b600_02bc9f67f0c666e84b6a09f6e8084534dc982a80"),
        (
            {"value_column": "col_text"},
            "sum_f1800_m300_b600_3c09d1715368d408d1244918800cce729cd31b92",
        ),
        ({"frequency": "10m"}, "sum_f600_m300_b600_c63700067afb9f6d02d021bf1d6f8b83c1871063"),
        (
            {"time_modulo_frequency": "10m"},
            "sum_f1800_m600_b600_0feb7b35dda52f82b152f6e2a6ed01096e381de3",
        ),
        ({"blind_spot": "20m"}, "sum_f1800_m300_b1200_49fd91160e4b06d3dd7d18380c1afe999d1f2e84"),
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
        ({"by_keys": "cust_id"}, "sum_f1800_m300_b600_3cb3b2b28a359956be02abe635c4446cb50710d7"),
        # Single groupby key specified as a list should give the same result
        ({"by_keys": ["cust_id"]}, "sum_f1800_m300_b600_3cb3b2b28a359956be02abe635c4446cb50710d7"),
        # Changing the by_keys changes the tile ID
        ({"by_keys": "col_text"}, "sum_f1800_m300_b600_894737abbc14a7b6ff25e1944cc3cf9ba0e9d82e"),
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
    assert tile_id == "sum_f1800_m300_b600_56a9fe1d7aeda26cab2ecba5e008d596b6dbcd0d"
    assert tile_id_pruned == "sum_f1800_m300_b600_05c80637bc162f85ec822c13cbe794febc74275d"

    # Note that this is different from above
    kwargs = copy.deepcopy(aggregate_kwargs)
    kwargs["value_column"] = "value_100"
    tile_id, tile_id_pruned = run_groupby_and_get_tile_table_identifier(
        snowflake_event_view, kwargs
    )
    assert tile_id == "sum_f1800_m300_b600_c667f57df4712495696e65a37abca65c1806b4c4"
    assert tile_id_pruned == "sum_f1800_m300_b600_b5038376279f00109bc42e18aeb6087a712f17d7"
