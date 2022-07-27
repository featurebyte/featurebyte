"""
Tests for featurebyte.query_graph.tiling
"""
import copy

import pytest

from featurebyte.api.entity import Entity
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
        feature_names=["sum_30m", "sum_2h", "sum_1d"],
        feature_job_setting={
            "blind_spot": "10m",
            "frequency": "30m",
            "time_modulo_frequency": "5m",
        },
    )
    return aggregate_kwargs


def get_parent_nodes(query_graph, node):
    """
    Retrieve parent nodes from the graph
    """
    parent_node_names = query_graph.backward_edges[node.name]
    return [query_graph.get_node_by_name(name) for name in parent_node_names]


def run_groupby_and_get_tile_table_identifier(
    event_view, aggregate_kwargs, groupby_kwargs=None, create_entity=True
):
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
        assert isinstance(by_key, str)
        if create_entity:
            Entity(name=by_key, serving_names=[by_key]).save()
        event_view[by_key].as_entity(by_key)
    feature_names = set(aggregate_kwargs["feature_names"])
    features = event_view.groupby(**groupby_kwargs).aggregate(**aggregate_kwargs)
    groupby_node = get_parent_nodes(event_view.graph, features[list(feature_names)[0]].node)[0]
    tile_id = groupby_node.parameters["tile_id"]
    pruned_graph, node_name_map = GlobalQueryGraph().prune(groupby_node, feature_names)
    mapped_node = pruned_graph.get_node_by_name(node_name_map[groupby_node.name])
    tile_id_pruned = mapped_node.parameters["tile_id"]
    assert tile_id == tile_id_pruned
    return tile_id


@pytest.mark.parametrize(
    "overrides,expected_tile_id",
    [
        ({}, "sum_f1800_m300_b600_afb4d56e30a685ee9128bfa58fe4ad76d32af512"),
        # Features with different windows can share the same tile table
        (
            {"windows": ["2d"], "feature_names": ["sum_2d"]},
            "sum_f1800_m300_b600_afb4d56e30a685ee9128bfa58fe4ad76d32af512",
        ),
        ({"method": "max"}, "max_f1800_m300_b600_cbd0fa45cce09ac94d9fd4bc89e0ddbf1f151cee"),
        (
            {"value_column": "col_text"},
            "sum_f1800_m300_b600_82950d6e35c7289fdae7f20edd88babe269f0354",
        ),
        ({"frequency": "10m"}, "sum_f600_m300_b600_cb609cabca55ad1be6855483db67afdcd41915f6"),
        (
            {"time_modulo_frequency": "10m"},
            "sum_f1800_m600_b600_b723b6a591bc31f33f8405ec0315d10a6f9fb0e4",
        ),
        ({"blind_spot": "20m"}, "sum_f1800_m300_b1200_e03f9b37c116da43b65c609671c1d4d37752e7c1"),
    ],
)
def test_tile_table_id__agg_parameters(
    snowflake_event_view, aggregate_kwargs, overrides, expected_tile_id
):
    """Test tile table IDs are expected given different aggregate() parameters"""
    feature_job_setting_params = {"frequency", "blind_spot", "time_modulo_frequency"}
    for key in overrides:
        if key in feature_job_setting_params:
            aggregate_kwargs["feature_job_setting"][key] = overrides[key]
    aggregate_kwargs.update(
        {key: val for key, val in overrides.items() if key not in feature_job_setting_params}
    )
    tile_id = run_groupby_and_get_tile_table_identifier(snowflake_event_view, aggregate_kwargs)
    assert tile_id == expected_tile_id


@pytest.mark.parametrize(
    "groupby_kwargs,expected_tile_id",
    [
        ({"by_keys": "cust_id"}, "sum_f1800_m300_b600_afb4d56e30a685ee9128bfa58fe4ad76d32af512"),
        # Single groupby key specified as a list should give the same result
        ({"by_keys": ["cust_id"]}, "sum_f1800_m300_b600_afb4d56e30a685ee9128bfa58fe4ad76d32af512"),
        # Changing the by_keys changes the tile ID
        ({"by_keys": "col_text"}, "sum_f1800_m300_b600_ff5363c5a5f75ab8ede72ec5ad649be96ed97622"),
        # Changing the category changes the tile ID
        (
            {"by_keys": "col_text", "category": "col_int"},
            "sum_f1800_m300_b600_38bb33143e67a3003c17180db571cdd589e38a73",
        ),
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
    assert tile_id == "sum_f1800_m300_b600_4d8e697a102c013b0e3327c1b67b9b731fcf0e50"

    # Note that this is different from above
    kwargs = copy.deepcopy(aggregate_kwargs)
    kwargs["value_column"] = "value_100"
    tile_id = run_groupby_and_get_tile_table_identifier(
        snowflake_event_view, kwargs, create_entity=False
    )
    assert tile_id == "sum_f1800_m300_b600_af0872a1ec08217ad860361d7ae23f31afe5bc73"
