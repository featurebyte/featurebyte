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
            [TileSpec(tile_expr='SUM("a_column")', tile_column_name="value_1234beef")],
            "SUM(value_1234beef)",
        ),
        (
            AggFunc.AVG,
            [
                TileSpec(tile_expr='SUM("a_column")', tile_column_name="sum_value_1234beef"),
                TileSpec(tile_expr='COUNT("a_column")', tile_column_name="count_value_1234beef"),
            ],
            "SUM(sum_value_1234beef) / SUM(count_value_1234beef)",
        ),
        (
            AggFunc.MIN,
            [TileSpec(tile_expr='MIN("a_column")', tile_column_name="value_1234beef")],
            "MIN(value_1234beef)",
        ),
        (
            AggFunc.MAX,
            [TileSpec(tile_expr='MAX("a_column")', tile_column_name="value_1234beef")],
            "MAX(value_1234beef)",
        ),
        (
            AggFunc.COUNT,
            [TileSpec(tile_expr="COUNT(*)", tile_column_name="value_1234beef")],
            "SUM(value_1234beef)",
        ),
        (
            AggFunc.NA_COUNT,
            [
                TileSpec(
                    tile_expr='SUM(CAST("a_column" IS NULL AS INTEGER))',
                    tile_column_name="value_1234beef",
                )
            ],
            "SUM(value_1234beef)",
        ),
    ],
)
def test_tiling_aggregators(agg_func, expected_tile_specs, expected_merge_expr):
    """Test tiling aggregators produces expected expressions"""
    agg_id = "1234beef"
    agg = get_aggregator(agg_func)
    tile_specs = agg.tile("a_column", agg_id)
    merge_expr = agg.merge(agg_id)
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
    agg_id = groupby_node.parameters["aggregation_id"]
    pruned_graph, node_name_map = GlobalQueryGraph().prune(groupby_node, feature_names)
    mapped_node = pruned_graph.get_node_by_name(node_name_map[groupby_node.name])
    tile_id_pruned = mapped_node.parameters["tile_id"]
    agg_id_pruned = mapped_node.parameters["aggregation_id"]
    assert tile_id == tile_id_pruned
    assert agg_id == agg_id_pruned
    return tile_id, agg_id


@pytest.mark.parametrize(
    "overrides,expected_tile_id,expected_agg_id",
    [
        (
            {},
            "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
            "sum_afb4d56e30a685ee9128bfa58fe4ad76d32af512",
        ),
        # Features with different windows can share the same tile table
        (
            {"windows": ["2d"], "feature_names": ["sum_2d"]},
            "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
            "sum_afb4d56e30a685ee9128bfa58fe4ad76d32af512",
        ),
        (
            {"method": "max"},
            "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
            "max_cbd0fa45cce09ac94d9fd4bc89e0ddbf1f151cee",
        ),
        (
            {"value_column": "col_text"},
            "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
            "sum_82950d6e35c7289fdae7f20edd88babe269f0354",
        ),
        (
            {"frequency": "10m"},
            "sf_table_f600_m300_b600_e9fcf46f6cde49a7c3a01aad14ee484840d2ceab",
            "sum_cb609cabca55ad1be6855483db67afdcd41915f6",
        ),
        (
            {"time_modulo_frequency": "10m"},
            "sf_table_f1800_m600_b600_026c372591ec0824894aa77fc4dbef4590795f9c",
            "sum_b723b6a591bc31f33f8405ec0315d10a6f9fb0e4",
        ),
        (
            {"blind_spot": "20m"},
            "sf_table_f1800_m300_b1200_a8d10831a92043738b09ab9075da58964831e385",
            "sum_e03f9b37c116da43b65c609671c1d4d37752e7c1",
        ),
    ],
)
def test_tile_table_id__agg_parameters(
    snowflake_event_view, aggregate_kwargs, overrides, expected_tile_id, expected_agg_id
):
    """Test tile table IDs are expected given different aggregate() parameters"""
    feature_job_setting_params = {"frequency", "blind_spot", "time_modulo_frequency"}
    for key in overrides:
        if key in feature_job_setting_params:
            aggregate_kwargs["feature_job_setting"][key] = overrides[key]
    aggregate_kwargs.update(
        {key: val for key, val in overrides.items() if key not in feature_job_setting_params}
    )
    tile_id, agg_id = run_groupby_and_get_tile_table_identifier(
        snowflake_event_view, aggregate_kwargs
    )
    assert (tile_id, agg_id) == (expected_tile_id, expected_agg_id)


@pytest.mark.parametrize(
    "groupby_kwargs,expected_tile_id, expected_agg_id",
    [
        (
            {"by_keys": "cust_id"},
            "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
            "sum_afb4d56e30a685ee9128bfa58fe4ad76d32af512",
        ),
        # Single groupby key specified as a list should give the same result
        (
            {"by_keys": ["cust_id"]},
            "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
            "sum_afb4d56e30a685ee9128bfa58fe4ad76d32af512",
        ),
        # Changing the by_keys changes the tile ID
        (
            {"by_keys": "col_text"},
            "sf_table_f1800_m300_b600_9b284bdefcc7f4da22fe1aa343af446d14c09836",
            "sum_ff5363c5a5f75ab8ede72ec5ad649be96ed97622",
        ),
        # Changing the category changes the tile ID
        (
            {"by_keys": "col_text", "category": "col_int"},
            "sf_table_f1800_m300_b600_f4cde374b421fc312888621e6fcc2ea160437bfa",
            "sum_38bb33143e67a3003c17180db571cdd589e38a73",
        ),
    ],
)
def test_tile_table_id__groupby_parameters(
    snowflake_event_view, aggregate_kwargs, groupby_kwargs, expected_tile_id, expected_agg_id
):
    """Test tile table IDs are expected given different groupby() parameters"""
    tile_id, agg_id = run_groupby_and_get_tile_table_identifier(
        snowflake_event_view, aggregate_kwargs, groupby_kwargs
    )
    assert (tile_id, agg_id) == (expected_tile_id, expected_agg_id)


def test_tile_table_id__transformations(snowflake_event_view, aggregate_kwargs):
    """Test different transformations produce different aggregation IDs, but same tile ID"""
    snowflake_event_view["value_10"] = snowflake_event_view["col_float"] * 10
    snowflake_event_view["value_100"] = snowflake_event_view["col_float"] * 100

    # Tile table id based on value_10
    kwargs = copy.deepcopy(aggregate_kwargs)
    kwargs["value_column"] = "value_10"
    tile_id, agg_id = run_groupby_and_get_tile_table_identifier(snowflake_event_view, kwargs)
    assert (tile_id, agg_id) == (
        "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
        "sum_657dcf96c117cdfe5928aaff963eb1eeb6d11027",
    )

    # Note that this is different from above
    kwargs = copy.deepcopy(aggregate_kwargs)
    kwargs["value_column"] = "value_100"
    tile_id, agg_id = run_groupby_and_get_tile_table_identifier(
        snowflake_event_view, kwargs, create_entity=False
    )
    assert (tile_id, agg_id) == (
        "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
        "sum_81483c0c3e2b4a1dc86100ce26c99012aa937bd5",
    )
