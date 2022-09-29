"""
Tests for featurebyte.query_graph.tiling
"""
import copy

import pytest

from featurebyte.api.entity import Entity
from featurebyte.api.event_data import EventData
from featurebyte.api.event_view import EventView
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
        (
            AggFunc.STD,
            [
                TileSpec(
                    tile_expr='SUM("a_column" * "a_column")',
                    tile_column_name="sum_value_squared_1234beef",
                ),
                TileSpec(tile_expr='SUM("a_column")', tile_column_name="sum_value_1234beef"),
                TileSpec(tile_expr='COUNT("a_column")', tile_column_name="count_value_1234beef"),
            ],
            (
                "SQRT(CASE WHEN ({variance}) < 0 THEN 0 ELSE ({variance}) END)".format(
                    variance=(
                        "(SUM(sum_value_squared_1234beef) / SUM(count_value_1234beef)) - "
                        "((SUM(sum_value_1234beef) / SUM(count_value_1234beef)) * "
                        "(SUM(sum_value_1234beef) / SUM(count_value_1234beef)))"
                    )
                )
            ),
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
    parent_node_names = query_graph.backward_edges_map[node.name]
    return [query_graph.get_node_by_name(name) for name in parent_node_names]


def run_groupby_and_get_tile_table_identifier(
    event_data_or_event_view, aggregate_kwargs, groupby_kwargs=None, create_entity=True
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
    event_view = event_data_or_event_view
    for by_key in by_keys:
        assert isinstance(by_key, str)
        if create_entity:
            Entity(name=by_key, serving_names=[by_key]).save()
        if isinstance(event_data_or_event_view, EventData):
            event_data_or_event_view[by_key].as_entity(by_key)
            event_view = EventView.from_event_data(event_data=event_data_or_event_view)

    feature_names = set(aggregate_kwargs["feature_names"])
    features = event_view.groupby(**groupby_kwargs).aggregate(**aggregate_kwargs)
    groupby_node = get_parent_nodes(event_view.graph, features[list(feature_names)[0]].node)[0]
    tile_id = groupby_node.parameters.tile_id
    agg_id = groupby_node.parameters.aggregation_id
    pruned_graph, node_name_map = GlobalQueryGraph().prune(groupby_node, feature_names)
    mapped_node = pruned_graph.get_node_by_name(node_name_map[groupby_node.name])
    tile_id_pruned = mapped_node.parameters.tile_id
    agg_id_pruned = mapped_node.parameters.aggregation_id
    assert tile_id == tile_id_pruned
    assert agg_id == agg_id_pruned
    return tile_id, agg_id


@pytest.mark.parametrize(
    "overrides,expected_tile_id,expected_agg_id",
    [
        (
            {},
            "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
            "sum_9b3973191f893fcce4eb8bf640bc33b34fd9078b",
        ),
        # Features with different windows can share the same tile table
        (
            {"windows": ["2d"], "feature_names": ["sum_2d"]},
            "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
            "sum_9b3973191f893fcce4eb8bf640bc33b34fd9078b",
        ),
        (
            {"method": "max"},
            "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
            "max_ce363fb1cf434719ec212e8d6e9c8a002bbb3d6c",
        ),
        (
            {"value_column": "col_text"},
            "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
            "sum_e715e05cee29c7f39690667184eab9bf2d368299",
        ),
        (
            {"frequency": "10m"},
            "sf_table_f600_m300_b600_e9fcf46f6cde49a7c3a01aad14ee484840d2ceab",
            "sum_6cd6aacbde10196f2f42d139ed9ba4ec44cecdae",
        ),
        (
            {"time_modulo_frequency": "10m"},
            "sf_table_f1800_m600_b600_026c372591ec0824894aa77fc4dbef4590795f9c",
            "sum_3d1735e6e90c0af13369cbabf5e437c6d5caca0d",
        ),
        (
            {"blind_spot": "20m"},
            "sf_table_f1800_m300_b1200_a8d10831a92043738b09ab9075da58964831e385",
            "sum_1d1278f11ef6e7337c8c8872012aa66e4d31eb2b",
        ),
    ],
)
def test_tile_table_id__agg_parameters(
    snowflake_event_data, aggregate_kwargs, overrides, expected_tile_id, expected_agg_id
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
        snowflake_event_data, aggregate_kwargs
    )
    assert (tile_id, agg_id) == (expected_tile_id, expected_agg_id)


@pytest.mark.parametrize(
    "groupby_kwargs,expected_tile_id, expected_agg_id",
    [
        (
            {"by_keys": "cust_id"},
            "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
            "sum_9b3973191f893fcce4eb8bf640bc33b34fd9078b",
        ),
        # Single groupby key specified as a list should give the same result
        (
            {"by_keys": ["cust_id"]},
            "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
            "sum_9b3973191f893fcce4eb8bf640bc33b34fd9078b",
        ),
        # Changing the by_keys changes the tile ID
        (
            {"by_keys": "col_text"},
            "sf_table_f1800_m300_b600_9b284bdefcc7f4da22fe1aa343af446d14c09836",
            "sum_4e9688d18a6aecd108e7c065a2adabd11db97d7f",
        ),
        # Changing the category changes the tile ID
        (
            {"by_keys": "col_text", "category": "col_int"},
            "sf_table_f1800_m300_b600_f4cde374b421fc312888621e6fcc2ea160437bfa",
            "sum_85587b8c3d7d491bcb693b0ec3a04ce848a119c9",
        ),
    ],
)
def test_tile_table_id__groupby_parameters(
    snowflake_event_data, aggregate_kwargs, groupby_kwargs, expected_tile_id, expected_agg_id
):
    """Test tile table IDs are expected given different groupby() parameters"""
    tile_id, agg_id = run_groupby_and_get_tile_table_identifier(
        snowflake_event_data, aggregate_kwargs, groupby_kwargs
    )
    assert (tile_id, agg_id) == (expected_tile_id, expected_agg_id)


def test_tile_table_id__transformations(snowflake_event_view_with_entity, aggregate_kwargs):
    """Test different transformations produce different aggregation IDs, but same tile ID"""
    snowflake_event_view_with_entity["value_10"] = (
        snowflake_event_view_with_entity["col_float"] * 10
    )
    snowflake_event_view_with_entity["value_100"] = (
        snowflake_event_view_with_entity["col_float"] * 100
    )

    # Tile table id based on value_10
    kwargs = copy.deepcopy(aggregate_kwargs)
    kwargs["value_column"] = "value_10"
    tile_id, agg_id = run_groupby_and_get_tile_table_identifier(
        snowflake_event_view_with_entity, kwargs, create_entity=False
    )
    assert (tile_id, agg_id) == (
        "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
        "sum_d2e853f90ba4564ee371f215f5b05137a97935e1",
    )

    # Note that this is different from above
    kwargs = copy.deepcopy(aggregate_kwargs)
    kwargs["value_column"] = "value_100"
    tile_id, agg_id = run_groupby_and_get_tile_table_identifier(
        snowflake_event_view_with_entity, kwargs, create_entity=False
    )
    assert (tile_id, agg_id) == (
        "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
        "sum_392be8924d7164dbb5f51163a355a37c372a54ce",
    )
