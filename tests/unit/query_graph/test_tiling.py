"""
Tests for featurebyte.query_graph.tiling
"""
import copy

import pytest

from featurebyte.api.entity import Entity
from featurebyte.api.event_data import EventData
from featurebyte.api.event_view import EventView
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.sql.tiling import AggFunc, TileSpec, get_aggregator


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
    features = event_view.groupby(**groupby_kwargs).aggregate_over(**aggregate_kwargs)
    groupby_node = get_parent_nodes(event_view.graph, features[list(feature_names)[0]].node)[0]
    tile_id = groupby_node.parameters.tile_id
    agg_id = groupby_node.parameters.aggregation_id
    pruned_graph, node_name_map = GlobalQueryGraph().prune(
        target_node=groupby_node, aggressive=True
    )
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
            "TILE_F1800_M300_B600_7BEF0E8B579190F960845A042B02B9BC538BD58E",
            "sum_a1a9657e29a711c4d09475bb8285da86250d2294",
        ),
        # Features with different windows can share the same tile table
        (
            {"windows": ["2d"], "feature_names": ["sum_2d"]},
            "TILE_F1800_M300_B600_7BEF0E8B579190F960845A042B02B9BC538BD58E",
            "sum_a1a9657e29a711c4d09475bb8285da86250d2294",
        ),
        (
            {"method": "max"},
            "TILE_F1800_M300_B600_7BEF0E8B579190F960845A042B02B9BC538BD58E",
            "max_77aff4637447d7e39aae06e2450bcceea29f5091",
        ),
        (
            {"value_column": "col_int"},
            "TILE_F1800_M300_B600_7BEF0E8B579190F960845A042B02B9BC538BD58E",
            "sum_fccea7adcc93eb0b5d82bb1b046ed85af302047e",
        ),
        (
            {"frequency": "10m"},
            "TILE_F600_M300_B600_8DBAE42DE55303D834763AC9CEF4F2CB36892796",
            "sum_b889e9245c35a0c83c59214bd896438740712db2",
        ),
        (
            {"time_modulo_frequency": "10m"},
            "TILE_F1800_M600_B600_3068970464C4549576A6D4D87991E4290113C5C9",
            "sum_4906c3058318d266c62fc7b0ea3535fa42977117",
        ),
        (
            {"blind_spot": "20m"},
            "TILE_F1800_M300_B1200_DA76FB682AB9469F4DD78584BDFE67E4615FC5A0",
            "sum_6bfb97f09d808f0e29566a49df5d62525ed2b346",
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
            "TILE_F1800_M300_B600_7BEF0E8B579190F960845A042B02B9BC538BD58E",
            "sum_a1a9657e29a711c4d09475bb8285da86250d2294",
        ),
        # Single groupby key specified as a list should give the same result
        (
            {"by_keys": ["cust_id"]},
            "TILE_F1800_M300_B600_7BEF0E8B579190F960845A042B02B9BC538BD58E",
            "sum_a1a9657e29a711c4d09475bb8285da86250d2294",
        ),
        # Changing the by_keys changes the tile ID
        (
            {"by_keys": "col_text"},
            "TILE_F1800_M300_B600_BC009ACDF0BE5A0E8C7566D0F23AF9D8928B7FE2",
            "sum_2c4b4b1dde248067cd9fa1d845562f92d8312911",
        ),
        # Changing the category changes the tile ID
        (
            {"by_keys": "col_text", "category": "col_int"},
            "TILE_F1800_M300_B600_77B2C7B4E9E15A547F5A8E135B31F4F95161541D",
            "sum_73226d9a8dfa93a2843f847364877a8627d8bdc2",
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
        "TILE_F1800_M300_B600_7BEF0E8B579190F960845A042B02B9BC538BD58E",
        "sum_40764234a63a0d95ce96b280071c6325320185b1",
    )

    # Note that this is different from above
    kwargs = copy.deepcopy(aggregate_kwargs)
    kwargs["value_column"] = "value_100"
    tile_id, agg_id = run_groupby_and_get_tile_table_identifier(
        snowflake_event_view_with_entity, kwargs, create_entity=False
    )
    assert (tile_id, agg_id) == (
        "TILE_F1800_M300_B600_7BEF0E8B579190F960845A042B02B9BC538BD58E",
        "sum_5d3d660c706349b0259a663b6c2877f8101a5a74",
    )
