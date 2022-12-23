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
            "TILE_F1800_M300_B600_C4876073C3B42D1C2D9D6942652545B3B4D3F178",
            "sum_fba233e0f502088c233315a322f4c51e939072c0",
        ),
        # Features with different windows can share the same tile table
        (
            {"windows": ["2d"], "feature_names": ["sum_2d"]},
            "TILE_F1800_M300_B600_C4876073C3B42D1C2D9D6942652545B3B4D3F178",
            "sum_fba233e0f502088c233315a322f4c51e939072c0",
        ),
        (
            {"method": "max"},
            "TILE_F1800_M300_B600_C4876073C3B42D1C2D9D6942652545B3B4D3F178",
            "max_06e7a7940e9e969f575a5cf733148b262c89343b",
        ),
        (
            {"value_column": "col_int"},
            "TILE_F1800_M300_B600_C4876073C3B42D1C2D9D6942652545B3B4D3F178",
            "sum_3beb4c553c758a940a017d611784ad1e5b89a357",
        ),
        (
            {"frequency": "10m"},
            "TILE_F600_M300_B600_E422506C03178D4B52385B0F3BA70B94CB9EDE58",
            "sum_f566208764ca39554a5c67d3b2534e9b64f31078",
        ),
        (
            {"time_modulo_frequency": "10m"},
            "TILE_F1800_M600_B600_3CB4913C9E0B09BF3542D2942FBCD0CB32D0A0B2",
            "sum_5111df860cbead5861ca362cbc546aa69da34398",
        ),
        (
            {"blind_spot": "20m"},
            "TILE_F1800_M300_B1200_99BB18FD49FF2E84C7F28DBC9C01AAA67C0683F5",
            "sum_7f85622077df24997429dee5446df950237ef60d",
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
            "TILE_F1800_M300_B600_C4876073C3B42D1C2D9D6942652545B3B4D3F178",
            "sum_fba233e0f502088c233315a322f4c51e939072c0",
        ),
        # Single groupby key specified as a list should give the same result
        (
            {"by_keys": ["cust_id"]},
            "TILE_F1800_M300_B600_C4876073C3B42D1C2D9D6942652545B3B4D3F178",
            "sum_fba233e0f502088c233315a322f4c51e939072c0",
        ),
        # Changing the by_keys changes the tile ID
        (
            {"by_keys": "col_text"},
            "TILE_F1800_M300_B600_8858C18F37F9897FFCA2A723B1183A7F945645F4",
            "sum_a3cfe30492599dbf87fd71a300a18ec41af52fe8",
        ),
        # Changing the category changes the tile ID
        (
            {"by_keys": "col_text", "category": "col_int"},
            "TILE_F1800_M300_B600_F84DE08D01CCB1ADDA538F1292061A6EDDD27328",
            "sum_621b30a0808ba24bfa773dfed14a5c719994e9b9",
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
        "TILE_F1800_M300_B600_C4876073C3B42D1C2D9D6942652545B3B4D3F178",
        "sum_bc29022c8400d0381e5f82a9a23ba21aaa855a2e",
    )

    # Note that this is different from above
    kwargs = copy.deepcopy(aggregate_kwargs)
    kwargs["value_column"] = "value_100"
    tile_id, agg_id = run_groupby_and_get_tile_table_identifier(
        snowflake_event_view_with_entity, kwargs, create_entity=False
    )
    assert (tile_id, agg_id) == (
        "TILE_F1800_M300_B600_C4876073C3B42D1C2D9D6942652545B3B4D3F178",
        "sum_6523d9be0d8565c81425140302be4397c923fc52",
    )


def test_tile_table_id__filter(snowflake_event_view_with_entity, aggregate_kwargs):
    """Test different filters produce different tile id"""
    view = snowflake_event_view_with_entity
    view_filtered = view[view["col_int"] > 10]

    tile_id, _ = run_groupby_and_get_tile_table_identifier(
        snowflake_event_view_with_entity, aggregate_kwargs, create_entity=False
    )
    tile_id_filtered, _ = run_groupby_and_get_tile_table_identifier(
        view_filtered, aggregate_kwargs, create_entity=False
    )

    # tile_ids is different due to different row index lineage
    assert tile_id != tile_id_filtered
