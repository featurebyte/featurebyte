"""
Tests for featurebyte.query_graph.tiling
"""
import copy

import pytest

from featurebyte.api.entity import Entity
from featurebyte.api.event_table import EventTable
from featurebyte.enum import DBVarType, SourceType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.tiling import AggFunc, InputColumn, get_aggregator


def make_expected_tile_spec(tile_expr, tile_column_name, tile_column_type=None):
    """
    Helper function to create the expected tile spec as a dictionary
    """
    if tile_column_type is None:
        tile_column_type = "FLOAT"
    return {
        "tile_expr": tile_expr,
        "tile_column_name": tile_column_name,
        "tile_column_type": tile_column_type,
    }


@pytest.mark.parametrize(
    "agg_func,expected_tile_specs,expected_merge_expr",
    [
        (
            AggFunc.SUM,
            [
                make_expected_tile_spec(
                    tile_expr='SUM("a_column")', tile_column_name="value_1234beef"
                )
            ],
            "SUM(value_1234beef)",
        ),
        (
            AggFunc.AVG,
            [
                make_expected_tile_spec(
                    tile_expr='SUM("a_column")', tile_column_name="sum_value_1234beef"
                ),
                make_expected_tile_spec(
                    tile_expr='COUNT("a_column")', tile_column_name="count_value_1234beef"
                ),
            ],
            "SUM(sum_value_1234beef) / SUM(count_value_1234beef)",
        ),
        (
            AggFunc.MIN,
            [
                make_expected_tile_spec(
                    tile_expr='MIN("a_column")', tile_column_name="value_1234beef"
                )
            ],
            "MIN(value_1234beef)",
        ),
        (
            AggFunc.MAX,
            [
                make_expected_tile_spec(
                    tile_expr='MAX("a_column")', tile_column_name="value_1234beef"
                )
            ],
            "MAX(value_1234beef)",
        ),
        (
            AggFunc.COUNT,
            [make_expected_tile_spec(tile_expr="COUNT(*)", tile_column_name="value_1234beef")],
            "SUM(value_1234beef)",
        ),
        (
            AggFunc.NA_COUNT,
            [
                make_expected_tile_spec(
                    tile_expr='SUM(CAST("a_column" IS NULL AS INTEGER))',
                    tile_column_name="value_1234beef",
                )
            ],
            "SUM(value_1234beef)",
        ),
        (
            AggFunc.STD,
            [
                make_expected_tile_spec(
                    tile_expr='SUM("a_column" * "a_column")',
                    tile_column_name="sum_value_squared_1234beef",
                ),
                make_expected_tile_spec(
                    tile_expr='SUM("a_column")', tile_column_name="sum_value_1234beef"
                ),
                make_expected_tile_spec(
                    tile_expr='COUNT("a_column")', tile_column_name="count_value_1234beef"
                ),
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
        (
            AggFunc.LATEST,
            [
                make_expected_tile_spec(
                    tile_expr='FIRST_VALUE("a_column")',
                    tile_column_name="value_1234beef",
                    tile_column_type="VARCHAR",
                )
            ],
            "FIRST_VALUE(value_1234beef)",
        ),
    ],
)
def test_tiling_aggregators(agg_func, expected_tile_specs, expected_merge_expr):
    """Test tiling aggregators produces expected expressions"""
    agg_id = "1234beef"
    agg = get_aggregator(agg_func, adapter=get_sql_adapter(SourceType.SNOWFLAKE))
    input_column = InputColumn(name="a_column", dtype=DBVarType.VARCHAR)
    tile_specs = agg.tile(input_column, agg_id)
    merge_expr = agg.merge(agg_id)
    assert [t.tile_expr.sql() for t in tile_specs] == [t["tile_expr"] for t in expected_tile_specs]
    assert [t.tile_column_name for t in tile_specs] == [
        t["tile_column_name"] for t in expected_tile_specs
    ]
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
    event_table_or_event_view, aggregate_kwargs, groupby_kwargs=None, create_entity=True
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
    event_view = event_table_or_event_view
    for by_key in by_keys:
        assert isinstance(by_key, str)
        if create_entity:
            Entity(name=by_key, serving_names=[by_key]).save()
        if isinstance(event_table_or_event_view, EventTable):
            event_table_or_event_view[by_key].as_entity(by_key)
            event_view = event_table_or_event_view.get_view()

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
            "TILE_F1800_M300_B600_B5CAF33CCFEDA76C257EC2CB7F66C4AD22009B0F",
            "sum_aed233b0e8a6e1c1e0d5427b126b03c949609481",
        ),
        # Features with different windows can share the same tile table
        (
            {"windows": ["2d"], "feature_names": ["sum_2d"]},
            "TILE_F1800_M300_B600_B5CAF33CCFEDA76C257EC2CB7F66C4AD22009B0F",
            "sum_aed233b0e8a6e1c1e0d5427b126b03c949609481",
        ),
        (
            {"method": "max"},
            "TILE_F1800_M300_B600_B5CAF33CCFEDA76C257EC2CB7F66C4AD22009B0F",
            "max_a0e8d39bef4c791f22bdc5f264234c12dc4e81d4",
        ),
        (
            {"value_column": "col_int"},
            "TILE_F1800_M300_B600_B5CAF33CCFEDA76C257EC2CB7F66C4AD22009B0F",
            "sum_34b69528f90906fc849330a206bad14c842a81ad",
        ),
        (
            {"frequency": "10m"},
            "TILE_F600_M300_B600_9EF598EF844B1DED25B255CCCBC231FBBF81A349",
            "sum_b75c6308ff1d3fe574de6f87625f1cacfc2bc755",
        ),
        (
            {"time_modulo_frequency": "10m"},
            "TILE_F1800_M600_B600_53A8F52711C0A5FB7DCB256CC1435E96398AB5ED",
            "sum_e5c2e1cab0c5934b3dd8a1e1db5e4650081a224b",
        ),
        (
            {"blind_spot": "20m"},
            "TILE_F1800_M300_B1200_CD464F2279CC9EBCD5B34AEB46A5DBF48776374B",
            "sum_c46e99bd6792f59467ad18c61a32dd286909180d",
        ),
    ],
)
def test_tile_table_id__agg_parameters(
    snowflake_event_table, aggregate_kwargs, overrides, expected_tile_id, expected_agg_id
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
        snowflake_event_table, aggregate_kwargs
    )
    assert (tile_id, agg_id) == (expected_tile_id, expected_agg_id)


@pytest.mark.parametrize(
    "groupby_kwargs,expected_tile_id, expected_agg_id",
    [
        (
            {"by_keys": "cust_id"},
            "TILE_F1800_M300_B600_B5CAF33CCFEDA76C257EC2CB7F66C4AD22009B0F",
            "sum_aed233b0e8a6e1c1e0d5427b126b03c949609481",
        ),
        # Single groupby key specified as a list should give the same result
        (
            {"by_keys": ["cust_id"]},
            "TILE_F1800_M300_B600_B5CAF33CCFEDA76C257EC2CB7F66C4AD22009B0F",
            "sum_aed233b0e8a6e1c1e0d5427b126b03c949609481",
        ),
        # Changing the by_keys changes the tile ID
        (
            {"by_keys": "col_text"},
            "TILE_F1800_M300_B600_09DB912713A7048A99527D8F90A96C1742B82F5E",
            "sum_3100fb55213ecb67d4c3a6c4c48b51fbc6f4d97c",
        ),
        # Changing the category changes the tile ID
        (
            {"by_keys": "col_text", "category": "col_int"},
            "TILE_F1800_M300_B600_5C355EF19FB7E4FE215A4B03282CC32B08378767",
            "sum_9c8476f50e4ad24dad295d6ed08801d41e0a1ddd",
        ),
    ],
)
def test_tile_table_id__groupby_parameters(
    snowflake_event_table, aggregate_kwargs, groupby_kwargs, expected_tile_id, expected_agg_id
):
    """Test tile table IDs are expected given different groupby() parameters"""
    tile_id, agg_id = run_groupby_and_get_tile_table_identifier(
        snowflake_event_table, aggregate_kwargs, groupby_kwargs
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
        "TILE_F1800_M300_B600_B5CAF33CCFEDA76C257EC2CB7F66C4AD22009B0F",
        "sum_7cd1383596f323a16611e5922ad909368829f7c0",
    )

    # Note that this is different from above
    kwargs = copy.deepcopy(aggregate_kwargs)
    kwargs["value_column"] = "value_100"
    tile_id, agg_id = run_groupby_and_get_tile_table_identifier(
        snowflake_event_view_with_entity, kwargs, create_entity=False
    )
    assert (tile_id, agg_id) == (
        "TILE_F1800_M300_B600_B5CAF33CCFEDA76C257EC2CB7F66C4AD22009B0F",
        "sum_51686dcbef617f0f3102702d0b23db80b2d836df",
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
