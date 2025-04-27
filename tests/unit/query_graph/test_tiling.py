"""
Tests for featurebyte.query_graph.tiling
"""

import copy

import pytest

from featurebyte import FeatureJobSetting
from featurebyte.api.entity import Entity
from featurebyte.api.event_table import EventTable
from featurebyte.enum import DBVarType
from featurebyte.query_graph.graph import GlobalQueryGraph
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
    "agg_func,parent_dtype,expected_tile_specs,expected_merge_expr",
    [
        (
            AggFunc.SUM,
            None,
            [
                make_expected_tile_spec(
                    tile_expr='SUM("a_column")', tile_column_name="value_1234beef"
                )
            ],
            "SUM(value_1234beef)",
        ),
        (
            AggFunc.AVG,
            None,
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
            AggFunc.AVG,
            DBVarType.FLOAT,  # adding a non-array parent type will use the normal average aggregator
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
            DBVarType.INT,
            [
                make_expected_tile_spec(
                    tile_expr='MIN("a_column")',
                    tile_column_name="value_1234beef",
                    tile_column_type="FLOAT",
                )
            ],
            "MIN(value_1234beef)",
        ),
        (
            AggFunc.MAX,
            DBVarType.TIMESTAMP,
            [
                make_expected_tile_spec(
                    tile_expr='MAX("a_column")',
                    tile_column_name="value_1234beef",
                    tile_column_type="TIMESTAMP_NTZ",
                )
            ],
            "MAX(value_1234beef)",
        ),
        (
            AggFunc.MAX,
            DBVarType.FLOAT,  # adding a non-array parent type will use the normal max aggregator
            [
                make_expected_tile_spec(
                    tile_expr='MAX("a_column")', tile_column_name="value_1234beef"
                )
            ],
            "MAX(value_1234beef)",
        ),
        (
            AggFunc.COUNT,
            None,
            [make_expected_tile_spec(tile_expr="COUNT(*)", tile_column_name="value_1234beef")],
            "SUM(value_1234beef)",
        ),
        (
            AggFunc.NA_COUNT,
            None,
            [
                make_expected_tile_spec(
                    tile_expr='SUM(CAST("a_column" IS NULL AS INT))',
                    tile_column_name="value_1234beef",
                )
            ],
            "SUM(value_1234beef)",
        ),
        (
            AggFunc.STD,
            None,
            [
                make_expected_tile_spec(
                    tile_expr='SUM(CAST("a_column" AS DOUBLE) * CAST("a_column" AS DOUBLE))',
                    tile_column_name="sum_value_squared_1234beef",
                ),
                make_expected_tile_spec(
                    tile_expr='SUM(CAST("a_column" AS DOUBLE))',
                    tile_column_name="sum_value_1234beef",
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
            DBVarType.VARCHAR,
            [
                make_expected_tile_spec(
                    tile_expr='FIRST_VALUE("a_column")',
                    tile_column_name="value_1234beef",
                    tile_column_type="VARCHAR",
                )
            ],
            "FIRST_VALUE(value_1234beef)",
        ),
        (
            AggFunc.MAX,
            DBVarType.ARRAY,
            [
                make_expected_tile_spec(
                    tile_expr='VECTOR_AGGREGATE_MAX("a_column")',
                    tile_column_name="value_1234beef",
                    tile_column_type="ARRAY",
                )
            ],
            "VECTOR_AGGREGATE_MAX(value_1234beef)",
        ),
        (
            AggFunc.AVG,
            DBVarType.ARRAY,
            [
                make_expected_tile_spec(
                    tile_expr='VECTOR_AGGREGATE_SUM("a_column")',
                    tile_column_name="sum_list_value_1234beef",
                    tile_column_type="ARRAY",
                ),
                make_expected_tile_spec(
                    tile_expr="CAST(COUNT(*) AS DOUBLE)",
                    tile_column_name="count_value_1234beef",
                    tile_column_type="FLOAT",
                ),
            ],
            "VECTOR_AGGREGATE_AVG(sum_list_value_1234beef, count_value_1234beef)",
        ),
    ],
)
def test_tiling_aggregators(
    agg_func, parent_dtype, expected_tile_specs, expected_merge_expr, adapter
):
    """Test tiling aggregators produces expected expressions"""
    agg_id = "1234beef"
    agg = get_aggregator(agg_func, adapter=adapter, parent_dtype=parent_dtype)
    input_column = InputColumn(name="a_column", dtype=parent_dtype)
    tile_specs = agg.tile(input_column, agg_id)
    merge_expr = agg.merge(agg_id).sql()
    assert [t.tile_expr.sql() for t in tile_specs] == [t["tile_expr"] for t in expected_tile_specs]
    assert [t.tile_column_name for t in tile_specs] == [
        t["tile_column_name"] for t in expected_tile_specs
    ]
    assert [t.tile_column_type for t in tile_specs] == [
        t["tile_column_type"] for t in expected_tile_specs
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
        feature_job_setting=FeatureJobSetting(
            blind_spot="10m",
            period="30m",
            offset="5m",
        ),
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
    pruned_graph, node_name_map = GlobalQueryGraph().prune(target_node=groupby_node)
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
            "TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295",
            "sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
        ),
        # Features with different windows can share the same tile table
        (
            {"windows": ["2d"], "feature_names": ["sum_2d"]},
            "TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295",
            "sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
        ),
        (
            {"method": "max"},
            "TILE_MAX_8386BC5866B03E1C3BC2DE69717E050B965EDD31",
            "max_8386bc5866b03e1c3bc2de69717e050b965edd31",
        ),
        (
            {"value_column": "col_int"},
            "TILE_SUM_976E8AEFA273A14A2998F4148BB19377387408FA",
            "sum_976e8aefa273a14a2998f4148bb19377387408fa",
        ),
        (
            {"period": "10m"},
            "TILE_SUM_2458AD1E0BBDC71E25E4F6AD60C405A66DB1C2F0",
            "sum_2458ad1e0bbdc71e25e4f6ad60c405a66db1c2f0",
        ),
        (
            {"offset": "10m"},
            "TILE_SUM_8CA0F4BAA7250E0CB73A9E297B136F3F02DF36A4",
            "sum_8ca0f4baa7250e0cb73a9e297b136f3f02df36a4",
        ),
        (
            {"blind_spot": "20m"},
            "TILE_SUM_522AC2033D65C09A17CE29DF387EA7F54B3138FD",
            "sum_522ac2033d65c09a17ce29df387ea7f54b3138fd",
        ),
    ],
)
def test_tile_table_id__agg_parameters(
    snowflake_event_table, aggregate_kwargs, overrides, expected_tile_id, expected_agg_id
):
    """Test tile table IDs are expected given different aggregate() parameters"""
    feature_job_setting_params = {"period", "blind_spot", "offset"}
    existing_fjs = aggregate_kwargs["feature_job_setting"]
    existing_fjs_dict = existing_fjs.json_dict()
    for key in overrides:
        if key in feature_job_setting_params:
            existing_fjs_dict[key] = overrides[key]
    aggregate_kwargs["feature_job_setting"] = FeatureJobSetting(**existing_fjs_dict)
    aggregate_kwargs.update({
        key: val for key, val in overrides.items() if key not in feature_job_setting_params
    })
    tile_id, agg_id = run_groupby_and_get_tile_table_identifier(
        snowflake_event_table, aggregate_kwargs
    )
    assert (tile_id, agg_id) == (expected_tile_id, expected_agg_id)


@pytest.mark.parametrize(
    "groupby_kwargs,expected_tile_id, expected_agg_id",
    [
        (
            {"by_keys": "cust_id"},
            "TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295",
            "sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
        ),
        # Single groupby key specified as a list should give the same result
        (
            {"by_keys": ["cust_id"]},
            "TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295",
            "sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
        ),
        # Changing the by_keys changes the tile ID
        (
            {"by_keys": "col_text"},
            "TILE_SUM_3A8EA84A0B340D67855C98B583FD4542185B78A8",
            "sum_3a8ea84a0b340d67855c98b583fd4542185b78a8",
        ),
        # Changing the category changes the tile ID
        (
            {"by_keys": "col_text", "category": "col_int"},
            "TILE_SUM_D831E1D1BA75E56E80304FEE68CB700E80B1898A",
            "sum_d831e1d1ba75e56e80304fee68cb700e80b1898a",
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
        "TILE_SUM_97E3469A34D3677123A6B2790BC4EF1E884103D6",
        "sum_97e3469a34d3677123a6b2790bc4ef1e884103d6",
    )

    # Note that this is different from above
    kwargs = copy.deepcopy(aggregate_kwargs)
    kwargs["value_column"] = "value_100"
    tile_id, agg_id = run_groupby_and_get_tile_table_identifier(
        snowflake_event_view_with_entity, kwargs, create_entity=False
    )
    assert (tile_id, agg_id) == (
        "TILE_SUM_A18A81569B0A6165960E6567ECDB26B882031FC3",
        "sum_a18a81569b0a6165960e6567ecdb26b882031fc3",
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
