"""
Tests for tile related SQL generation using high level API fixtures
"""

from dataclasses import asdict
from datetime import datetime

import pytest
from bson import ObjectId

from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import PartitionColumnFilter, PartitionColumnFilters
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.sql.interpreter.tile import JoinKeysLineage
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.fixture
def feature_with_lag(snowflake_event_view_with_entity, feature_group_feature_job_setting):
    """
    Fixture for a feature with lag
    """
    view = snowflake_event_view_with_entity
    view["lag_col"] = view["col_float"].lag("cust_id")
    feature = view.groupby("cust_id").aggregate_over(
        value_column="lag_col",
        method="sum",
        windows=["30m"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["my_feature"],
    )["my_feature"]
    return feature


@pytest.fixture
def complex_feature_but_push_down_eligible(
    non_time_based_features,
    snowflake_event_view_with_entity,
    snowflake_scd_view_with_entity,
    feature_group_feature_job_setting,
):
    """
    Fixture for a complex feature that can still benefit from pushing down the date filter
    """
    joined_view = snowflake_event_view_with_entity.join(
        snowflake_scd_view_with_entity,
        on="cust_id",
        rsuffix="_scd",
    )
    joined_view = joined_view.add_feature("added_feature", non_time_based_features[0], "cust_id")
    joined_view["added_feature"] = joined_view["added_feature"] + joined_view["col_float_scd"]
    feature = joined_view.groupby("cust_id").aggregate_over(
        value_column="added_feature",
        method="sum",
        windows=["30m"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["my_feature"],
    )["my_feature"]
    return feature


def get_tile_sql_used_in_scheduled_tasks(feature):
    """
    Helper function to get the tile SQL used in scheduled tasks
    """
    tile_specs = ExtendedFeatureModel(**feature.model_dump(by_alias=True)).tile_specs
    assert len(tile_specs) == 1
    return tile_specs[0].tile_compute_query.get_combined_query_expr().sql(pretty=True)


def test_scheduled_tile_sql__can_push_down_date_filter(float_feature, update_fixtures):
    """
    Test date filters are applied for simple features
    """
    tile_sql = get_tile_sql_used_in_scheduled_tasks(float_feature)
    assert_equal_with_expected_fixture(
        tile_sql, "tests/fixtures/expected_tile_sql_feature_without_lag.sql", update_fixtures
    )


def test_scheduled_tile_sql__cannot_push_down_date_filter(feature_with_lag, update_fixtures):
    """
    Test date filters are not applied if feature has lag
    """
    tile_sql = get_tile_sql_used_in_scheduled_tasks(feature_with_lag)
    assert_equal_with_expected_fixture(
        tile_sql, "tests/fixtures/expected_tile_sql_feature_with_lag.sql", update_fixtures
    )


def test_scheduled_tile_sql__complex(complex_feature_but_push_down_eligible, update_fixtures):
    """
    Test date filters are applied even for a complex feature as long as the feature does not contain
    any lag operations
    """
    tile_sql = get_tile_sql_used_in_scheduled_tasks(complex_feature_but_push_down_eligible)
    assert_equal_with_expected_fixture(
        tile_sql,
        "tests/fixtures/expected_tile_sql_complex_feature_push_down_eligible.sql",
        update_fixtures,
    )


def test_join_keys_lineage():
    """
    Test JoinKeysLineage
    """
    table_1 = ObjectId()
    table_2 = ObjectId()
    table_3 = ObjectId()

    join_keys_lineage = JoinKeysLineage()
    assert join_keys_lineage.get_other_columns(table_1, "col_1") == []

    join_keys_lineage.add_join_key(table_1, "col_1", table_2, "col_2")
    join_keys_lineage.add_join_key(table_2, "col_2", table_3, "col_3")
    assert join_keys_lineage.get_other_columns(table_1, "col_1") == [
        (table_2, "col_2"),
        (table_3, "col_3"),
    ]


def test_on_demand_tile_sql_event_timestamp_schema(
    float_feature_with_event_timestamp_schema, source_info, update_fixtures
):
    """
    Test on demand tile sql for event table with event timestamp schema
    """
    query_graph = float_feature_with_event_timestamp_schema.cached_model.graph
    interpreter = GraphInterpreter(query_graph, source_info)
    groupby_node = query_graph.get_node_by_name("groupby_1")
    tile_gen_sqls = interpreter.construct_tile_gen_sql(groupby_node, is_on_demand=True)
    assert len(tile_gen_sqls) == 1

    info = tile_gen_sqls[0]
    info_dict = asdict(info)
    info_dict.pop("tile_compute_spec")
    assert_equal_with_expected_fixture(
        tile_gen_sqls[0].sql,
        "tests/fixtures/expected_tile_sql_on_demand_event_timestamp_schema.sql",
        update_fixtures,
    )


def test_on_demand_tile_sql_partition_column_filters(
    float_feature_with_partition_column,
    snowflake_event_table_with_partition_column,
    source_info,
    update_fixtures,
):
    """
    Test on demand tile sql for feature with partition column filters
    """
    query_graph = float_feature_with_partition_column.cached_model.graph
    interpreter = GraphInterpreter(query_graph, source_info)
    groupby_node = query_graph.get_node_by_name("groupby_1")
    partition_column_filters = PartitionColumnFilters(
        mapping={
            snowflake_event_table_with_partition_column.id: PartitionColumnFilter(
                from_timestamp=make_literal_value(
                    datetime(2023, 1, 1, 0, 0, 0), cast_as_timestamp=True
                ),
                to_timestamp=make_literal_value(
                    datetime(2023, 6, 1, 0, 0, 0), cast_as_timestamp=True
                ),
            )
        }
    )
    tile_gen_sqls = interpreter.construct_tile_gen_sql(
        groupby_node,
        is_on_demand=True,
        partition_column_filters=partition_column_filters,
    )
    assert len(tile_gen_sqls) == 1

    info = tile_gen_sqls[0]
    info_dict = asdict(info)
    info_dict.pop("tile_compute_spec")
    tile_sql = tile_gen_sqls[0].sql
    assert (
        "\"col_text\" >= TO_CHAR(CAST('2023-01-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d %H:%M:%S')"
        in tile_sql
    )
    assert (
        "\"col_text\" <= TO_CHAR(CAST('2023-06-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d %H:%M:%S')"
        in tile_sql
    )
    assert_equal_with_expected_fixture(
        tile_gen_sqls[0].sql,
        "tests/fixtures/expected_tile_sql_on_demand_partition_column_filters.sql",
        update_fixtures,
    )


def test_scheduled_tile_sql_partition_column_filters(
    float_feature_with_partition_column,
    snowflake_event_table_with_partition_column,
    source_info,
    update_fixtures,
):
    """
    Test scheduled tile sql for feature with partition column filters
    """
    query_graph = float_feature_with_partition_column.cached_model.graph
    interpreter = GraphInterpreter(query_graph, source_info)
    groupby_node = query_graph.get_node_by_name("groupby_1")
    tile_gen_sqls = interpreter.construct_tile_gen_sql(groupby_node, is_on_demand=False)
    assert len(tile_gen_sqls) == 1

    info = tile_gen_sqls[0]
    info_dict = asdict(info)
    info_dict.pop("tile_compute_spec")
    tile_sql = tile_gen_sqls[0].sql
    assert_equal_with_expected_fixture(
        tile_sql,
        "tests/fixtures/expected_tile_sql_scheduled_partition_column_filters.sql",
        update_fixtures,
    )
