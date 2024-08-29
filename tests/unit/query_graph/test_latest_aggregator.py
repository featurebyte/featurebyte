"""
Unit tests for featurebyte.query_graph.sql.aggregator.latest.LatestAggregator

"""

import textwrap

import pytest
from sqlglot.expressions import select

from featurebyte import SourceType
from featurebyte.query_graph.sql.aggregator.latest import LatestAggregator
from featurebyte.query_graph.sql.specs import TileBasedAggregationSpec
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.fixture
def agg_specs_no_window(global_graph, latest_value_without_window_feature_node, adapter):
    """
    Fixture of TileAggregationSpec without window
    """
    parent_nodes = global_graph.get_input_node_names(latest_value_without_window_feature_node)
    assert len(parent_nodes) == 1
    groupby_node = global_graph.get_node_by_name(parent_nodes[0])
    return TileBasedAggregationSpec.from_groupby_query_node(
        global_graph,
        groupby_node,
        adapter=adapter,
        agg_result_name_include_serving_names=True,
    )


@pytest.fixture
def agg_specs_offset(global_graph, latest_value_offset_without_window_feature_node, adapter):
    """
    Fixture of TileAggregationSpec with unbounded window and offset
    """
    parent_nodes = global_graph.get_input_node_names(
        latest_value_offset_without_window_feature_node
    )
    assert len(parent_nodes) == 1
    groupby_node = global_graph.get_node_by_name(parent_nodes[0])
    return TileBasedAggregationSpec.from_groupby_query_node(
        global_graph,
        groupby_node,
        adapter=adapter,
        agg_result_name_include_serving_names=True,
    )


def create_latest_aggregator(agg_specs, source_info, **kwargs):
    """
    Helper function to create a LatestAggregator
    """
    aggregator = LatestAggregator(source_info=source_info, **kwargs)
    for spec in agg_specs:
        aggregator.update(spec)
    return aggregator


def test_get_required_serving_names(agg_specs_no_window, source_info):
    """
    Test get_required_serving_names method
    """
    aggregator = LatestAggregator(source_info=source_info)
    for spec in agg_specs_no_window:
        aggregator.update(spec)
    assert aggregator.get_required_serving_names() == {"CUSTOMER_ID", "BUSINESS_ID"}


def test_latest_aggregator(agg_specs_no_window, source_info):
    """
    Test LatestAggregator update_aggregation_table_expr
    """
    aggregator = create_latest_aggregator(agg_specs_no_window, source_info=source_info)
    assert len(list(aggregator.specs_set.get_grouped_aggregation_specs())) == 1

    result = aggregator.update_aggregation_table_expr(
        table_expr=select("a", "b", "c").from_("my_table"),
        point_in_time_column="POINT_IN_TIME",
        current_columns=["a", "b", "c"],
        current_query_index=0,
    )

    expected = textwrap.dedent(
        """
        SELECT
          REQ."a" AS "a",
          REQ."b" AS "b",
          REQ."c" AS "c",
          REQ."_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_b4a6546e024f3a059bd67f454028e56c5a37826e" AS "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_b4a6546e024f3a059bd67f454028e56c5a37826e"
        FROM (
          SELECT
            L."a" AS "a",
            L."b" AS "b",
            L."c" AS "c",
            R.value_latest_b4a6546e024f3a059bd67f454028e56c5a37826e AS "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_b4a6546e024f3a059bd67f454028e56c5a37826e"
          FROM (
            SELECT
              "__FB_KEY_COL_0",
              "__FB_KEY_COL_1",
              "__FB_LAST_TS",
              "a",
              "b",
              "c"
            FROM (
              SELECT
                "__FB_KEY_COL_0",
                "__FB_KEY_COL_1",
                LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0", "__FB_KEY_COL_1" ORDER BY "__FB_TS_COL", "__FB_TS_TIE_BREAKER_COL" NULLS LAST) AS "__FB_LAST_TS",
                "a",
                "b",
                "c",
                "__FB_EFFECTIVE_TS_COL"
              FROM (
                SELECT
                  FLOOR((
                    DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
                  ) / 3600) AS "__FB_TS_COL",
                  "CUSTOMER_ID" AS "__FB_KEY_COL_0",
                  "BUSINESS_ID" AS "__FB_KEY_COL_1",
                  NULL AS "__FB_EFFECTIVE_TS_COL",
                  0 AS "__FB_TS_TIE_BREAKER_COL",
                  "a" AS "a",
                  "b" AS "b",
                  "c" AS "c"
                FROM (
                  SELECT
                    a,
                    b,
                    c
                  FROM my_table
                )
                UNION ALL
                SELECT
                  "INDEX" AS "__FB_TS_COL",
                  "cust_id" AS "__FB_KEY_COL_0",
                  "biz_id" AS "__FB_KEY_COL_1",
                  "INDEX" AS "__FB_EFFECTIVE_TS_COL",
                  1 AS "__FB_TS_TIE_BREAKER_COL",
                  NULL AS "a",
                  NULL AS "b",
                  NULL AS "c"
                FROM TILE_F3600_M1800_B900_AF1FD0AEE34EC80A96A6D5A486CE40F5A2267B4E
              )
            )
            WHERE
              "__FB_EFFECTIVE_TS_COL" IS NULL
          ) AS L
          LEFT JOIN TILE_F3600_M1800_B900_AF1FD0AEE34EC80A96A6D5A486CE40F5A2267B4E AS R
            ON L."__FB_LAST_TS" = R."INDEX"
            AND L."__FB_KEY_COL_0" = R."cust_id"
            AND L."__FB_KEY_COL_1" = R."biz_id"
        ) AS REQ
        """
    ).strip()
    assert result.updated_table_expr.sql(pretty=True) == expected

    assert result.column_names == [
        "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_b4a6546e024f3a059bd67f454028e56c5a37826e"
    ]
    assert result.updated_index == 0


def test_latest_aggregator__no_specs(source_info):
    """
    Test calling update_aggregation_table_expr when there are no unbounded windows (no specs)
    """
    aggregator = create_latest_aggregator([], source_info=source_info)
    assert len(list(aggregator.specs_set.get_grouped_aggregation_specs())) == 0

    result = aggregator.update_aggregation_table_expr(
        table_expr=select("a", "b", "c").from_("my_table"),
        point_in_time_column="POINT_IN_TIME",
        current_columns=["a", "b", "c"],
        current_query_index=0,
    )
    assert result.updated_table_expr.sql() == "SELECT a, b, c FROM my_table"
    assert result.column_names == []
    assert result.updated_index == 0


def test_latest_aggregator__online_retrieval(agg_specs_no_window, source_info, update_fixtures):
    """
    Test feature values should be calculated on demand from the tile table during online retrieval
    """
    aggregator = create_latest_aggregator(
        agg_specs_no_window, source_info=source_info, is_online_serving=True
    )
    assert len(list(aggregator.specs_set.get_grouped_aggregation_specs())) == 1

    result = aggregator.update_aggregation_table_expr(
        table_expr=select("a", "b", "c").from_("my_table"),
        point_in_time_column="POINT_IN_TIME",
        current_columns=["a", "b", "c"],
        current_query_index=0,
    )
    assert_equal_with_expected_fixture(
        result.updated_table_expr.sql(pretty=True),
        "tests/fixtures/aggregator/expected_latest_aggregator_update_online.sql",
        update_fixture=update_fixtures,
    )
    assert result.column_names == [
        "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_b4a6546e024f3a059bd67f454028e56c5a37826e"
    ]
    assert result.updated_index == 0


def test_latest_aggregator_offset(agg_specs_offset, source_info, update_fixtures):
    """
    Test latest aggregation with offset
    """
    aggregator = create_latest_aggregator(
        agg_specs_offset, source_info=source_info, is_online_serving=True
    )
    assert len(list(aggregator.specs_set.get_grouped_aggregation_specs())) == 1

    result = aggregator.update_aggregation_table_expr(
        table_expr=select("a", "b", "c").from_("my_table"),
        point_in_time_column="POINT_IN_TIME",
        current_columns=["a", "b", "c"],
        current_query_index=0,
    )
    assert_equal_with_expected_fixture(
        result.updated_table_expr.sql(pretty=True),
        "tests/fixtures/aggregator/expected_latest_aggregator_update_offset.sql",
        update_fixture=update_fixtures,
    )
    assert result.column_names == [
        "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_b4a6546e024f3a059bd67f454028e56c5a37826e_o172800",
    ]
    assert result.updated_index == 0
