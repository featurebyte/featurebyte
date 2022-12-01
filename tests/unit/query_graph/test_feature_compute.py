"""
Tests for featurebyte.query_graph.feature_compute
"""
import copy
import textwrap
from dataclasses import asdict

import pytest
from sqlglot import select

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME
from featurebyte.query_graph.sql.feature_compute import (
    FeatureExecutionPlanner,
    NonTimeAwareRequestTablePlan,
    RequestTablePlan,
)
from featurebyte.query_graph.sql.specs import (
    FeatureSpec,
    ItemAggregationSpec,
    PointInTimeAggregationSpec,
)


@pytest.fixture(name="agg_spec_template")
def agg_spec_template_fixture():
    """Fixture for an AggregationSpec"""
    agg_spec = PointInTimeAggregationSpec(
        window=86400,
        frequency=3600,
        blind_spot=120,
        time_modulo_frequency=1800,
        tile_table_id="some_tile_id",
        aggregation_id="some_agg_id",
        keys=["CUST_ID"],
        serving_names=["CID"],
        value_by=None,
        merge_expr="SUM(value)",
        feature_name="Amount (1d sum)",
    )
    return agg_spec


@pytest.fixture(name="agg_spec_sum_1d")
def agg_spec_sum_1d_fixture(agg_spec_template):
    """Fixture for an AggregationSpec with 1 day sum"""
    agg_spec = copy.deepcopy(agg_spec_template)
    agg_spec.tile_table_id = "sum_1d_tile_id"
    return agg_spec


@pytest.fixture(name="agg_spec_max_1d")
def agg_spec_max_1d_fixture(agg_spec_template):
    """Fixture for an AggregationSpec with 1 day max"""
    agg_spec = copy.deepcopy(agg_spec_template)
    agg_spec.merge_expr = "MAX(value)"
    agg_spec.tile_table_id = "max_1d_tile_id"
    return agg_spec


@pytest.fixture(name="agg_spec_max_2h")
def agg_spec_max_2h_fixture(agg_spec_template):
    """Fixture for an AggregationSpec with 2 hour max"""
    agg_spec = copy.deepcopy(agg_spec_template)
    agg_spec.window = 7200
    agg_spec.merge_expr = "MAX(value)"
    agg_spec.tile_table_id = "max_2h_tile_id"
    return agg_spec


@pytest.fixture(name="item_agg_spec")
def item_agg_spec_fixture():
    agg_spec = ItemAggregationSpec(
        keys=["order_id"],
        serving_names=["OID"],
        feature_name="Order Size",
        agg_expr=select("*").from_("tab"),
    )
    return agg_spec


def assert_sql_equal(sql, expected):
    """Helper function to check that SQL code matches with expected"""
    sql = textwrap.dedent(sql).strip()
    expected = textwrap.dedent(expected).strip()
    assert sql == expected


def test_request_table_plan__share_expanded_table(agg_spec_sum_1d, agg_spec_max_1d):
    """Test that two compatible AggregationSpec shares the same expanded request table"""
    plan = RequestTablePlan(source_type=SourceType.SNOWFLAKE)
    plan.add_aggregation_spec(agg_spec_sum_1d)
    plan.add_aggregation_spec(agg_spec_max_1d)

    assert (
        plan.get_expanded_request_table_name(agg_spec_sum_1d)
        == "REQUEST_TABLE_W86400_F3600_BS120_M1800_CID"
    )
    assert (
        plan.get_expanded_request_table_name(agg_spec_max_1d)
        == "REQUEST_TABLE_W86400_F3600_BS120_M1800_CID"
    )

    ctes = plan.construct_request_tile_indices_ctes(request_table_name=REQUEST_TABLE_NAME)
    assert len(ctes) == 1

    cte = ctes[0]
    assert cte[0] == '"REQUEST_TABLE_W86400_F3600_BS120_M1800_CID"'
    expected_sql = """
    SELECT
      POINT_IN_TIME,
      "CID",
      FLOOR((
        DATE_PART(EPOCH_SECOND, POINT_IN_TIME) - 1800
      ) / 3600) AS "__FB_LAST_TILE_INDEX",
      FLOOR((
        DATE_PART(EPOCH_SECOND, POINT_IN_TIME) - 1800
      ) / 3600) - 24 AS "__FB_FIRST_TILE_INDEX"
    FROM (
      SELECT DISTINCT
        POINT_IN_TIME,
        "CID"
      FROM REQUEST_TABLE
    )
    """
    assert_sql_equal(cte[1].sql(pretty=True), expected_sql)


def test_request_table_plan__no_sharing(agg_spec_max_2h, agg_spec_max_1d):
    """Test that two incompatible AggregationSpec does not share expanded request tables"""
    plan = RequestTablePlan(source_type=SourceType.SNOWFLAKE)
    plan.add_aggregation_spec(agg_spec_max_2h)
    plan.add_aggregation_spec(agg_spec_max_1d)

    assert (
        plan.get_expanded_request_table_name(agg_spec_max_2h)
        == "REQUEST_TABLE_W7200_F3600_BS120_M1800_CID"
    )
    assert (
        plan.get_expanded_request_table_name(agg_spec_max_1d)
        == "REQUEST_TABLE_W86400_F3600_BS120_M1800_CID"
    )

    ctes = plan.construct_request_tile_indices_ctes(request_table_name=REQUEST_TABLE_NAME)
    assert len(ctes) == 2

    # check expanded table for 2h
    name, sql = ctes[0]
    assert name == '"REQUEST_TABLE_W7200_F3600_BS120_M1800_CID"'
    expected_sql = """
    SELECT
      POINT_IN_TIME,
      "CID",
      FLOOR((
        DATE_PART(EPOCH_SECOND, POINT_IN_TIME) - 1800
      ) / 3600) AS "__FB_LAST_TILE_INDEX",
      FLOOR((
        DATE_PART(EPOCH_SECOND, POINT_IN_TIME) - 1800
      ) / 3600) - 2 AS "__FB_FIRST_TILE_INDEX"
    FROM (
      SELECT DISTINCT
        POINT_IN_TIME,
        "CID"
      FROM REQUEST_TABLE
    )
    """
    assert_sql_equal(sql.sql(pretty=True), expected_sql)

    # check expanded table for 1d
    name, sql = ctes[1]
    assert name == '"REQUEST_TABLE_W86400_F3600_BS120_M1800_CID"'
    expected_sql = """
    SELECT
      POINT_IN_TIME,
      "CID",
      FLOOR((
        DATE_PART(EPOCH_SECOND, POINT_IN_TIME) - 1800
      ) / 3600) AS "__FB_LAST_TILE_INDEX",
      FLOOR((
        DATE_PART(EPOCH_SECOND, POINT_IN_TIME) - 1800
      ) / 3600) - 24 AS "__FB_FIRST_TILE_INDEX"
    FROM (
      SELECT DISTINCT
        POINT_IN_TIME,
        "CID"
      FROM REQUEST_TABLE
    )
    """
    assert_sql_equal(sql.sql(pretty=True), expected_sql)


def test_non_time_aware_request_table_plan(item_agg_spec):
    """
    Test NonTimeAwareRequestTablePlan
    """
    plan = NonTimeAwareRequestTablePlan()
    plan.add_aggregation_spec(item_agg_spec)
    assert plan.get_request_table_name(item_agg_spec) == "REQUEST_TABLE_OID"
    ctes = plan.construct_request_table_ctes(REQUEST_TABLE_NAME)
    assert len(ctes) == 1
    name, sql = ctes[0]
    expected_sql = """
    SELECT DISTINCT
      "OID"
    FROM REQUEST_TABLE
    """
    assert_sql_equal(sql.sql(pretty=True), expected_sql)


def test_feature_execution_planner(query_graph_with_groupby, groupby_node_aggregation_id):
    """Test FeatureExecutionPlanner generates the correct plan from groupby node"""
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    planner = FeatureExecutionPlanner(query_graph_with_groupby, source_type=SourceType.SNOWFLAKE)
    plan = planner.generate_plan([groupby_node])
    assert list(plan.point_in_time_aggregation_spec_set.get_grouped_aggregation_specs()) == [
        [
            PointInTimeAggregationSpec(
                window=7200,
                frequency=3600,
                blind_spot=900,
                time_modulo_frequency=1800,
                tile_table_id="fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d",
                aggregation_id=f"avg_{groupby_node_aggregation_id}",
                keys=["cust_id"],
                serving_names=["CUSTOMER_ID"],
                value_by=None,
                merge_expr=(
                    f"SUM(sum_value_avg_{groupby_node_aggregation_id}) / "
                    f"SUM(count_value_avg_{groupby_node_aggregation_id})"
                ),
                feature_name="a_2h_average",
            )
        ],
        [
            PointInTimeAggregationSpec(
                window=172800,
                frequency=3600,
                blind_spot=900,
                time_modulo_frequency=1800,
                tile_table_id="fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d",
                aggregation_id=f"avg_{groupby_node_aggregation_id}",
                keys=["cust_id"],
                serving_names=["CUSTOMER_ID"],
                value_by=None,
                merge_expr=(
                    f"SUM(sum_value_avg_{groupby_node_aggregation_id}) / "
                    f"SUM(count_value_avg_{groupby_node_aggregation_id})"
                ),
                feature_name="a_48h_average",
            )
        ],
    ]
    assert plan.feature_specs == {
        "a_2h_average": FeatureSpec(
            feature_name="a_2h_average",
            feature_expr=f'"agg_w7200_avg_{groupby_node_aggregation_id}"',
        ),
        "a_48h_average": FeatureSpec(
            feature_name="a_48h_average",
            feature_expr=f'"agg_w172800_avg_{groupby_node_aggregation_id}"',
        ),
    }


def test_feature_execution_planner__serving_names_mapping(
    query_graph_with_groupby, groupby_node_aggregation_id
):
    """Test FeatureExecutionPlanner with serving names mapping provided"""
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    mapping = {"CUSTOMER_ID": "NEW_CUST_ID"}
    planner = FeatureExecutionPlanner(
        query_graph_with_groupby, serving_names_mapping=mapping, source_type=SourceType.SNOWFLAKE
    )
    plan = planner.generate_plan([groupby_node])
    assert list(plan.point_in_time_aggregation_spec_set.get_grouped_aggregation_specs()) == [
        [
            PointInTimeAggregationSpec(
                window=7200,
                frequency=3600,
                blind_spot=900,
                time_modulo_frequency=1800,
                tile_table_id="fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d",
                aggregation_id=f"avg_{groupby_node_aggregation_id}",
                keys=["cust_id"],
                serving_names=["NEW_CUST_ID"],
                value_by=None,
                merge_expr=(
                    f"SUM(sum_value_avg_{groupby_node_aggregation_id}) / "
                    f"SUM(count_value_avg_{groupby_node_aggregation_id})"
                ),
                feature_name="a_2h_average",
            )
        ],
        [
            PointInTimeAggregationSpec(
                window=172800,
                frequency=3600,
                blind_spot=900,
                time_modulo_frequency=1800,
                tile_table_id="fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d",
                aggregation_id=f"avg_{groupby_node_aggregation_id}",
                keys=["cust_id"],
                serving_names=["NEW_CUST_ID"],
                value_by=None,
                merge_expr=(
                    f"SUM(sum_value_avg_{groupby_node_aggregation_id}) / "
                    f"SUM(count_value_avg_{groupby_node_aggregation_id})"
                ),
                feature_name="a_48h_average",
            )
        ],
    ]
    assert plan.feature_specs == {
        "a_2h_average": FeatureSpec(
            feature_name="a_2h_average",
            feature_expr=f'"agg_w7200_avg_{groupby_node_aggregation_id}"',
        ),
        "a_48h_average": FeatureSpec(
            feature_name="a_48h_average",
            feature_expr=f'"agg_w172800_avg_{groupby_node_aggregation_id}"',
        ),
    }


def test_feature_execution_planner__item_aggregation(global_graph, order_size_feature_group_node):
    """
    Test FeatureExecutionPlanner on an ItemGroupby node
    """
    mapping = {"order_id": "NEW_ORDER_ID"}
    planner = FeatureExecutionPlanner(
        global_graph, serving_names_mapping=mapping, source_type=SourceType.SNOWFLAKE
    )
    plan = planner.generate_plan([order_size_feature_group_node])

    # Check item aggregation specs
    item_aggregation_specs = plan.item_aggregation_specs
    assert len(item_aggregation_specs) == 1
    spec_dict = asdict(item_aggregation_specs[0])
    agg_expr = spec_dict.pop("agg_expr")
    expected_agg_expr = """
    SELECT
      "order_id",
      COUNT(*) AS "order_size"
    FROM (
      SELECT
        "order_id" AS "order_id",
        "item_id" AS "item_id",
        "item_name" AS "item_name",
        "item_type" AS "item_type"
      FROM "db"."public"."item_table"
    )
    GROUP BY
      "order_id"
    """
    assert_sql_equal(agg_expr.sql(pretty=True), expected_agg_expr)
    assert spec_dict == {
        "keys": ["order_id"],
        "serving_names": ["NEW_ORDER_ID"],
        "feature_name": "order_size",
    }

    # Check feature specs
    assert plan.feature_specs == {
        "order_size": FeatureSpec(feature_name="order_size", feature_expr='"order_size"')
    }
