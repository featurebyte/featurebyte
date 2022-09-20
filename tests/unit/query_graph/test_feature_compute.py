"""
Tests for featurebyte.query_graph.feature_compute
"""
import copy
import textwrap

import pytest

from featurebyte.query_graph.feature_common import AggregationSpec, FeatureSpec
from featurebyte.query_graph.feature_compute import (
    FeatureExecutionPlanner,
    SnowflakeRequestTablePlan,
)


@pytest.fixture(name="agg_spec_template")
def agg_spec_template_fixture():
    """Fixture for an AggregationSpec"""
    agg_spec = AggregationSpec(
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


def assert_sql_equal(sql, expected):
    """Helper function to check that SQL code matches with expected"""
    sql = textwrap.dedent(sql).strip()
    expected = textwrap.dedent(expected).strip()
    assert sql == expected


def test_request_table_plan__share_expanded_table(agg_spec_sum_1d, agg_spec_max_1d):
    """Test that two compatible AggregationSpec shares the same expanded request table"""
    plan = SnowflakeRequestTablePlan()
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

    ctes = plan.construct_request_tile_indices_ctes()
    assert len(ctes) == 1

    cte = ctes[0]
    assert cte[0] == '"REQUEST_TABLE_W86400_F3600_BS120_M1800_CID"'
    expected_sql = """
    SELECT
      POINT_IN_TIME,
      "CID",
      DATE_PART(epoch, POINT_IN_TIME) AS __FB_TS,
      FLOOR((__FB_TS - 1800) / 3600) AS __LAST_TILE_INDEX,
      __LAST_TILE_INDEX - 24 AS __FIRST_TILE_INDEX
    FROM (
        SELECT DISTINCT
          POINT_IN_TIME,
          "CID"
        FROM REQUEST_TABLE
    )
    """
    assert_sql_equal(cte[1], expected_sql)


def test_request_table_plan__no_sharing(agg_spec_max_2h, agg_spec_max_1d):
    """Test that two incompatible AggregationSpec does not share expanded request tables"""
    plan = SnowflakeRequestTablePlan()
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

    ctes = plan.construct_request_tile_indices_ctes()
    assert len(ctes) == 2

    # check expanded table for 2h
    name, sql = ctes[0]
    assert name == '"REQUEST_TABLE_W7200_F3600_BS120_M1800_CID"'
    expected_sql = """
    SELECT
      POINT_IN_TIME,
      "CID",
      DATE_PART(epoch, POINT_IN_TIME) AS __FB_TS,
      FLOOR((__FB_TS - 1800) / 3600) AS __LAST_TILE_INDEX,
      __LAST_TILE_INDEX - 2 AS __FIRST_TILE_INDEX
    FROM (
        SELECT DISTINCT
          POINT_IN_TIME,
          "CID"
        FROM REQUEST_TABLE
    )
    """
    assert_sql_equal(sql, expected_sql)

    # check expanded table for 1d
    name, sql = ctes[1]
    assert name == '"REQUEST_TABLE_W86400_F3600_BS120_M1800_CID"'
    expected_sql = """
    SELECT
      POINT_IN_TIME,
      "CID",
      DATE_PART(epoch, POINT_IN_TIME) AS __FB_TS,
      FLOOR((__FB_TS - 1800) / 3600) AS __LAST_TILE_INDEX,
      __LAST_TILE_INDEX - 24 AS __FIRST_TILE_INDEX
    FROM (
        SELECT DISTINCT
          POINT_IN_TIME,
          "CID"
        FROM REQUEST_TABLE
    )
    """
    assert_sql_equal(sql, expected_sql)


def test_feature_execution_planner(query_graph_with_groupby):
    """Test FeatureExecutionPlanner generates the correct plan from groupby node"""
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    planner = FeatureExecutionPlanner(query_graph_with_groupby)
    plan = planner.generate_plan([groupby_node])
    assert list(plan.aggregation_spec_set.get_grouped_aggregation_specs()) == [
        [
            AggregationSpec(
                window=7200,
                frequency=3600,
                blind_spot=900,
                time_modulo_frequency=1800,
                tile_table_id="fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d",
                aggregation_id="avg_53307fe1790a553cf1ca703e44b92619ad86dc8f",
                keys=["cust_id"],
                serving_names=["CUSTOMER_ID"],
                value_by=None,
                merge_expr="SUM(sum_value_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f) / SUM(count_value_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f)",
                feature_name="a_2h_average",
            )
        ],
        [
            AggregationSpec(
                window=172800,
                frequency=3600,
                blind_spot=900,
                time_modulo_frequency=1800,
                tile_table_id="fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d",
                aggregation_id="avg_53307fe1790a553cf1ca703e44b92619ad86dc8f",
                keys=["cust_id"],
                serving_names=["CUSTOMER_ID"],
                value_by=None,
                merge_expr="SUM(sum_value_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f) / SUM(count_value_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f)",
                feature_name="a_48h_average",
            )
        ],
    ]
    assert plan.feature_specs == {
        "a_2h_average": FeatureSpec(
            feature_name="a_2h_average",
            feature_expr='"agg_w7200_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f"',
        ),
        "a_48h_average": FeatureSpec(
            feature_name="a_48h_average",
            feature_expr='"agg_w172800_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f"',
        ),
    }


def test_feature_execution_planner__serving_names_mapping(query_graph_with_groupby):
    """Test FeatureExecutionPlanner with serving names mapping provided"""
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    mapping = {"CUSTOMER_ID": "NEW_CUST_ID"}
    planner = FeatureExecutionPlanner(query_graph_with_groupby, serving_names_mapping=mapping)
    plan = planner.generate_plan([groupby_node])
    assert list(plan.aggregation_spec_set.get_grouped_aggregation_specs()) == [
        [
            AggregationSpec(
                window=7200,
                frequency=3600,
                blind_spot=900,
                time_modulo_frequency=1800,
                tile_table_id="fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d",
                aggregation_id="avg_53307fe1790a553cf1ca703e44b92619ad86dc8f",
                keys=["cust_id"],
                serving_names=["NEW_CUST_ID"],
                value_by=None,
                merge_expr="SUM(sum_value_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f) / SUM(count_value_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f)",
                feature_name="a_2h_average",
            )
        ],
        [
            AggregationSpec(
                window=172800,
                frequency=3600,
                blind_spot=900,
                time_modulo_frequency=1800,
                tile_table_id="fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d",
                aggregation_id="avg_53307fe1790a553cf1ca703e44b92619ad86dc8f",
                keys=["cust_id"],
                serving_names=["NEW_CUST_ID"],
                value_by=None,
                merge_expr="SUM(sum_value_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f) / SUM(count_value_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f)",
                feature_name="a_48h_average",
            )
        ],
    ]
    assert plan.feature_specs == {
        "a_2h_average": FeatureSpec(
            feature_name="a_2h_average",
            feature_expr='"agg_w7200_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f"',
        ),
        "a_48h_average": FeatureSpec(
            feature_name="a_48h_average",
            feature_expr='"agg_w172800_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f"',
        ),
    }
