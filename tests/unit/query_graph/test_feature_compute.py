"""
Tests for featurebyte.query_graph.feature_compute
"""
import copy
import textwrap

import pytest

from featurebyte.query_graph.feature_common import AggregationSpec
from featurebyte.query_graph.feature_compute import SnowflakeRequestTablePlan


@pytest.fixture(name="agg_spec_template")
def agg_spec_template_fixture():
    """Fixture for an AggregationSpec"""
    agg_spec = AggregationSpec(
        window=86400,
        frequency=3600,
        blind_spot=120,
        time_modulo_frequency=1800,
        tile_table_id="some_tile_id",
        entity_ids=["CUST_ID"],
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
        == "REQUEST_TABLE_W86400_F3600_BS120_M1800_CUST_ID"
    )
    assert (
        plan.get_expanded_request_table_name(agg_spec_max_1d)
        == "REQUEST_TABLE_W86400_F3600_BS120_M1800_CUST_ID"
    )

    ctes = plan.construct_request_tile_indices_ctes()
    assert len(ctes) == 1

    cte = ctes[0]
    assert cte[0] == "REQUEST_TABLE_W86400_F3600_BS120_M1800_CUST_ID"
    expected_sql = """
    SELECT
        REQ.POINT_IN_TIME,
        REQ.CUST_ID,
        T.value AS REQ_TILE_INDEX
    FROM (
        SELECT DISTINCT POINT_IN_TIME, CUST_ID FROM REQUEST_TABLE
    ) REQ,
    Table(
        Flatten(
            SELECT F_COMPUTE_TILE_INDICES(
                DATE_PART(epoch, REQ.POINT_IN_TIME),
                86400,
                3600,
                120,
                1800
            )
        )
    ) T
    """
    assert_sql_equal(cte[1], expected_sql)


def test_request_table_plan__no_sharing(agg_spec_max_2h, agg_spec_max_1d):
    """Test that two incompatible AggregationSpec does not share expanded request tables"""
    plan = SnowflakeRequestTablePlan()
    plan.add_aggregation_spec(agg_spec_max_2h)
    plan.add_aggregation_spec(agg_spec_max_1d)

    assert (
        plan.get_expanded_request_table_name(agg_spec_max_2h)
        == "REQUEST_TABLE_W7200_F3600_BS120_M1800_CUST_ID"
    )
    assert (
        plan.get_expanded_request_table_name(agg_spec_max_1d)
        == "REQUEST_TABLE_W86400_F3600_BS120_M1800_CUST_ID"
    )

    ctes = plan.construct_request_tile_indices_ctes()
    assert len(ctes) == 2

    # check expanded table for 2h
    name, sql = ctes[0]
    assert name == "REQUEST_TABLE_W7200_F3600_BS120_M1800_CUST_ID"
    expected_sql = """
    SELECT
        REQ.POINT_IN_TIME,
        REQ.CUST_ID,
        T.value AS REQ_TILE_INDEX
    FROM (
        SELECT DISTINCT POINT_IN_TIME, CUST_ID FROM REQUEST_TABLE
    ) REQ,
    Table(
        Flatten(
            SELECT F_COMPUTE_TILE_INDICES(
                DATE_PART(epoch, REQ.POINT_IN_TIME),
                7200,
                3600,
                120,
                1800
            )
        )
    ) T
    """
    assert_sql_equal(sql, expected_sql)

    # check expanded table for 1d
    name, sql = ctes[1]
    assert name == "REQUEST_TABLE_W86400_F3600_BS120_M1800_CUST_ID"
    expected_sql = """
    SELECT
        REQ.POINT_IN_TIME,
        REQ.CUST_ID,
        T.value AS REQ_TILE_INDEX
    FROM (
        SELECT DISTINCT POINT_IN_TIME, CUST_ID FROM REQUEST_TABLE
    ) REQ,
    Table(
        Flatten(
            SELECT F_COMPUTE_TILE_INDICES(
                DATE_PART(epoch, REQ.POINT_IN_TIME),
                86400,
                3600,
                120,
                1800
            )
        )
    ) T
    """
    assert_sql_equal(sql, expected_sql)
