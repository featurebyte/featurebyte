"""
Tests for featurebyte.query_graph.feature_compute
"""
import copy
import textwrap
from dataclasses import asdict

import pytest
from bson import ObjectId
from sqlglot import select

from featurebyte.enum import SourceType
from featurebyte.query_graph.node.generic import ItemGroupbyParameters
from featurebyte.query_graph.sql.aggregator.request_table import RequestTablePlan
from featurebyte.query_graph.sql.aggregator.window import TileBasedRequestTablePlan
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner
from featurebyte.query_graph.sql.specs import (
    AggregationSource,
    FeatureSpec,
    ItemAggregationSpec,
    TileBasedAggregationSpec,
)


@pytest.fixture(name="agg_spec_template")
def agg_spec_template_fixture():
    """Fixture for an AggregationSpec"""
    agg_spec = TileBasedAggregationSpec(
        window=86400,
        frequency=3600,
        blind_spot=120,
        time_modulo_frequency=1800,
        tile_table_id="some_tile_id",
        aggregation_id="some_agg_id",
        keys=["CUST_ID"],
        serving_names=["CID"],
        serving_names_mapping=None,
        value_by=None,
        merge_expr="SUM(value)",
        feature_name="Amount (1d sum)",
        is_order_dependent=False,
        tile_value_columns=["value"],
        entity_ids=[ObjectId()],
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
    parameters = ItemGroupbyParameters(
        keys=["order_id"],
        serving_names=["OID"],
        name="Order Size",
        agg_func="count",
    )
    agg_spec = ItemAggregationSpec(
        parameters=parameters,
        serving_names=["OID"],
        serving_names_mapping=None,
        aggregation_source=AggregationSource(
            expr=select("*").from_("tab"), query_node_name="input_1"
        ),
        entity_ids=[ObjectId()],
    )
    return agg_spec


def assert_sql_equal(sql, expected):
    """Helper function to check that SQL code matches with expected"""
    sql = textwrap.dedent(sql).strip()
    expected = textwrap.dedent(expected).strip()
    assert sql == expected


def test_request_table_plan__share_expanded_table(agg_spec_sum_1d, agg_spec_max_1d):
    """Test that two compatible AggregationSpec shares the same expanded request table"""
    plan = TileBasedRequestTablePlan(source_type=SourceType.SNOWFLAKE)
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
    assert cte[0].sql() == '"REQUEST_TABLE_W86400_F3600_BS120_M1800_CID"'
    expected_sql = """
    SELECT
      "POINT_IN_TIME",
      "CID",
      FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
      ) / 3600) AS "__FB_LAST_TILE_INDEX",
      FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
      ) / 3600) - 24 AS "__FB_FIRST_TILE_INDEX"
    FROM (
      SELECT DISTINCT
        "POINT_IN_TIME",
        "CID"
      FROM REQUEST_TABLE
    )
    """
    assert_sql_equal(cte[1].sql(pretty=True), expected_sql)


def test_request_table_plan__no_sharing(agg_spec_max_2h, agg_spec_max_1d):
    """Test that two incompatible AggregationSpec does not share expanded request tables"""
    plan = TileBasedRequestTablePlan(source_type=SourceType.SNOWFLAKE)
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
    assert name.sql() == '"REQUEST_TABLE_W7200_F3600_BS120_M1800_CID"'
    expected_sql = """
    SELECT
      "POINT_IN_TIME",
      "CID",
      FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
      ) / 3600) AS "__FB_LAST_TILE_INDEX",
      FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
      ) / 3600) - 2 AS "__FB_FIRST_TILE_INDEX"
    FROM (
      SELECT DISTINCT
        "POINT_IN_TIME",
        "CID"
      FROM REQUEST_TABLE
    )
    """
    assert_sql_equal(sql.sql(pretty=True), expected_sql)

    # check expanded table for 1d
    name, sql = ctes[1]
    assert name.sql() == '"REQUEST_TABLE_W86400_F3600_BS120_M1800_CID"'
    expected_sql = """
    SELECT
      "POINT_IN_TIME",
      "CID",
      FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
      ) / 3600) AS "__FB_LAST_TILE_INDEX",
      FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
      ) / 3600) - 24 AS "__FB_FIRST_TILE_INDEX"
    FROM (
      SELECT DISTINCT
        "POINT_IN_TIME",
        "CID"
      FROM REQUEST_TABLE
    )
    """
    assert_sql_equal(sql.sql(pretty=True), expected_sql)


def test_non_time_aware_request_table_plan(item_agg_spec):
    """
    Test NonTimeAwareRequestTablePlan
    """
    plan = RequestTablePlan(is_time_aware=False)
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
    planner = FeatureExecutionPlanner(
        query_graph_with_groupby, source_type=SourceType.SNOWFLAKE, is_online_serving=False
    )
    plan = planner.generate_plan([groupby_node])
    assert list(
        plan.aggregators["window"].window_aggregation_spec_set.get_grouped_aggregation_specs()
    ) == [
        [
            TileBasedAggregationSpec(
                window=7200,
                frequency=3600,
                blind_spot=900,
                time_modulo_frequency=1800,
                tile_table_id="TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
                aggregation_id=f"avg_{groupby_node_aggregation_id}",
                keys=["cust_id"],
                serving_names=["CUSTOMER_ID"],
                serving_names_mapping=None,
                value_by=None,
                merge_expr=(
                    f"SUM(sum_value_avg_{groupby_node_aggregation_id}) / "
                    f"SUM(count_value_avg_{groupby_node_aggregation_id})"
                ),
                feature_name="a_2h_average",
                is_order_dependent=False,
                tile_value_columns=[
                    f"sum_value_avg_{groupby_node_aggregation_id}",
                    f"count_value_avg_{groupby_node_aggregation_id}",
                ],
                entity_ids=[ObjectId("637516ebc9c18f5a277a78db")],
            )
        ],
        [
            TileBasedAggregationSpec(
                window=172800,
                frequency=3600,
                blind_spot=900,
                time_modulo_frequency=1800,
                tile_table_id="TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
                aggregation_id=f"avg_{groupby_node_aggregation_id}",
                keys=["cust_id"],
                serving_names=["CUSTOMER_ID"],
                serving_names_mapping=None,
                value_by=None,
                merge_expr=(
                    f"SUM(sum_value_avg_{groupby_node_aggregation_id}) / "
                    f"SUM(count_value_avg_{groupby_node_aggregation_id})"
                ),
                feature_name="a_48h_average",
                is_order_dependent=False,
                tile_value_columns=[
                    f"sum_value_avg_{groupby_node_aggregation_id}",
                    f"count_value_avg_{groupby_node_aggregation_id}",
                ],
                entity_ids=[ObjectId("637516ebc9c18f5a277a78db")],
            )
        ],
    ]
    assert plan.feature_specs == {
        "a_2h_average": FeatureSpec(
            feature_name="a_2h_average",
            feature_expr=f'"_fb_internal_window_w7200_avg_{groupby_node_aggregation_id}"',
        ),
        "a_48h_average": FeatureSpec(
            feature_name="a_48h_average",
            feature_expr=f'"_fb_internal_window_w172800_avg_{groupby_node_aggregation_id}"',
        ),
    }
    assert plan.required_entity_ids == {ObjectId("637516ebc9c18f5a277a78db")}


def test_feature_execution_planner__serving_names_mapping(
    query_graph_with_groupby, groupby_node_aggregation_id
):
    """Test FeatureExecutionPlanner with serving names mapping provided"""
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    mapping = {"CUSTOMER_ID": "NEW_CUST_ID"}
    planner = FeatureExecutionPlanner(
        query_graph_with_groupby,
        serving_names_mapping=mapping,
        source_type=SourceType.SNOWFLAKE,
        is_online_serving=False,
    )
    plan = planner.generate_plan([groupby_node])
    assert list(
        plan.aggregators["window"].window_aggregation_spec_set.get_grouped_aggregation_specs()
    ) == [
        [
            TileBasedAggregationSpec(
                window=7200,
                frequency=3600,
                blind_spot=900,
                time_modulo_frequency=1800,
                tile_table_id="TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
                aggregation_id=f"avg_{groupby_node_aggregation_id}",
                keys=["cust_id"],
                serving_names=["NEW_CUST_ID"],
                serving_names_mapping=mapping,
                value_by=None,
                merge_expr=(
                    f"SUM(sum_value_avg_{groupby_node_aggregation_id}) / "
                    f"SUM(count_value_avg_{groupby_node_aggregation_id})"
                ),
                feature_name="a_2h_average",
                is_order_dependent=False,
                tile_value_columns=[
                    f"sum_value_avg_{groupby_node_aggregation_id}",
                    f"count_value_avg_{groupby_node_aggregation_id}",
                ],
                entity_ids=[ObjectId("637516ebc9c18f5a277a78db")],
            )
        ],
        [
            TileBasedAggregationSpec(
                window=172800,
                frequency=3600,
                blind_spot=900,
                time_modulo_frequency=1800,
                tile_table_id="TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
                aggregation_id=f"avg_{groupby_node_aggregation_id}",
                keys=["cust_id"],
                serving_names=["NEW_CUST_ID"],
                serving_names_mapping=mapping,
                value_by=None,
                merge_expr=(
                    f"SUM(sum_value_avg_{groupby_node_aggregation_id}) / "
                    f"SUM(count_value_avg_{groupby_node_aggregation_id})"
                ),
                feature_name="a_48h_average",
                is_order_dependent=False,
                tile_value_columns=[
                    f"sum_value_avg_{groupby_node_aggregation_id}",
                    f"count_value_avg_{groupby_node_aggregation_id}",
                ],
                entity_ids=[ObjectId("637516ebc9c18f5a277a78db")],
            )
        ],
    ]
    assert plan.feature_specs == {
        "a_2h_average": FeatureSpec(
            feature_name="a_2h_average",
            feature_expr=f'"_fb_internal_window_w7200_avg_{groupby_node_aggregation_id}"',
        ),
        "a_48h_average": FeatureSpec(
            feature_name="a_48h_average",
            feature_expr=f'"_fb_internal_window_w172800_avg_{groupby_node_aggregation_id}"',
        ),
    }


def test_feature_execution_planner__lookup_features(global_graph, projected_lookup_features):
    """
    Test FeatureExecutionPlanner on an LookupFeature node
    """
    mapping = {"cust_id": "CUSTOMER_ID"}
    planner = FeatureExecutionPlanner(
        global_graph,
        serving_names_mapping=mapping,
        source_type=SourceType.SNOWFLAKE,
        is_online_serving=False,
    )
    nodes = list(projected_lookup_features)
    plan = planner.generate_plan(nodes)
    aggregator = plan.aggregators["lookup"]

    # Check aggregation results
    agg_results = aggregator.get_direct_lookups()
    assert len(agg_results) == 1
    agg_result_dict = asdict(agg_results[0])
    agg_result_dict.pop("expr")
    assert agg_result_dict == {
        "column_names": [
            "_fb_internal_lookup_cust_value_1_input_1",
            "_fb_internal_lookup_cust_value_2_input_1",
        ],
        "join_keys": ["CUSTOMER_ID"],
        "event_timestamp_column": None,
    }

    # Check required serving names
    assert plan.required_serving_names == {"CUSTOMER_ID"}


def test_feature_execution_planner__query_graph_with_graph_node(
    query_graph_with_cleaning_ops_and_groupby,
):
    """Test FeatureExecutionPlanner generates the plan without any error"""
    query_graph, groupby_node = query_graph_with_cleaning_ops_and_groupby
    planner = FeatureExecutionPlanner(
        query_graph, source_type=SourceType.SNOWFLAKE, is_online_serving=False
    )
    execution_plan = planner.generate_plan([groupby_node])
    groupby_node_aggregation_id = "d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac"
    assert execution_plan.feature_specs == {
        "a_2h_average": FeatureSpec(
            feature_name="a_2h_average",
            feature_expr=f'"_fb_internal_window_w7200_avg_{groupby_node_aggregation_id}"',
        ),
        "a_48h_average": FeatureSpec(
            feature_name="a_48h_average",
            feature_expr=f'"_fb_internal_window_w172800_avg_{groupby_node_aggregation_id}"',
        ),
    }


def test_feature_execution_planner__feature_no_entity_ids(
    query_graph_with_groupby_no_entity_ids,
    groupby_node_aggregation_id,
):
    """
    Test FeatureExecutionPlanner when feature node has no entity_ids
    """
    groupby_node = query_graph_with_groupby_no_entity_ids.get_node_by_name("groupby_1")
    planner = FeatureExecutionPlanner(
        query_graph_with_groupby_no_entity_ids,
        source_type=SourceType.SNOWFLAKE,
        is_online_serving=False,
    )
    plan = planner.generate_plan([groupby_node])
    assert plan.required_entity_ids == set()
