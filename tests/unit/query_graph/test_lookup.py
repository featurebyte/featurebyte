"""
Tests for Lookup SQLNode
"""

import textwrap

import pytest

from featurebyte.query_graph.sql.aggregator.lookup import LookupAggregator
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType
from featurebyte.query_graph.sql.specifications.lookup import LookupSpec
from tests.unit.query_graph.util import get_combined_aggregation_expr_from_aggregator
from tests.util.helper import assert_equal_with_expected_fixture


def test_lookup_node_aggregation(global_graph, lookup_node, source_info):
    """
    Test lookup feature node sql during aggregation
    """
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.AGGREGATION, source_info=source_info
    )
    sql_tree = sql_graph.build(lookup_node).sql
    assert (
        sql_tree.sql(pretty=True)
        == textwrap.dedent(
            """
        SELECT
          "cust_id" AS "cust_id",
          "cust_value_1" AS "cust_value_1",
          "cust_value_2" AS "cust_value_2"
        FROM "db"."public"."dimension_table"
        """
        ).strip()
    )


def test_lookup_node_post_aggregation(global_graph, lookup_feature_node, lookup_node, source_info):
    """
    Test post aggregation expressions for Lookup node is correct
    """
    aggregation_specs = {
        lookup_node.name: LookupSpec.from_query_graph_node(
            node=lookup_node,
            graph=global_graph,
            source_info=source_info,
        )
    }
    sql_graph = SQLOperationGraph(
        global_graph,
        sql_type=SQLType.POST_AGGREGATION,
        source_info=source_info,
        aggregation_specs=aggregation_specs,
    )
    expr = sql_graph.build(lookup_feature_node).sql
    assert (
        expr.sql()
        == '("_fb_internal_CUSTOMER_ID_lookup_cust_value_1_input_1" + "_fb_internal_CUSTOMER_ID_lookup_cust_value_2_input_1")'
    )


@pytest.fixture
def daily_snapshots_table_agg_spec(global_graph, snapshots_lookup_feature_node, source_info):
    """
    Fixture for daily snapshots table aggregation specification
    """
    parent_nodes = global_graph.get_input_node_names(snapshots_lookup_feature_node)
    assert len(parent_nodes) == 1
    agg_node = global_graph.get_node_by_name(parent_nodes[0])
    return LookupSpec.from_query_graph_node(
        node=agg_node,
        graph=global_graph,
        source_info=source_info,
        agg_result_name_include_serving_names=True,
    )[0]


@pytest.mark.parametrize(
    "test_case_name",
    [
        "daily_snapshots",
    ],
)
def test_snapshots_table_lookup(request, test_case_name, update_fixtures, source_info):
    """
    Test lookup aggregator for snapshots table
    """
    test_case_mapping = {
        "daily_snapshots": "daily_snapshots_table_agg_spec",
    }
    fixture_name = test_case_mapping[test_case_name]
    fixture_obj = request.getfixturevalue(fixture_name)
    if isinstance(fixture_obj, list):
        agg_specs = fixture_obj
    else:
        agg_specs = [fixture_obj]

    aggregator = LookupAggregator(source_info=source_info)
    for agg_spec in agg_specs:
        aggregator.update(agg_spec)
    result_expr = get_combined_aggregation_expr_from_aggregator(aggregator)
    assert_equal_with_expected_fixture(
        result_expr.sql(pretty=True),
        f"tests/fixtures/aggregator/expected_lookup_aggregator_{test_case_name}.sql",
        update_fixture=update_fixtures,
    )
