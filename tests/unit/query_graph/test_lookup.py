"""
Tests for Lookup SQLNode
"""

import textwrap

from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType
from featurebyte.query_graph.sql.specifications.lookup import LookupSpec


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
