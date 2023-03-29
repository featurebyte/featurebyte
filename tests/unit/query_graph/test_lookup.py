"""
Tests for Lookup SQLNode
"""
import textwrap

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType


def test_lookup_node_aggregation(global_graph, lookup_node):
    """
    Test lookup feature node sql during aggregation
    """
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.AGGREGATION, source_type=SourceType.SNOWFLAKE
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


def test_lookup_node_post_aggregation(global_graph, lookup_feature_node):
    """
    Test post aggregation expressions for Lookup node is correct
    """
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.POST_AGGREGATION, source_type=SourceType.SNOWFLAKE
    )
    expr = sql_graph.build(lookup_feature_node).sql
    assert (
        expr.sql()
        == '("_fb_internal_lookup_cust_value_1_input_1" + "_fb_internal_lookup_cust_value_2_input_1")'
    )
