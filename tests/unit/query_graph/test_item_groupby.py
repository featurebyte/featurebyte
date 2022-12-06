"""
Tests for ItemGroupby SQLNode
"""
import textwrap

import pytest

from featurebyte.enum import AggFunc, SourceType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType


@pytest.mark.parametrize(
    "parent, agg_func, expected_expr",
    [
        (None, AggFunc.COUNT, "COUNT(*)"),
        ("item_id", AggFunc.SUM, 'SUM("item_id")'),
        ("item_id", AggFunc.MIN, 'MIN("item_id")'),
        ("item_id", AggFunc.MAX, 'MAX("item_id")'),
        ("item_id", AggFunc.AVG, 'AVG("item_id")'),
        ("item_id", AggFunc.STD, 'STDDEV("item_id")'),
        ("item_id", AggFunc.NA_COUNT, 'SUM(CAST("item_id" IS NULL AS INTEGER))'),
    ],
)
def test_item_groupby_sql_node(global_graph, item_data_input_node, parent, agg_func, expected_expr):
    """
    Test ItemGroupby sql generation
    """
    node_params = {
        "keys": ["order_id"],
        "serving_names": ["order_id"],
        "parent": parent,
        "agg_func": agg_func,
        "name": "feature_name",
    }
    groupby_node = global_graph.add_operation(
        node_type=NodeType.ITEM_GROUPBY,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[item_data_input_node],
    )
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.EVENT_VIEW_PREVIEW, source_type=SourceType.SNOWFLAKE
    )
    sql_tree = sql_graph.build(groupby_node).sql
    expected = textwrap.dedent(
        f"""
        SELECT
          "order_id",
          {expected_expr} AS "feature_name"
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
    ).strip()
    assert sql_tree.sql(pretty=True) == expected
