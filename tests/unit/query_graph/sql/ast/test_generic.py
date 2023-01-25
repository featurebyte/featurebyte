"""
Test generic module
"""
import pytest

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.generic import Conditional
from tests.unit.query_graph.test_sql import make_context, make_str_expression_node


def test_conditional__scalar_parameter(input_node):
    """
    Test conditional node
    """
    series_node = make_str_expression_node(table_node=input_node, expr="series")
    mask = make_str_expression_node(table_node=input_node, expr="mask")
    node = Conditional.build(
        make_context(
            node_type=NodeType.CONDITIONAL,
            parameters={
                "value": 2,
            },
            input_sql_nodes=[series_node, mask],
        )
    )
    assert node.sql.sql() == "CASE WHEN mask THEN 2 ELSE series END"


def test_conditional__series_mask(input_node):
    """
    Test conditional node
    """
    series_node = make_str_expression_node(table_node=input_node, expr="series")
    mask = make_str_expression_node(table_node=input_node, expr="mask")
    assigned_series = make_str_expression_node(table_node=input_node, expr="assigned_series")
    node = Conditional.build(
        make_context(
            node_type=NodeType.CONDITIONAL,
            input_sql_nodes=[series_node, mask, assigned_series],
        )
    )
    assert node.sql.sql() == "CASE WHEN mask THEN assigned_series ELSE series END"


def test_conditional__both_provided_should_error(input_node):
    """
    Test conditional node
    """
    series_node = make_str_expression_node(table_node=input_node, expr="series")
    mask = make_str_expression_node(table_node=input_node, expr="mask")
    assigned_series = make_str_expression_node(table_node=input_node, expr="assigned_series")
    with pytest.raises(ValueError) as exc:
        Conditional.build(
            make_context(
                node_type=NodeType.CONDITIONAL,
                parameters={
                    "value": 2,
                },
                input_sql_nodes=[series_node, mask, assigned_series],
            )
        )
    assert "too many values provided" in str(exc)
