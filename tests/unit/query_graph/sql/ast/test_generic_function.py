"""
Test generic function node
"""
from featurebyte.query_graph.sql.ast.function import GenericFunctionNode
from tests.unit.query_graph.test_sql import make_context, make_str_expression_node


def test_generic_function(input_node):
    """
    Test generic function node
    """
    series_node = make_str_expression_node(table_node=input_node, expr="a")
    node = GenericFunctionNode.build(
        make_context(
            node_type="generic_function",
            parameters={
                "sql_function_name": "my_func",
                "function_parameters": [
                    {"column_name": "a", "dtype": "FLOAT", "input_form": "column"},
                    {"value": 1, "dtype": "INT", "input_form": "value"},
                    {"value": 2.0, "dtype": "FLOAT", "input_form": "value"},
                    {"value": "hello", "dtype": "VARCHAR", "input_form": "value"},
                    {"value": True, "dtype": "BOOL", "input_form": "value"},
                ],
            },
            input_sql_nodes=[series_node],
        )
    )
    assert node.sql.sql() == "MY_FUNC(a, 1, 2.0, 'hello', TRUE)"
