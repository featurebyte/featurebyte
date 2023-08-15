"""
Test for special operations that do not have a specific SQLNode implementation: filter, project, assign
"""
from featurebyte import SourceType
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType
from featurebyte.query_graph.sql.interpreter.base import BaseGraphInterpreter


def test_handle_filter_node(snowflake_event_view_with_entity):
    """
    Test handling for filter query node
    """
    view = snowflake_event_view_with_entity
    filtered_view = view[view["col_float"] > 1.5]
    graph, node = BaseGraphInterpreter(view.graph, SourceType.SNOWFLAKE).flatten_graph(
        filtered_view.node.name
    )
    sql_node = SQLOperationGraph(
        query_graph=graph, sql_type=SQLType.AGGREGATION, source_type=SourceType.SNOWFLAKE
    ).build(node)
    assert sql_node.context.query_node.name.startswith("input")
    assert sql_node.context.current_query_node.name == node.name


def test_handle_assign_node(snowflake_event_view_with_entity):
    """
    Test handling for assign query node
    """
    view = snowflake_event_view_with_entity
    view["col_float"] = view["col_float"] + 1.5
    graph, node = BaseGraphInterpreter(view.graph, SourceType.SNOWFLAKE).flatten_graph(
        view.node.name
    )
    sql_node = SQLOperationGraph(
        query_graph=graph, sql_type=SQLType.AGGREGATION, source_type=SourceType.SNOWFLAKE
    ).build(node)
    assert sql_node.context.query_node.name.startswith("input")
    assert sql_node.context.current_query_node.name == node.name


def test_handle_project_node(snowflake_event_view_with_entity):
    """
    Test handling for project query node
    """
    view = snowflake_event_view_with_entity
    view = view[["col_int", "col_float"]]
    graph, node = BaseGraphInterpreter(view.graph, SourceType.SNOWFLAKE).flatten_graph(
        view.node.name
    )
    sql_node = SQLOperationGraph(
        query_graph=graph, sql_type=SQLType.AGGREGATION, source_type=SourceType.SNOWFLAKE
    ).build(node)
    assert sql_node.context.query_node.name.startswith("input")
    assert sql_node.context.current_query_node.name == node.name
