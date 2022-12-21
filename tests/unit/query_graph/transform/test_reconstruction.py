"""
Test reconstruction module
"""
from featurebyte.enum import TableDataType
from featurebyte.query_graph.transform.reconstruction import GroupbyNode


def test_get_table_details__event_data(graph_single_node, event_data_table_details):
    """
    Test _get_table_details - single input node of event data should return the event data table details.
    """
    graph, node_input = graph_single_node
    assert len(graph.nodes) == 1
    assert node_input.parameters.type == TableDataType.EVENT_DATA
    assert node_input.parameters.table_details == event_data_table_details

    details = GroupbyNode._get_table_details(graph, node_input)
    assert details == event_data_table_details


def test_get_table_details__scd_data(graph_single_node_scd_data, scd_table_details):
    """
    Test _get_table_details - single input node of SCD data should return the event data table details.
    """
    graph, node_input = graph_single_node_scd_data
    assert len(graph.nodes) == 1
    assert node_input.parameters.type == TableDataType.SCD_DATA
    assert node_input.parameters.table_details == scd_table_details

    details = GroupbyNode._get_table_details(graph, node_input)
    assert details == scd_table_details
