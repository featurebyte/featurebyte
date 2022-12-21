"""
Test reconstruction module
"""
from featurebyte.enum import TableDataType
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.transform.reconstruction import GroupbyNode


def test_get_table_details(graph_single_node, groupby_node_params):
    """
    Test _get_table_details - single input node of event data should return the event data table details.
    """
    graph, node_input = graph_single_node
    assert len(graph.nodes) == 1
    assert node_input.parameters.type == TableDataType.EVENT_DATA
    event_data_table_details = TableDetails(
        database_name="db",
        schema_name="public",
        table_name="transaction",
    )
    assert node_input.parameters.table_details == event_data_table_details

    details = GroupbyNode._get_table_details(graph, node_input)
    assert details == event_data_table_details
