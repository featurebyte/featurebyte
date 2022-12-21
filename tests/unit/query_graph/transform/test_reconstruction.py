"""
Test reconstruction module
"""
import pytest

from featurebyte.enum import TableDataType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
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


@pytest.fixture(name="graph_with_scd_and_event_data")
def get_graph_with_scd_and_event_data_fixture(
    global_graph, scd_table_details, event_data_table_details, snowflake_feature_store_details_dict
):
    """
    Fixture to get graph with SCD and event data input nodes
    """
    event_data_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "type": "event_data",
            "columns": ["column"],
            "table_details": event_data_table_details.dict(),
            "feature_store_details": snowflake_feature_store_details_dict,
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )

    node_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "type": "scd_data",
            "columns": ["column"],
            "table_details": scd_table_details.dict(),
            "feature_store_details": snowflake_feature_store_details_dict,
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[event_data_input],
    )

    yield global_graph, node_input


def test_get_table_details__event_data_and_scd_data(
    graph_with_scd_and_event_data, event_data_table_details
):
    """
    Test _get_table_details - two input node with SCD and event data should return the event data table details.
    """
    graph, node_input = graph_with_scd_and_event_data
    assert len(graph.nodes) == 2

    details = GroupbyNode._get_table_details(graph, node_input)
    assert details == event_data_table_details
