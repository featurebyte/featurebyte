import pytest

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from tests.util.helper import add_groupby_operation


@pytest.fixture(name="item_data_input_node")
def item_data_input_node_fixture(global_graph, input_details):
    node_params = {
        "type": "item_data",
        "columns": ["order_id", "item_id", "item_name", "item_type"],
    }
    node_params.update(input_details)
    node_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    return node_input


@pytest.fixture(name="event_data_input_node")
def event_data_input_node_fixture(global_graph, input_details):
    """Fixture of a query with some operations ready to run groupby"""
    # pylint: disable=duplicate-code
    node_params = {
        "type": "event_data",
        "columns": ["ts", "cust_id", "order_id", "order_method"],
        "timestamp": "ts",
    }
    node_params.update(input_details)
    node_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    return node_input


@pytest.fixture(name="item_data_join_event_data_node")
def item_data_join_event_data_node_fixture(
    global_graph,
    item_data_input_node,
    event_data_input_node,
):
    """
    Result of:

    item_view.join_event_data_attributes()
    """
    node_params = {
        "left_on": "order_id",
        "right_on": "order_id",
        "left_input_columns": ["order_id", "item_id", "item_name", "item_type"],
        "left_output_columns": ["order_id", "item_id", "item_name", "item_type"],
        "right_input_columns": ["order_method"],
        "right_output_columns": ["order_method"],
        "join_type": "left",
    }
    node = global_graph.add_operation(
        node_type=NodeType.JOIN,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[item_data_input_node, event_data_input_node],
    )
    return node


@pytest.fixture(name="order_size_feature_group_node")
def order_size_feature_group_node_fixture(global_graph, item_data_input_node):
    """
    Result of:

    item_view.groupby("order_id").aggregate(method="count")
    """
    node_params = {
        "keys": ["order_id"],
        "serving_names": ["order_id"],
        "parent": None,
        "agg_func": "count",
        "names": ["order_size"],
    }
    groupby_node = global_graph.add_operation(
        node_type=NodeType.ITEM_GROUPBY,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[item_data_input_node],
    )
    return groupby_node


@pytest.fixture(name="order_size_feature_join_node")
def order_size_feature_join_node_fixture(
    global_graph,
    order_size_feature_group_node,
    event_data_input_node,
):
    """
    Result of:

    event_view["order_size"] = order_size_feature.get_value(entity="order_id")
    """
    node_params = {
        "left_on": "order_id",
        "right_on": "order_id",
        "left_input_columns": ["ts", "cust_id", "order_id", "order_method"],
        "left_output_columns": ["ts", "cust_id", "order_id", "order_method"],
        "right_input_columns": ["order_size"],
        "right_output_columns": ["order_size"],
        "join_type": "left",
    }
    node = global_graph.add_operation(
        node_type=NodeType.JOIN,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[event_data_input_node, order_size_feature_group_node],
    )
    return node


@pytest.fixture(name="order_size_agg_by_cust_id_node")
def order_size_agg_by_cust_id_node_fixture(global_graph, order_size_feature_join_node):
    node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": None,
        "parent": "order_size",
        "agg_func": "avg",
        "time_modulo_frequency": 1800,  # 30m
        "frequency": 3600,  # 1h
        "blind_spot": 900,  # 15m
        "timestamp": "ts",
        "names": ["order_size_30d_avg"],
        "windows": ["30d"],
    }
    node = add_groupby_operation(global_graph, node_params, order_size_feature_join_node)
    return node


def test_item_data_join_event_data_attributes(global_graph, item_data_join_event_data_node):
    raise


def test_order_size_feature(global_graph, order_size_feature_join_node):
    raise


def test_double_aggregation(global_graph, order_size_agg_by_cust_id_node):
    raise
