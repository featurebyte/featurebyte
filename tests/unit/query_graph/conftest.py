"""
Common test fixtures used across unit test directories related to query_graph
"""
import copy
import json

import pytest
from bson import ObjectId

from featurebyte import MissingValueImputation
from featurebyte.core.frame import Frame
from featurebyte.enum import DBVarType
from featurebyte.models import DimensionTableModel
from featurebyte.models.parent_serving import ParentServingPreparation
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalGraphState, GlobalQueryGraph
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.node import construct_node
from featurebyte.query_graph.node.schema import FeatureStoreDetails, TableDetails
from tests.util.helper import add_groupby_operation


@pytest.fixture(name="global_graph")
def global_query_graph():
    """
    Empty query graph fixture
    """
    GlobalGraphState.reset()
    yield GlobalQueryGraph()


@pytest.fixture(name="input_details")
def input_details_fixture(request):
    """
    Fixture for table_details and feature_store details for use in tests that rely only on graph
    (not API objects).

    To obtain query graph fixtures with a different source type, indirect parametrize this fixture
    with the table source type name ("snowflake" or "databricks"). Parametrization of this fixture is
    optional; the default value is "snowflake".
    """
    kind = "snowflake"
    if hasattr(request, "param"):
        kind = request.param
    assert kind in {"snowflake", "databricks"}
    if kind == "snowflake":
        input_details = {
            "table_details": {
                "database_name": "db",
                "schema_name": "public",
                "table_name": "event_table",
            },
            "feature_store_details": {
                "type": "snowflake",
                "details": {
                    "database": "db",
                    "sf_schema": "public",
                    "account": "account",
                    "warehouse": "warehouse",
                },
            },
        }
    else:
        input_details = {
            "table_details": {
                "database_name": "db",
                "schema_name": "public",
                "table_name": "event_table",
            },
            "feature_store_details": {
                "type": "databricks",
                "details": {
                    "server_hostname": "databricks-hostname",
                    "http_path": "databricks-http-path",
                    "featurebyte_schema": "public",
                    "featurebyte_catalog": "hive_metastore",
                },
            },
        }
    return input_details


@pytest.fixture(name="entity_id")
def entity_id_fixture():
    """
    Fixture of an entity_id
    """
    return ObjectId("63dbe68cd918ef71acffd127")


@pytest.fixture(name="input_node")
def input_node_fixture(global_graph, input_details):
    """Fixture of a query with some operations ready to run groupby"""
    # pylint: disable=duplicate-code
    node_params = {
        "type": "event_table",
        "columns": [
            {"name": "ts", "dtype": DBVarType.TIMESTAMP},
            {"name": "cust_id", "dtype": DBVarType.INT},
            {"name": "a", "dtype": DBVarType.FLOAT},
            {"name": "b", "dtype": DBVarType.FLOAT},
        ],
        "timestamp_column": "ts",
    }
    node_params.update(input_details)
    node_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    return node_input


@pytest.fixture(name="item_table_input_details")
def item_table_input_details_fixture(input_details):
    """Similar to input_details but for an ItemTable table"""
    input_details = copy.deepcopy(input_details)
    input_details["table_details"]["table_name"] = "item_table"
    return input_details


@pytest.fixture(name="item_table_input_node")
def item_table_input_node_fixture(global_graph, item_table_input_details):
    """Fixture of an input node representing an ItemTable"""
    node_params = {
        "type": "item_table",
        "columns": [
            {"name": "order_id", "dtype": DBVarType.INT},
            {"name": "item_id", "dtype": DBVarType.INT},
            {"name": "item_name", "dtype": DBVarType.VARCHAR},
            {"name": "item_type", "dtype": DBVarType.VARCHAR},
        ],
    }
    node_params.update(item_table_input_details)
    node_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    return node_input


@pytest.fixture(name="scd_table_input_details")
def scd_table_input_details_fixture(input_details):
    """Similar to input_details but for an SlowlyChangingView table"""
    input_details = copy.deepcopy(input_details)
    input_details["table_details"]["table_name"] = "customer_profile_table"
    return input_details


@pytest.fixture(name="scd_table_input_node")
def scd_table_input_node_fixture(global_graph, scd_table_input_details):
    """Fixture of an SlowlyChangingView input node"""
    node_params = {
        "type": "scd_table",
        "columns": [
            {"name": "effective_ts", "dtype": DBVarType.TIMESTAMP},
            {"name": "cust_id", "dtype": DBVarType.INT},
            {"name": "membership_status", "dtype": DBVarType.VARCHAR},
        ],
    }
    node_params.update(scd_table_input_details)
    node_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    return node_input


@pytest.fixture(name="dimension_table_input_details")
def dimension_table_input_details_fixture(input_details):
    """Similar to input_details but for a Dimension table"""
    input_details = copy.deepcopy(input_details)
    input_details["table_details"]["table_name"] = "dimension_table"
    return input_details


@pytest.fixture(name="dimension_table_input_node")
def dimension_table_input_node_fixture(global_graph, dimension_table_input_details):
    """Fixture of a DimensionTable input node"""
    node_params = {
        "type": "dimension_table",
        "columns": [
            {"name": "cust_id", "dtype": DBVarType.INT},
            {"name": "cust_value_1", "dtype": DBVarType.FLOAT},
            {"name": "cust_value_2", "dtype": DBVarType.FLOAT},
        ],
    }
    node_params.update(dimension_table_input_details)
    node_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    return node_input


@pytest.fixture(name="event_table_input_node")
def event_table_input_node_fixture(global_graph, input_details):
    """Fixture of an EventTable input node"""
    # pylint: disable=duplicate-code
    node_params = {
        "type": "event_table",
        "columns": [
            {"name": "ts", "dtype": DBVarType.TIMESTAMP},
            {"name": "cust_id", "dtype": DBVarType.INT},
            {"name": "order_id", "dtype": DBVarType.INT},
            {"name": "order_method", "dtype": DBVarType.VARCHAR},
        ],
        "timestamp": "ts",  # DEV-556: this should be timestamp_column
    }
    node_params.update(input_details)
    node_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    return node_input


@pytest.fixture(name="query_graph_and_assign_node")
def query_graph_and_assign_node_fixture(global_graph, input_node):
    """Fixture of a query with some operations ready to run groupby"""
    # pylint: disable=duplicate-code
    proj_a = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    proj_b = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["b"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    sum_node = global_graph.add_operation(
        node_type=NodeType.ADD,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a, proj_b],
    )
    assign_node = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "c"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node, sum_node],
    )
    return global_graph, assign_node


@pytest.fixture(name="groupby_node_params")
def groupby_node_params_fixture():
    """Fixture groupby node parameters"""
    node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": None,
        "parent": "a",
        "agg_func": "avg",
        "time_modulo_frequency": 1800,  # 30m
        "frequency": 3600,  # 1h
        "blind_spot": 900,  # 15m
        "timestamp": "ts",
        "names": ["a_2h_average", "a_48h_average"],
        "windows": ["2h", "48h"],
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db")],
    }
    return node_params


@pytest.fixture(name="groupby_node_params_max_agg")
def groupby_node_params_max_agg_fixture():
    """Fixture groupby node parameters

    Same feature job settings as groupby_node_params_fixture, but with different aggregation and
    different feature windows
    """
    node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": None,
        "parent": "a",
        "agg_func": "max",
        "time_modulo_frequency": 1800,  # 30m
        "frequency": 3600,  # 1h
        "blind_spot": 900,  # 15m
        "timestamp": "ts",
        "names": ["a_2h_max", "a_36h_max"],
        "windows": ["2h", "36h"],
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db")],
    }
    return node_params


@pytest.fixture(name="groupby_node_params_sum_agg")
def groupby_node_params_sum_agg_fixture():
    """Fixture groupby node parameters

    Same feature job settings as groupby_node_params_fixture, but with different aggregation and
    different feature windows
    """
    node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": None,
        "parent": "a",
        "agg_func": "sum",
        "time_modulo_frequency": 1800,  # 30m
        "frequency": 3600,  # 1h
        "blind_spot": 900,  # 15m
        "timestamp": "ts",
        "names": ["a_2h_sum", "a_36h_sum"],
        "windows": ["2h", "36h"],
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db")],
    }
    return node_params


@pytest.fixture(name="query_graph_with_groupby")
def query_graph_with_groupby_fixture(query_graph_and_assign_node, groupby_node_params):
    """Fixture of a query graph with a groupby operation"""
    graph, assign_node = query_graph_and_assign_node
    node_params = groupby_node_params
    add_groupby_operation(graph, node_params, assign_node)
    return graph


@pytest.fixture(name="query_graph_with_groupby_no_entity_ids")
def query_graph_with_groupby_fixture_no_entity_ids(
    query_graph_and_assign_node, groupby_node_params
):
    """
    Fixture of a query graph with a groupby operation (DEV-556: old version without entity_ids)
    """
    graph, assign_node = query_graph_and_assign_node
    node_params = copy.deepcopy(groupby_node_params)
    node_params.pop("entity_ids")
    add_groupby_operation(graph, node_params, assign_node)
    return graph


@pytest.fixture(name="query_graph_with_groupby_and_feature_nodes")
def query_graph_with_groupby_and_feature_nodes_fixture(query_graph_with_groupby):
    graph = query_graph_with_groupby
    feature_proj_1 = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_2h_average"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[graph.get_node_by_name("groupby_1")],
    )
    feature_proj_2 = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_48h_average"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[graph.get_node_by_name("groupby_1")],
    )
    feature_post_processed = graph.add_operation(
        node_type=NodeType.ADD,
        node_params={"value": 123},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[graph.get_node_by_name(feature_proj_2.name)],
    )
    feature_alias = graph.add_operation(
        node_type=NodeType.ALIAS,
        node_params={"name": "a_48h_average plus 123"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[graph.get_node_by_name(feature_post_processed.name)],
    )
    return graph, feature_proj_1, feature_alias


@pytest.fixture(name="groupby_node_aggregation_id")
def groupby_node_aggregation_id_fixture(query_graph_with_groupby):
    """Groupby node the aggregation id (without aggregation method part)"""
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    aggregation_id = groupby_node.parameters.aggregation_id.split("_")[1]
    assert aggregation_id == "47938f0bfcde2a5c7d483ce1926aa72900653d65"
    return aggregation_id


@pytest.fixture(name="query_graph_with_category_groupby")
def query_graph_with_category_groupby_fixture(query_graph_and_assign_node, groupby_node_params):
    """Fixture of a query graph with a groupby operation"""
    graph, assign_node = query_graph_and_assign_node
    node_params = groupby_node_params
    node_params["value_by"] = "product_type"
    add_groupby_operation(graph, node_params, assign_node)
    return graph


@pytest.fixture(name="query_graph_with_similar_groupby_nodes")
def query_graph_with_similar_groupby_nodes(
    query_graph_and_assign_node,
    groupby_node_params,
    groupby_node_params_sum_agg,
    groupby_node_params_max_agg,
):
    """Fixture of a query graph with two similar groupby operations (identical job settings and
    entity columns)
    """
    graph, assign_node = query_graph_and_assign_node
    node1 = add_groupby_operation(graph, groupby_node_params, assign_node)
    node2 = add_groupby_operation(graph, groupby_node_params_max_agg, assign_node)
    node3 = add_groupby_operation(graph, groupby_node_params_sum_agg, assign_node)
    return [node1, node2, node3], graph


@pytest.fixture(name="complex_feature_query_graph")
def complex_feature_query_graph_fixture(query_graph_with_groupby):
    """Fixture of a query graph with two independent groupby operations"""
    graph = query_graph_with_groupby
    node_params = {
        "keys": ["biz_id"],
        "value_by": None,
        "parent": "a",
        "agg_func": "sum",
        "time_modulo_frequency": 1800,  # 30m
        "frequency": 3600,  # 1h
        "blind_spot": 900,  # 15m
        "timestamp": "ts",
        "names": ["a_7d_sum_by_business"],
        "windows": ["7d"],
        "serving_names": ["BUSINESS_ID"],
        "entity_ids": [ObjectId("6375171ac9c18f5a277a78dc")],
    }
    assign_node = graph.get_node_by_name("assign_1")
    groupby_1 = graph.get_node_by_name("groupby_1")
    groupby_2 = add_groupby_operation(graph, node_params, assign_node)
    feature_proj_1 = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_2h_average"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[groupby_1],
    )
    feature_proj_2 = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_7d_sum_by_business"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[groupby_2],
    )
    complex_feature_node = graph.add_operation(
        node_type=NodeType.DIV,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[feature_proj_1, feature_proj_2],
    )
    complex_feature_node_alias = graph.add_operation(
        node_type=NodeType.ALIAS,
        node_params={"name": "a_2h_avg_by_user_div_7d_by_biz"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[complex_feature_node],
    )
    return complex_feature_node_alias, graph


@pytest.fixture(name="join_node_params")
def join_node_params_fixture():
    """Join node parameters fixture"""
    return {
        "left_on": "order_id",
        "right_on": "order_id",
        "left_input_columns": ["order_method"],
        "left_output_columns": ["order_method"],
        "right_input_columns": ["order_id", "item_id", "item_name", "item_type"],
        "right_output_columns": ["order_id", "item_id", "item_name", "item_type"],
        "join_type": "inner",
    }


@pytest.fixture(name="item_table_join_event_table_node")
def item_table_join_event_table_node_fixture(
    global_graph,
    item_table_input_node,
    event_table_input_node,
    join_node_params,
):
    """
    Fixture of a join node that joins EventTable columns into ItemView. Result of:

    item_view.join_event_table_attributes()
    """
    node = global_graph.add_operation(
        node_type=NodeType.JOIN,
        node_params=join_node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[event_table_input_node, item_table_input_node],
    )
    return node


@pytest.fixture(name="item_table_joined_event_table_feature_node")
def item_table_joined_event_table_feature_node_fixture(
    global_graph, item_table_join_event_table_node
):
    """
    Fixture of a feature using item table joined with event table as input
    """
    node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": "item_type",
        "parent": None,
        "agg_func": "count",
        "time_modulo_frequency": 1800,  # 30m
        "frequency": 3600,  # 1h
        "blind_spot": 900,  # 15m
        "timestamp": "ts",
        "names": ["item_type_count_30d"],
        "windows": ["30d"],
    }
    groupby_node = add_groupby_operation(
        global_graph, node_params, item_table_join_event_table_node
    )
    feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["item_type_count_30d"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(groupby_node.name)],
    )
    return feature_node


@pytest.fixture(name="order_size_feature_group_node")
def order_size_feature_group_node_fixture(global_graph, item_table_input_node):
    """
    Fixture of a non-time aware groupby node
    """
    node_params = {
        "keys": ["order_id"],
        "serving_names": ["order_id"],
        "parent": None,
        "agg_func": "count",
        "name": "order_size",
        "entity_ids": [ObjectId("63748c9244bc4549b25f8200")],
    }
    groupby_node = global_graph.add_operation(
        node_type=NodeType.ITEM_GROUPBY,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[item_table_input_node],
    )
    return groupby_node


@pytest.fixture(name="order_size_feature_node")
def order_size_feature_node_fixture(global_graph, order_size_feature_group_node):
    """
    Fixture of a non-time aware Feature from ItemView. Result of:

    order_size_feature = item_view.groupby("order_id").aggregate(method="count") + 123
    """
    graph = global_graph
    feature_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["order_size"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[graph.get_node_by_name(order_size_feature_group_node.name)],
    )
    add_node = global_graph.add_operation(
        node_type=NodeType.ADD,
        node_params={"value": 123},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[feature_node],
    )
    return add_node


@pytest.fixture(name="order_size_feature_join_node")
def order_size_feature_join_node_fixture(
    global_graph,
    order_size_feature_node,
    event_table_input_node,
):
    """
    Fixture of a non-time aware feature joined to EventView. Result of:

    event_view.add_feature("ord_size", order_size_feature, entity="order_id")
    """
    node_params = {
        "view_entity_column": "order_id",
        "feature_entity_column": "order_id",
        "name": "ord_size",
    }
    node = global_graph.add_operation(
        node_type=NodeType.JOIN_FEATURE,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[event_table_input_node, order_size_feature_node],
    )
    return node


@pytest.fixture(name="order_size_agg_by_cust_id_graph")
def order_size_agg_by_cust_id_graph_fixture(global_graph, order_size_feature_join_node, entity_id):
    """
    Fixture of a groupby node using a non-time aware feature as the parent
    """
    node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "entity_ids": [entity_id],
        "value_by": None,
        "parent": "ord_size",
        "agg_func": "avg",
        "time_modulo_frequency": 1800,  # 30m
        "frequency": 3600,  # 1h
        "blind_spot": 900,  # 15m
        "timestamp": "ts",
        "names": ["order_size_30d_avg"],
        "windows": ["30d"],
    }
    node = add_groupby_operation(global_graph, node_params, order_size_feature_join_node)
    return global_graph, node


@pytest.fixture(name="item_table_join_event_table_with_renames_node")
def item_table_join_event_table_with_renames_node_fixture(
    global_graph,
    item_table_input_node,
    event_table_input_node,
):
    """
    Fixture of a join node with column renames
    """
    node_params = {
        "left_on": "order_id",
        "right_on": "order_id",
        "left_input_columns": ["order_id", "order_method"],
        "left_output_columns": ["order_id", "order_method_left"],
        "right_input_columns": ["item_type", "item_name"],
        "right_output_columns": ["item_type_right", "item_name_right"],
        "join_type": "inner",
    }
    node = global_graph.add_operation(
        node_type=NodeType.JOIN,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[event_table_input_node, item_table_input_node],
    )
    return node


@pytest.fixture(name="mixed_point_in_time_and_item_aggregations")
def mixed_point_in_time_and_item_aggregations_fixture(
    query_graph_with_groupby,
    item_table_input_node,
    entity_id,
):
    """
    Fixture for a graph with both point in time and item (non-time aware) aggregations
    """
    graph = query_graph_with_groupby
    node_params = {
        "keys": ["order_id"],
        "serving_names": ["order_id"],
        "entity_ids": [entity_id],
        "parent": None,
        "agg_func": "count",
        "name": "order_size",
    }
    item_groupby_node = graph.add_operation(
        node_type=NodeType.ITEM_GROUPBY,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[item_table_input_node],
    )
    item_groupby_feature_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["order_size"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[graph.get_node_by_name(item_groupby_node.name)],
    )
    groupby_node = graph.get_node_by_name("groupby_1")
    return graph, groupby_node, item_groupby_feature_node


@pytest.fixture(name="mixed_point_in_time_and_item_aggregations_features")
def mixed_point_in_time_and_item_aggregations_features_fixture(
    mixed_point_in_time_and_item_aggregations,
):
    graph, groupby_node, item_groupby_feature_node = mixed_point_in_time_and_item_aggregations
    feature_proj_1 = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_48h_average"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[graph.get_node_by_name(groupby_node.name)],
    )
    return graph, feature_proj_1, item_groupby_feature_node


@pytest.fixture(name="scd_join_node")
def scd_join_node_fixture(
    global_graph,
    event_table_input_node,
    scd_table_input_node,
):
    """
    Fixture of a join node that performs an SCD join between EventTable and DimensionTable
    """
    node_params = {
        "left_on": "cust_id",
        "right_on": "cust_id",
        "left_input_columns": ["event_timestamp", "cust_id", "event_column_1", "event_column_2"],
        "left_output_columns": [
            "event_timestamp",
            "cust_id",
            "event_column_1_out",
            "event_column_2_out",
        ],
        "right_input_columns": ["membership_status"],
        "right_output_columns": ["latest_membership_status"],
        "join_type": "left",
        "scd_parameters": {
            "left_timestamp_column": "event_timestamp",
            "effective_timestamp_column": "effective_timestamp",
        },
    }
    node = global_graph.add_operation(
        node_type=NodeType.JOIN,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[event_table_input_node, scd_table_input_node],
    )
    return node


@pytest.fixture(name="lookup_node")
def lookup_node_fixture(global_graph, dimension_table_input_node, entity_id):
    """
    Fixture of a lookup feature node with multiple features
    """
    node_params = {
        "input_column_names": ["cust_value_1", "cust_value_2"],
        "feature_names": ["CUSTOMER ATTRIBUTE 1", "CUSTOMER ATTRIBUTE 2"],
        "entity_column": "cust_id",
        "serving_name": "CUSTOMER_ID",
        "entity_id": entity_id,
    }
    lookup_node = global_graph.add_operation(
        node_type=NodeType.LOOKUP,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[dimension_table_input_node],
    )
    return lookup_node


@pytest.fixture(name="projected_lookup_features")
def projected_lookup_features_fixture(global_graph, lookup_node):
    """
    Fixture of features projected from lookup node
    """
    feature_node_1 = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["CUSTOMER ATTRIBUTE 1"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(lookup_node.name)],
    )
    feature_node_2 = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["CUSTOMER ATTRIBUTE 2"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(lookup_node.name)],
    )
    return feature_node_1, feature_node_2


@pytest.fixture(name="lookup_feature_node")
def lookup_feature_node_fixture(global_graph, projected_lookup_features):
    """
    Fixture of a derived lookup feature
    """
    feature_node_1, feature_node_2 = projected_lookup_features
    feature_node = global_graph.add_operation(
        node_type=NodeType.ADD,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[feature_node_1, feature_node_2],
    )
    feature_alias = global_graph.add_operation(
        node_type=NodeType.ALIAS,
        node_params={"name": "MY FEATURE"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[feature_node],
    )
    return feature_alias


@pytest.fixture(name="event_lookup_node")
def event_lookup_node_fixture(global_graph, event_table_input_node, entity_id):
    """
    Fixture of a lookup feature node from EventTable
    """
    node_params = {
        "input_column_names": ["order_method"],
        "feature_names": ["Order Method"],
        "entity_column": "order_id",
        "serving_name": "ORDER_ID",
        "entity_id": entity_id,
        "event_parameters": {"event_timestamp_column": "ts"},
    }
    lookup_node = global_graph.add_operation(
        node_type=NodeType.LOOKUP,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[event_table_input_node],
    )
    return lookup_node


@pytest.fixture(name="event_lookup_feature_node")
def event_lookup_feature_node_fixture(global_graph, event_lookup_node):
    """
    Fixture of a lookup feature from EventTable
    """
    feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["Order Method"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(event_lookup_node.name)],
    )
    return feature_node


@pytest.fixture(name="scd_lookup_node_parameters")
def scd_lookup_node_parameters_fixture(entity_id):
    return {
        "input_column_names": ["membership_status"],
        "feature_names": ["Current Membership Status"],
        "entity_column": "cust_id",
        "serving_name": "CUSTOMER_ID",
        "entity_id": entity_id,
        "scd_parameters": {
            "effective_timestamp_column": "event_timestamp",
            "natural_key_column": "cust_id",
            "current_flag_column": "is_record_current",
        },
    }


@pytest.fixture(name="scd_lookup_node")
def scd_lookup_node_fixture(global_graph, scd_lookup_node_parameters, scd_table_input_node):
    """
    Fixture of a SCD lookup node
    """
    lookup_node = global_graph.add_operation(
        node_type=NodeType.LOOKUP,
        node_params=scd_lookup_node_parameters,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[scd_table_input_node],
    )
    return lookup_node


@pytest.fixture(name="scd_lookup_without_current_flag_node")
def scd_lookup_without_current_flag_node_fixture(
    global_graph, scd_lookup_node_parameters, scd_table_input_node
):
    """
    Fixture of a SCD lookup node without current flag column
    """
    scd_lookup_node_parameters = copy.deepcopy(scd_lookup_node_parameters)
    scd_lookup_node_parameters["scd_parameters"].pop("current_flag_column")
    lookup_node = global_graph.add_operation(
        node_type=NodeType.LOOKUP,
        node_params=scd_lookup_node_parameters,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[scd_table_input_node],
    )
    return lookup_node


@pytest.fixture(name="scd_lookup_feature_node")
def scd_lookup_feature_node_fixture(global_graph, scd_lookup_node):
    """
    Fixture of a SCD lookup feature node
    """
    feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["Current Membership Status"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(scd_lookup_node.name)],
    )
    return feature_node


@pytest.fixture(name="scd_offset_lookup_node")
def scd_offset_lookup_node_fixture(global_graph, scd_table_input_node, scd_lookup_node_parameters):
    node_params = copy.deepcopy(scd_lookup_node_parameters)
    node_params["scd_parameters"]["offset"] = "14d"
    lookup_node = global_graph.add_operation(
        node_type=NodeType.LOOKUP,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[scd_table_input_node],
    )
    return lookup_node


@pytest.fixture(name="scd_offset_lookup_feature_node")
def scd_offset_lookup_feature_node_fixture(global_graph, scd_offset_lookup_node):
    """
    Fixture of a SCD lookup feature node with an offset
    """
    feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["Current Membership Status"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(scd_offset_lookup_node.name)],
    )
    return feature_node


@pytest.fixture(name="latest_value_aggregation_feature_node")
def latest_value_aggregation_feature_node_fixture(global_graph, input_node):
    node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": None,
        "parent": "a",
        "agg_func": "latest",
        "time_modulo_frequency": 1800,  # 30m
        "frequency": 3600,  # 1h
        "blind_spot": 900,  # 15m
        "timestamp": "ts",
        "names": ["a_latest_value_past_90d"],
        "windows": ["90d"],
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db")],
    }
    groupby_node = add_groupby_operation(global_graph, node_params, input_node)
    feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_latest_value_past_90d"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(groupby_node.name)],
    )
    return feature_node


@pytest.fixture(name="latest_value_without_window_feature_node")
def latest_value_without_window_feature_node_fixture(global_graph, input_node):
    node_params = {
        "keys": ["cust_id", "biz_id"],
        "serving_names": ["CUSTOMER_ID", "BUSINESS_ID"],
        "value_by": None,
        "parent": "a",
        "agg_func": "latest",
        "time_modulo_frequency": 1800,  # 30m
        "frequency": 3600,  # 1h
        "blind_spot": 900,  # 15m
        "timestamp": "ts",
        "names": ["a_latest_value"],
        "windows": [None],
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db"), ObjectId("637516ebc9c18f5a277a78dc")],
    }
    groupby_node = add_groupby_operation(global_graph, node_params, input_node)
    feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_latest_value"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(groupby_node.name)],
    )
    return feature_node


@pytest.fixture(name="aggregate_asat_feature_node")
def aggregate_asat_feature_node_fixture(global_graph, scd_table_input_node):
    node_params = {
        "keys": ["membership_status"],
        "serving_names": ["MEMBERSHIP_STATUS"],
        "value_by": None,
        "parent": None,
        "agg_func": "count",
        "name": "asat_feature",
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db")],
        "effective_timestamp_column": "effective_ts",
        "natural_key_column": "cust_id",
    }
    aggregate_asat_node = global_graph.add_operation(
        node_type=NodeType.AGGREGATE_AS_AT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[scd_table_input_node],
    )
    feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["asat_feature"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(aggregate_asat_node.name)],
    )
    return feature_node


@pytest.fixture(name="event_table_details")
def get_event_table_details_fixture():
    """
    Get event table details fixture
    """
    return TableDetails(
        database_name="db",
        schema_name="public",
        table_name="transaction",
    )


@pytest.fixture(name="graph_single_node")
def query_graph_single_node(
    global_graph, event_table_details, snowflake_feature_store_details_dict
):
    """
    Query graph with a single node
    """
    node_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "type": "event_table",
            "columns": [{"name": "column", "dtype": "FLOAT"}],
            "table_details": event_table_details.dict(),
            "feature_store_details": snowflake_feature_store_details_dict,
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    pruned_graph, node_name_map = global_graph.prune(target_node=node_input, aggressive=True)
    mapped_node = pruned_graph.get_node_by_name(node_name_map[node_input.name])
    assert mapped_node.name == "input_1"
    graph_dict = global_graph.dict()
    assert graph_dict == pruned_graph.dict()
    assert graph_dict["nodes"] == [
        {
            "name": "input_1",
            "type": "input",
            "parameters": {
                "type": "event_table",
                "columns": [{"name": "column", "dtype": "FLOAT"}],
                "table_details": event_table_details.dict(),
                "feature_store_details": snowflake_feature_store_details_dict,
                "timestamp_column": None,
                "id": None,
                "id_column": None,
            },
            "output_type": "frame",
        }
    ]
    assert graph_dict["edges"] == []
    assert node_input.type == "input"
    yield global_graph, node_input


@pytest.fixture(name="graph_two_nodes")
def query_graph_two_nodes(graph_single_node):
    """
    Query graph with two nodes
    """
    graph, node_input = graph_single_node
    node_proj = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["column"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )

    pruned_graph, node_name_map = graph.prune(target_node=node_proj, aggressive=True)
    mapped_node = pruned_graph.get_node_by_name(node_name_map[node_proj.name])
    assert mapped_node.name == "project_1"
    graph_dict = graph.dict()
    assert graph_dict == pruned_graph.dict()
    assert set(node["name"] for node in graph_dict["nodes"]) == {"input_1", "project_1"}
    assert graph_dict["edges"] == [{"source": "input_1", "target": "project_1"}]
    assert node_proj == construct_node(
        name="project_1", type="project", parameters={"columns": ["column"]}, output_type="series"
    )
    yield graph, node_input, node_proj


@pytest.fixture(name="graph_three_nodes")
def query_graph_three_nodes(graph_two_nodes):
    """
    Query graph with three nodes
    """
    graph, node_input, node_proj = graph_two_nodes
    node_eq = graph.add_operation(
        node_type=NodeType.EQ,
        node_params={"value": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_proj],
    )
    pruned_graph, node_name_map = graph.prune(target_node=node_eq, aggressive=True)
    mapped_node = pruned_graph.get_node_by_name(node_name_map[node_eq.name])
    assert mapped_node.name == "eq_1"
    graph_dict = graph.dict()
    assert graph_dict == pruned_graph.dict()
    assert set(node["name"] for node in graph_dict["nodes"]) == {"input_1", "project_1", "eq_1"}
    assert graph_dict["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "project_1", "target": "eq_1"},
    ]
    assert node_eq == construct_node(
        name="eq_1", type="eq", parameters={"value": 1}, output_type="series"
    )
    yield graph, node_input, node_proj, node_eq


@pytest.fixture(name="graph_four_nodes")
def query_graph_four_nodes(graph_three_nodes):
    """
    Query graph with four nodes
    """
    graph, node_input, node_proj, node_eq = graph_three_nodes
    node_filter = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, node_eq],
    )
    pruned_graph, node_name_map = graph.prune(target_node=node_filter, aggressive=True)
    mapped_node = pruned_graph.get_node_by_name(node_name_map[node_filter.name])
    assert mapped_node.name == "filter_1"
    graph_dict = graph.dict()
    assert graph_dict == pruned_graph.dict()
    assert set(node["name"] for node in graph_dict["nodes"]) == {
        "input_1",
        "project_1",
        "eq_1",
        "filter_1",
    }
    assert graph_dict["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "project_1", "target": "eq_1"},
        {"source": "input_1", "target": "filter_1"},
        {"source": "eq_1", "target": "filter_1"},
    ]
    assert node_filter == construct_node(
        name="filter_1", type="filter", parameters={}, output_type="frame"
    )
    yield graph, node_input, node_proj, node_eq, node_filter


@pytest.fixture(name="dataframe")
def dataframe_fixture(global_graph, snowflake_feature_store):
    """
    Frame test fixture
    """
    columns_info = [
        {"name": "CUST_ID", "dtype": DBVarType.INT},
        {"name": "PRODUCT_ACTION", "dtype": DBVarType.VARCHAR},
        {"name": "VALUE", "dtype": DBVarType.FLOAT},
        {"name": "MASK", "dtype": DBVarType.BOOL},
        {"name": "TIMESTAMP_VALUE", "dtype": DBVarType.TIMESTAMP},
    ]
    node = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "type": "source_table",
            "columns": columns_info,
            "timestamp": "VALUE",
            "table_details": {
                "database_name": "db",
                "schema_name": "public",
                "table_name": "transaction",
            },
            "feature_store_details": {
                "type": "snowflake",
                "details": {
                    "database": "db",
                    "sf_schema": "public",
                    "account": "account",
                    "warehouse": "warehouse",
                },
            },
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    yield Frame(
        feature_store=snowflake_feature_store,
        tabular_source={
            "feature_store_id": snowflake_feature_store.id,
            "table_details": {
                "database_name": "db",
                "schema_name": "public",
                "table_name": "some_table_name",
            },
        },
        columns_info=columns_info,
        node_name=node.name,
    )


@pytest.fixture(name="query_graph_with_cleaning_ops_graph_node")
def query_graph_with_cleaning_ops_graph_node_fixture(global_graph, input_node):
    """Fixture of a query with some operations ready to run groupby"""
    graph_node, proxy_input_nodes = GraphNode.create(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
        graph_node_type=GraphNodeType.CLEANING,
    )
    operation = MissingValueImputation(imputed_value=0)
    node_op = operation.add_cleaning_operation(
        graph_node=graph_node,
        input_node=graph_node.output_node,
        dtype=DBVarType.UNKNOWN,
    )
    graph_node.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "a", "value": None},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[proxy_input_nodes[0], node_op],
    )
    inserted_graph_node = global_graph.add_node(graph_node, [input_node])
    return global_graph, inserted_graph_node


@pytest.fixture(name="query_graph_with_cleaning_ops_and_groupby")
def query_graph_with_cleaning_ops_and_groupby_fixture(
    query_graph_with_cleaning_ops_graph_node, groupby_node_params
):
    """Fixture of a query graph (with cleaning operations) and a groupby operation"""
    graph, graph_node = query_graph_with_cleaning_ops_graph_node
    node_params = groupby_node_params
    groupby_node = add_groupby_operation(graph, node_params, graph_node)
    return graph, groupby_node


@pytest.fixture(name="parent_serving_preparation")
def parent_serving_preparation_fixture():

    with open("tests/fixtures/request_payloads/dimension_table.json") as f:
        data_model = DimensionTableModel(**json.load(f))

    with open("tests/fixtures/request_payloads/feature_store.json") as f:
        feature_store_details = FeatureStoreDetails(**json.load(f))

    parent_serving_preparation = ParentServingPreparation(
        join_steps=[
            {
                "table": data_model,
                "parent_key": "col_int",
                "parent_serving_name": "COL_INT",
                "child_key": "col_text",
                "child_serving_name": "COL_TEXT",
            }
        ],
        feature_store_details=feature_store_details,
    )
    return parent_serving_preparation
