"""
Common test fixtures used across unit test directories related to query_graph
"""

import copy
import json

import pytest
from bson import ObjectId

from featurebyte import Crontab, MissingValueImputation
from featurebyte.core.frame import Frame
from featurebyte.enum import DBVarType
from featurebyte.models import DimensionTableModel, EntityModel
from featurebyte.models.column_statistics import (
    ColumnStatisticsInfo,
    ColumnStatisticsModel,
    StatisticsModel,
)
from featurebyte.models.parent_serving import (
    EntityLookupStepCreator,
    FeatureNodeRelationshipsInfo,
    ParentServingPreparation,
)
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import construct_node
from featurebyte.query_graph.node.metadata.operation import NodeOutputCategory, OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import NodeCodeGenOutput
from featurebyte.query_graph.node.schema import FeatureStoreDetails, SnowflakeDetails, TableDetails
from tests.util.helper import add_groupby_operation, reset_global_graph


@pytest.fixture(name="global_graph")
def global_query_graph():
    """
    Empty query graph fixture
    """
    yield reset_global_graph()


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
                    "database_name": "db",
                    "schema_name": "public",
                    "role_name": "role",
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
                    "host": "databricks-hostname",
                    "http_path": "databricks-http-path",
                    "schema_name": "public",
                    "catalog_name": "hive_metastore",
                },
            },
        }
    return input_details


@pytest.fixture(name="node_code_gen_output_factory")
def node_code_gen_output_factory_fixture():
    """
    Fixture for node code generation output
    """

    def _generate_node_code_gen_output(var_name_or_expr):
        """
        Generate node code generation output
        """
        return NodeCodeGenOutput(
            var_name_or_expr=var_name_or_expr,
            operation_structure=OperationStructure(
                output_type=NodeOutputType.SERIES,
                output_category=NodeOutputCategory.VIEW,
                row_index_lineage=tuple(),
            ),
        )

    return _generate_node_code_gen_output


@pytest.fixture(name="snowflake_feature_store_details")
def snowflake_feature_store_details_fixture():
    """
    Fixture for a FeatureStoreDetails object
    """
    return FeatureStoreDetails(**{
        "type": "snowflake",
        "details": {
            "database_name": "db",
            "schema_name": "public",
            "role_name": "role",
            "account": "account",
            "warehouse": "warehouse",
        },
    })


@pytest.fixture(name="database_details")
def database_details_fixture(input_details):
    """
    Fixture for database details
    """
    database_details = input_details["feature_store_details"]["details"]
    return SnowflakeDetails(**database_details)


@pytest.fixture(name="entity_id")
def entity_id_fixture():
    """
    Fixture of an entity_id
    """
    return ObjectId("63dbe68cd918ef71acffd127")


@pytest.fixture(name="input_node")
def input_node_fixture(global_graph, input_details):
    """Fixture of a query with some operations ready to run groupby"""

    node_params = {
        "type": "event_table",
        "columns": [
            {"name": "ts", "dtype": DBVarType.TIMESTAMP},
            {"name": "cust_id", "dtype": DBVarType.INT},
            {"name": "biz_id", "dtype": DBVarType.INT},
            {"name": "product_type", "dtype": DBVarType.INT},
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
    """Similar to input_details but for an SCDView table"""
    input_details = copy.deepcopy(input_details)
    input_details["table_details"]["table_name"] = "customer_profile_table"
    return input_details


@pytest.fixture(name="scd_table_input_node")
def scd_table_input_node_fixture(global_graph, scd_table_input_details):
    """Fixture of an SCDView input node"""
    node_params = {
        "type": "scd_table",
        "columns": [
            {"name": "effective_ts", "dtype": DBVarType.TIMESTAMP},
            {"name": "cust_id", "dtype": DBVarType.INT},
            {"name": "membership_status", "dtype": DBVarType.VARCHAR},
        ],
        "effective_timestamp_column": "effective_ts",
        "current_flag_column": "is_record_current",
        "id": ObjectId("66c372f39da9ad8e66c1eec6"),
    }
    node_params.update(scd_table_input_details)
    node_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    return node_input


@pytest.fixture(name="scd_table_input_node_with_tz")
def scd_table_input_node_with_tz_fixture(global_graph, scd_table_input_details):
    """Fixture of an SCDView input node"""
    node_params = {
        "type": "scd_table",
        "columns": [
            {
                "name": "effective_ts",
                "dtype": DBVarType.TIMESTAMP,
                "dtype_metadata": {
                    "timestamp_schema": {
                        "format_string": None,
                        "is_utc_time": None,
                        "timezone": {"column_name": "timezone", "type": "timezone"},
                    }
                },
            },
            {"name": "cust_id", "dtype": DBVarType.INT},
            {"name": "timezone", "dtype": DBVarType.VARCHAR},
            {"name": "membership_status", "dtype": DBVarType.VARCHAR},
        ],
        "effective_timestamp_column": "effective_ts",
        "current_flag_column": "is_record_current",
        "id": ObjectId("66c372f39da9ad8e66c1eec6"),
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


@pytest.fixture(name="event_table_input_node_parameters")
def event_table_input_node_parameters_fixture(global_graph, input_details):
    """Fixture for EventTable input node parameters"""
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
    return node_params


@pytest.fixture(name="event_table_input_node")
def event_table_input_node_fixture(global_graph, event_table_input_node_parameters):
    """Fixture of an EventTable input node"""
    node_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=event_table_input_node_parameters,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    return node_input


@pytest.fixture(name="event_table_id")
def event_table_id_fixture():
    """Fixture for EventTable id"""
    return ObjectId("66c335fd9da9ad8e66c1eec5")


@pytest.fixture(name="event_table_input_node_with_id")
def event_table_input_node_with_id_fixture(
    global_graph, event_table_input_node_parameters, event_table_id
):
    """Fixture of an EventTable input node with id"""
    event_table_input_node_parameters["id"] = event_table_id
    node_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=event_table_input_node_parameters,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    return node_input


@pytest.fixture(name="time_series_table_input_node")
def time_series_table_input_node_fixture(global_graph, input_details):
    """Fixture of an input node for a time series table"""
    input_details = copy.deepcopy(input_details)
    input_details["table_details"]["table_name"] = "customer_snapshot"
    node_params = {
        "type": "time_series_table",
        "columns": [
            {
                "name": "snapshot_date",
                "dtype": DBVarType.VARCHAR,
                "dtype_metadata": {"timestamp_schema": {"format_string": "YYYYMMDD"}},
            },
            {"name": "cust_id", "dtype": DBVarType.INT},
            {"name": "a", "dtype": DBVarType.FLOAT},
        ],
        "reference_datetime_column": "snapshot_date",
        "reference_datetime_schema": {"timestamp_schema": {"format_string", "YYYYMMDD"}},
        "time_interval": {"unit": "DAY", "value": 1},
        "id": ObjectId("67643eeab0f7b5c9c7683e46"),
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


@pytest.fixture(name="customer_entity_id")
def customer_entity_id_fixture():
    return ObjectId("637516ebc9c18f5a277a78db")


@pytest.fixture(name="customer_entity")
def customer_entity_fixture(customer_entity_id):
    return EntityModel(
        _id=customer_entity_id,
        name="customer",
        serving_names=["CUSTOMER_ID"],
    )


@pytest.fixture(name="groupby_node_params")
def groupby_node_params_fixture(customer_entity_id):
    """Fixture groupby node parameters"""
    node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": None,
        "parent": "a",
        "agg_func": "avg",
        "feature_job_setting": {
            "offset": "1800s",  # 30m
            "period": "3600s",  # 1h
            "blind_spot": "900s",  # 15m
        },
        "timestamp": "ts",
        "names": ["a_2h_average", "a_48h_average"],
        "windows": ["2h", "48h"],
        "entity_ids": [customer_entity_id],
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
        "feature_job_setting": {
            "offset": "1800s",  # 30m
            "period": "3600s",  # 1h
            "blind_spot": "900s",  # 15m
        },
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
        "feature_job_setting": {
            "offset": "1800s",  # 30m
            "period": "3600s",  # 1h
            "blind_spot": "900s",  # 15m
        },
        "timestamp": "ts",
        "names": ["a_2h_sum", "a_36h_sum"],
        "windows": ["2h", "36h"],
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db")],
    }
    return node_params


@pytest.fixture(name="groupby_node_params_different_feature_job_settings")
def groupby_node_params_different_feature_job_settings_fixture():
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
        "feature_job_setting": {
            "offset": "900s",  # 15m
            "period": "3600s",  # 1h
            "blind_spot": "900s",  # 15m
        },
        "timestamp": "ts",
        "names": ["a_2h_sum_v2", "a_36h_sum_v2"],
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


@pytest.fixture(name="query_graph_with_forward_aggregate")
def query_graph_with_forward_aggregate_fixture(query_graph_and_assign_node, event_table_details):
    """Fixture of a query graph with a forward aggregate operation"""
    graph, assign_node = query_graph_and_assign_node
    graph.add_operation(
        node_type=NodeType.FORWARD_AGGREGATE,
        node_params={
            "name": "biz_id_sum_7d",
            "window": "7d",
            "table_details": event_table_details,
            "timestamp_col": "timestamp_col",
            "keys": ["biz_id"],
            "agg_func": "sum",
            "serving_names": ["BUSINESS_ID"],
            "parent": "a",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node],
    )
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
    assert aggregation_id == "13c45b8622761dd28afb4640ac3ed355d57d789f"
    return aggregation_id


@pytest.fixture(name="query_graph_with_category_groupby")
def query_graph_with_category_groupby_fixture(query_graph_and_assign_node, groupby_node_params):
    """Fixture of a query graph with a groupby operation"""
    graph, assign_node = query_graph_and_assign_node
    node_params = groupby_node_params
    node_params["value_by"] = "product_type"
    add_groupby_operation(graph, node_params, assign_node)
    return graph


@pytest.fixture(name="query_graph_with_category_groupby_multiple")
def query_graph_with_category_groupby_multiple_fixture(
    query_graph_and_assign_node, groupby_node_params
):
    """Fixture of a query graph with a groupby operation"""
    graph, assign_node = query_graph_and_assign_node

    def _add_feature(params):
        node_params = copy.deepcopy(groupby_node_params)
        node_params["value_by"] = "product_type"
        node_params.update(params)
        groupby_node = add_groupby_operation(graph, node_params, assign_node)
        feature_proj = graph.add_operation(
            node_type=NodeType.PROJECT,
            node_params={"columns": [params["names"][0]]},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[groupby_node],
        )
        feature = graph.add_operation(
            node_type=NodeType.COUNT_DICT_TRANSFORM,
            node_params={"transform_type": "entropy"},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[feature_proj],
        )
        return feature

    feature_node_1 = _add_feature({
        "parent": "a",
        "agg_func": "max",
        "names": ["a_2h_max_by_product_type"],
    })
    feature_node_2 = _add_feature({
        "parent": "a",
        "agg_func": "min",
        "names": ["a_2h_min_by_product_type"],
    })
    complex_feature_node = graph.add_operation(
        node_type=NodeType.DIV,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[feature_node_1, feature_node_2],
    )
    complex_feature_node_alias = graph.add_operation(
        node_type=NodeType.ALIAS,
        node_params={"name": "complex_cross_aggregate"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[complex_feature_node],
    )
    return graph, complex_feature_node_alias


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


@pytest.fixture(name="query_graph_with_different_groupby_nodes")
def query_graph_with_different_groupby_nodes_fixture(
    query_graph_and_assign_node,
    groupby_node_params,
    groupby_node_params_different_feature_job_settings,
):
    """
    Fixture of a query graph with two different groupby nodes (different job settings)
    """
    graph, assign_node = query_graph_and_assign_node
    node1 = add_groupby_operation(
        graph,
        groupby_node_params,
        assign_node,
        override_tile_id="tile_id_1",
        override_aggregation_id="agg_id_2",
    )
    node2 = add_groupby_operation(
        graph,
        groupby_node_params_different_feature_job_settings,
        assign_node,
        override_tile_id="tile_id_2",
        override_aggregation_id="agg_id_1",
    )
    return [node1, node2], graph


@pytest.fixture(name="business_entity_id")
def business_entity_id_fixture():
    return ObjectId("6375171ac9c18f5a277a78dc")


@pytest.fixture(name="business_entity")
def business_entity(business_entity_id):
    return EntityModel(
        _id=business_entity_id,
        name="business",
        serving_names=["BUSINESS_ID"],
    )


@pytest.fixture(name="complex_feature_query_graph")
def complex_feature_query_graph_fixture(query_graph_with_groupby, business_entity_id):
    """Fixture of a query graph with two independent groupby operations"""
    graph = query_graph_with_groupby
    node_params = {
        "keys": ["biz_id"],
        "value_by": None,
        "parent": "a",
        "agg_func": "sum",
        "feature_job_setting": {
            "offset": "1800s",  # 30m
            "period": "3600s",  # 1h
            "blind_spot": "900s",  # 15m
        },
        "timestamp": "ts",
        "names": ["a_7d_sum_by_business"],
        "windows": ["7d"],
        "serving_names": ["BUSINESS_ID"],
        "entity_ids": [business_entity_id],
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


@pytest.fixture(name="relation_table_id")
def relation_table_id_fixture():
    return ObjectId("00000000000000000000000a")


@pytest.fixture(name="relation_table")
def relation_table_fixture(
    relation_table_id, snowflake_feature_store, customer_entity_id, business_entity_id
):
    return DimensionTableModel(
        _id=relation_table_id,
        dimension_id_column="relation_cust_id",
        columns_info=[
            {"name": "relation_cust_id", "dtype": DBVarType.INT, "entity_id": customer_entity_id},
            {"name": "relation_biz_id", "dtype": DBVarType.INT, "entity_id": business_entity_id},
        ],
        tabular_source=TabularSource(**{
            "feature_store_id": snowflake_feature_store.id,
            "table_details": {
                "database_name": "db",
                "schema_name": "public",
                "table_name": "some_table_name",
            },
        }),
    )


@pytest.fixture(name="feature_node_relationships_info_business_is_parent_of_user")
def feature_node_relationships_info_business_is_parent_of_user_fixture(
    complex_feature_query_graph, customer_entity_id, business_entity_id, relation_table_id
):
    """
    Fixture for a FeatureNodeRelationshipsInfo that defines parent child relationship between
    business and user entities. To be used together with complex_feature_query_graph.
    """
    feature_node, _ = complex_feature_query_graph
    return FeatureNodeRelationshipsInfo(
        node_name=feature_node.name,
        relationships_info=[
            EntityRelationshipInfo(
                _id=ObjectId("100000000000000000000000"),
                relationship_type="child_parent",
                entity_id=customer_entity_id,
                related_entity_id=business_entity_id,
                relation_table_id=relation_table_id,
            )
        ],
        primary_entity_ids=[customer_entity_id],
    )


@pytest.fixture(name="entity_lookup_step_creator")
def entity_lookup_step_creator_fixture(
    feature_node_relationships_info_business_is_parent_of_user,
    customer_entity,
    business_entity,
    relation_table,
):
    """
    Fixture for an EntityLookupStepCreator object
    """
    relationships = feature_node_relationships_info_business_is_parent_of_user.relationships_info[:]
    return EntityLookupStepCreator(
        entity_relationships_info=relationships,
        entities_by_id={customer_entity.id: customer_entity, business_entity.id: business_entity},
        tables_by_id={relation_table.id: relation_table},
    )


@pytest.fixture(name="join_node_params")
def join_node_params_fixture():
    """Join node parameters fixture"""
    return {
        "left_on": "order_id",
        "right_on": "order_id",
        "left_input_columns": ["order_method", "cust_id", "ts"],
        "left_output_columns": ["order_method", "cust_id", "ts"],
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
        "feature_job_setting": {
            "offset": "1800s",  # 30m
            "period": "3600s",  # 1h
            "blind_spot": "900s",  # 15m
        },
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
        "feature_job_setting": {
            "offset": "1800s",  # 30m
            "period": "3600s",  # 1h
            "blind_spot": "900s",  # 15m
        },
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
    event_table_input_node_with_id,
    scd_table_input_node,
):
    """
    Fixture of a join node that performs an SCD join between EventTable and SCDTable
    """
    node_params = {
        "left_on": "cust_id",
        "right_on": "cust_id",
        "left_input_columns": ["ts", "cust_id", "order_id", "order_method"],
        "left_output_columns": [
            "ts",
            "cust_id",
            "order_id",
            "order_method",
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
        input_nodes=[event_table_input_node_with_id, scd_table_input_node],
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


@pytest.fixture(name="window_aggregate_on_simple_view_feature_node")
def window_aggregate_on_simple_view_feature_node_fixture(
    global_graph, event_table_input_node_with_id
):
    """
    Fixture of a window aggregate feature on a simple event view without any joins
    """
    node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": None,
        "parent": None,
        "agg_func": "count",
        "feature_job_setting": {
            "offset": "1800s",  # 30m
            "period": "3600s",  # 1h
            "blind_spot": "900s",  # 15m
        },
        "timestamp": "ts",
        "names": ["order_count_90d"],
        "windows": ["90d"],
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db")],
    }
    groupby_node = add_groupby_operation(global_graph, node_params, event_table_input_node_with_id)
    feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["order_count_90d"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(groupby_node.name)],
    )
    return feature_node


@pytest.fixture(name="window_aggregate_on_view_with_scd_join_feature_node")
def window_aggregate_on_view_with_scd_join_feature_node_fixture(global_graph, scd_join_node):
    """
    Fixture of a window aggregate feature on a view with SCD join
    """
    node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": None,
        "parent": "latest_membership_status",
        "agg_func": "na_count",
        "feature_job_setting": {
            "offset": "1800s",  # 30m
            "period": "3600s",  # 1h
            "blind_spot": "900s",  # 15m
        },
        "timestamp": "ts",
        "names": ["latest_membership_status_na_count_90d"],
        "windows": ["90d"],
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db")],
    }
    groupby_node = add_groupby_operation(global_graph, node_params, scd_join_node)
    feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["latest_membership_status_na_count_90d"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(groupby_node.name)],
    )
    return feature_node


@pytest.fixture(name="complex_composite_window_aggregate_on_view_with_scd_join_feature_node")
def complex_composite_window_aggregate_on_view_with_scd_join_feature_node_fixture(
    global_graph, scd_join_node
):
    """
    Fixture of a window aggregate feature on a view with SCD join with composite groupby keys
    """
    node_params = {
        "keys": ["cust_id", "latest_membership_status"],
        "serving_names": ["CUSTOMER_ID", "MEMBERSHIP_STATUS"],
        "value_by": None,
        "parent": "latest_membership_status",
        "agg_func": "na_count",
        "feature_job_setting": {
            "offset": "1800s",  # 30m
            "period": "3600s",  # 1h
            "blind_spot": "900s",  # 15m
        },
        "timestamp": "ts",
        "names": ["latest_membership_status_na_count_90d"],
        "windows": ["90d"],
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db"), ObjectId("66c40c289da9ad8e66c1eec7")],
    }
    groupby_node = add_groupby_operation(global_graph, node_params, scd_join_node)
    feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["latest_membership_status_na_count_90d"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(groupby_node.name)],
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
        "feature_job_setting": {
            "offset": "1800s",  # 30m
            "period": "3600s",  # 1h
            "blind_spot": "900s",  # 15m
        },
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


@pytest.fixture(name="latest_value_groupby_node_parameters")
def latest_value_groupby_node_parameters_fixture():
    """
    Fixture for latest value groupby node parameters
    """
    return {
        "keys": ["cust_id", "biz_id"],
        "serving_names": ["CUSTOMER_ID", "BUSINESS_ID"],
        "value_by": None,
        "parent": "a",
        "agg_func": "latest",
        "feature_job_setting": {
            "offset": "1800s",  # 30m
            "period": "3600s",  # 1h
            "blind_spot": "900s",  # 15m
        },
        "timestamp": "ts",
        "names": ["a_latest_value"],
        "windows": [None],
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db"), ObjectId("637516ebc9c18f5a277a78dc")],
    }


@pytest.fixture(name="latest_value_without_window_feature_node")
def latest_value_without_window_feature_node_fixture(
    global_graph, input_node, latest_value_groupby_node_parameters
):
    groupby_node = add_groupby_operation(
        global_graph, latest_value_groupby_node_parameters, input_node
    )
    feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_latest_value"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(groupby_node.name)],
    )
    return feature_node


@pytest.fixture(name="latest_value_offset_without_window_feature_node")
def latest_value_offset_without_window_feature_node_fixture(
    global_graph, input_node, latest_value_groupby_node_parameters
):
    node_params = copy.deepcopy(latest_value_groupby_node_parameters)
    node_params.update({"offset": "48h", "names": ["a_latest_value_offset_48h"]})
    groupby_node = add_groupby_operation(global_graph, node_params, input_node)
    feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_latest_value_offset_48h"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(groupby_node.name)],
    )
    return feature_node


@pytest.fixture(name="window_aggregate_with_offset_feature_node")
def window_aggregate_with_offset_feature_node(global_graph, input_node):
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
        "names": ["a_sum_24h_offset_8h"],
        "windows": ["24h"],
        "offset": "8h",
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db")],
    }
    groupby_node = add_groupby_operation(global_graph, node_params, input_node)
    feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_sum_24h_offset_8h"]},
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


@pytest.fixture(name="aggregate_asat_with_offset_feature_node")
def aggregate_asat_with_offset_feature_node_fixture(global_graph, scd_table_input_node):
    node_params = {
        "keys": ["membership_status"],
        "serving_names": ["MEMBERSHIP_STATUS"],
        "value_by": None,
        "parent": None,
        "agg_func": "count",
        "name": "asat_feature_offset_7d",
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db")],
        "effective_timestamp_column": "effective_ts",
        "natural_key_column": "cust_id",
        "offset": "7d",
    }
    aggregate_asat_node = global_graph.add_operation(
        node_type=NodeType.AGGREGATE_AS_AT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[scd_table_input_node],
    )
    feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["asat_feature_offset_7d"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(aggregate_asat_node.name)],
    )
    return feature_node


@pytest.fixture(name="time_since_last_event_feature_node")
def time_since_last_event_feature_node_fixture(global_graph, input_node):
    node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": None,
        "parent": "ts",
        "agg_func": "latest",
        "feature_job_setting": {
            "offset": "1800s",  # 30m
            "period": "3600s",  # 1h
            "blind_spot": "900s",  # 15m
        },
        "timestamp": "ts",
        "names": ["latest_event_timestamp_90d"],
        "windows": ["90d"],
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db")],
    }
    groupby_node = add_groupby_operation(global_graph, node_params, input_node)
    latest_timestamp_feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["latest_event_timestamp_90d"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(groupby_node.name)],
    )
    request_point_in_time_node = global_graph.add_operation(
        node_type=NodeType.REQUEST_COLUMN,
        node_params={"column_name": "POINT_IN_TIME", "dtype": DBVarType.TIMESTAMP},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[],
    )
    time_since_last_event_feature_node = global_graph.add_operation(
        node_type=NodeType.DATE_DIFF,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[latest_timestamp_feature_node, request_point_in_time_node],
    )
    time_since_last_event_feature_node = global_graph.add_operation(
        node_type=NodeType.ALIAS,
        node_params={"name": "time_since_last_event"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[time_since_last_event_feature_node],
    )
    return time_since_last_event_feature_node


@pytest.fixture(name="non_tile_window_aggregate_feature_node")
def non_tile_window_aggregation_feature_node_fixture(global_graph, input_node):
    """
    Fixture for a non-tile window aggregate feature node
    """
    node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": None,
        "parent": "a",
        "agg_func": "sum",
        "feature_job_setting": {
            "offset": "1800s",  # 30m
            "period": "12h",  # 12h
            "blind_spot": "900s",  # 15m
        },
        "timestamp": "ts",
        "names": ["a_48h_sum_no_tile"],
        "windows": ["48h"],
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db")],
    }
    aggregate_node = global_graph.add_operation(
        node_type=NodeType.NON_TILE_WINDOW_AGGREGATE,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node],
    )
    feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_48h_sum_no_tile"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(aggregate_node.name)],
    )
    return feature_node


@pytest.fixture(name="non_tile_window_aggregate_complex_feature_node")
def non_tile_window_aggregation_complex_feature_node_fixture(global_graph, input_node):
    """
    Fixture for a complex non-tile window aggregate feature node with multiple windows
    """
    node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": None,
        "parent": "a",
        "agg_func": "sum",
        "feature_job_setting": {
            "offset": "1800s",  # 30m
            "period": "12h",  # 12h
            "blind_spot": "900s",  # 15m
        },
        "timestamp": "ts",
        "names": ["a_2h_sum_no_tile", "a_48h_sum_no_tile"],
        "windows": ["2h", "48h"],
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db")],
    }
    aggregate_node = global_graph.add_operation(
        node_type=NodeType.NON_TILE_WINDOW_AGGREGATE,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node],
    )
    temp_feature_1 = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_2h_sum_no_tile"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(aggregate_node.name)],
    )
    temp_feature_2 = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_48h_sum_no_tile"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(aggregate_node.name)],
    )
    div_feature = global_graph.add_operation(
        node_type=NodeType.DIV,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[temp_feature_1, temp_feature_2],
    )
    feature_node = global_graph.add_operation(
        node_type=NodeType.ALIAS,
        node_params={"name": "a_2h_48h_sum_ratio_no_tile"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[div_feature],
    )
    return feature_node


@pytest.fixture(name="time_series_window_aggregate_feature_node")
def time_series_window_aggregate_feature_node_fixture(global_graph, time_series_table_input_node):
    """
    Fixture for a time series window aggregate feature node
    """
    node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": None,
        "parent": "a",
        "agg_func": "sum",
        "feature_job_setting": {
            "crontab": "0 0 * * *",  # daily at midnight
            "timezone": "Etc/UTC",
            "reference_timezone": "Asia/Singapore",
        },
        "names": ["a_7d_sum"],
        "windows": [{"unit": "DAY", "size": 7}],
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db")],
        "reference_datetime_column": "snapshot_date",
        "reference_datetime_metadata": {
            "timestamp_schema": {
                "format_string": "YYYYMMDD",
                "timezone": "Asia/Singapore",
            },
        },
        "time_interval": {"unit": "DAY", "value": 1},
    }
    aggregate_node = global_graph.add_operation(
        node_type=NodeType.TIME_SERIES_WINDOW_AGGREGATE,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[time_series_table_input_node],
    )
    feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_7d_sum"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(aggregate_node.name)],
    )
    return feature_node


@pytest.fixture(name="time_series_window_aggregate_with_blind_spot_feature_node")
def time_series_window_aggregate_with_blind_spot_feature_node_fixture(
    global_graph, time_series_table_input_node
):
    """
    Fixture for a time series window aggregate feature node with a cron feature job setting with
    blind spot and all-string crontab fields (otherwise equivalent)
    """
    node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": None,
        "parent": "a",
        "agg_func": "sum",
        "feature_job_setting": {
            "crontab": Crontab(
                minute="0", hour="0", day_of_month="*", month_of_year="*", day_of_week="*"
            ),  # daily at midnight
            "timezone": "Etc/UTC",
            "reference_timezone": "Asia/Singapore",
            "blind_spot": "3d",
        },
        "names": ["a_7d_sum_bs3d"],
        "windows": [{"unit": "DAY", "size": 7}],
        "entity_ids": [ObjectId("637516ebc9c18f5a277a78db")],
        "reference_datetime_column": "snapshot_date",
        "reference_datetime_metadata": {
            "timestamp_schema": {
                "format_string": "YYYYMMDD",
                "timezone": "Asia/Singapore",
            },
        },
        "time_interval": {"unit": "DAY", "value": 1},
    }
    aggregate_node = global_graph.add_operation(
        node_type=NodeType.TIME_SERIES_WINDOW_AGGREGATE,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[time_series_table_input_node],
    )
    feature_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_7d_sum_bs3d"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(aggregate_node.name)],
    )
    return feature_node


@pytest.fixture(name="column_statistics_info")
def column_statistics_info_fixture(time_series_table_input_node):
    """
    Fixture of ColumnStatisticsInfo
    """
    return ColumnStatisticsInfo(
        all_column_statistics={
            time_series_table_input_node.parameters.id: {
                time_series_table_input_node.parameters.reference_datetime_column: ColumnStatisticsModel(
                    table_id=time_series_table_input_node.parameters.id,
                    column_name=time_series_table_input_node.parameters.reference_datetime_column,
                    stats=StatisticsModel(
                        distinct_count=100,
                    ),
                )
            }
        }
    )


@pytest.fixture(name="feature_nodes_all_types")
def feature_nodes_all_types_fixture(
    mixed_point_in_time_and_item_aggregations,
    lookup_feature_node,
    scd_lookup_feature_node,
    latest_value_aggregation_feature_node,
    latest_value_without_window_feature_node,
    aggregate_asat_feature_node,
):
    """
    Fixture for feature query graph nodes of multiple types
    """
    _, groupby_node, item_groupby_feature_node = mixed_point_in_time_and_item_aggregations
    nodes = [
        groupby_node,
        item_groupby_feature_node,
        lookup_feature_node,
        scd_lookup_feature_node,
        latest_value_aggregation_feature_node,
        latest_value_without_window_feature_node,
        aggregate_asat_feature_node,
    ]
    return nodes


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
            "table_details": event_table_details.model_dump(),
            "feature_store_details": snowflake_feature_store_details_dict,
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    pruned_graph, node_name_map = global_graph.prune(target_node=node_input)
    mapped_node = pruned_graph.get_node_by_name(node_name_map[node_input.name])
    assert mapped_node.name == "input_1"
    graph_dict = global_graph.model_dump()
    assert graph_dict == pruned_graph.model_dump()
    assert graph_dict["nodes"] == [
        {
            "name": "input_1",
            "type": "input",
            "parameters": {
                "type": "event_table",
                "columns": [{"name": "column", "dtype": "FLOAT", "dtype_metadata": None}],
                "table_details": event_table_details.model_dump(),
                "feature_store_details": snowflake_feature_store_details_dict,
                "timestamp_column": None,
                "id": None,
                "id_column": None,
                "event_timestamp_timezone_offset": None,
                "event_timestamp_timezone_offset_column": None,
                "event_timestamp_schema": None,
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

    pruned_graph, node_name_map = graph.prune(target_node=node_proj)
    mapped_node = pruned_graph.get_node_by_name(node_name_map[node_proj.name])
    assert mapped_node.name == "project_1"
    graph_dict = graph.model_dump()
    assert graph_dict == pruned_graph.model_dump()
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
    pruned_graph, node_name_map = graph.prune(target_node=node_eq)
    mapped_node = pruned_graph.get_node_by_name(node_name_map[node_eq.name])
    assert mapped_node.name == "eq_1"
    graph_dict = graph.model_dump()
    assert graph_dict == pruned_graph.model_dump()
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
    pruned_graph, node_name_map = graph.prune(target_node=node_filter)
    mapped_node = pruned_graph.get_node_by_name(node_name_map[node_filter.name])
    assert mapped_node.name == "filter_1"
    graph_dict = graph.model_dump()
    assert graph_dict == pruned_graph.model_dump()
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
                    "database_name": "db",
                    "schema_name": "public",
                    "role_name": "role",
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


@pytest.fixture(name="query_graph_with_lag_node")
def query_graph_with_lag_node_fixture(global_graph, input_node):
    """Fixture of a query graph with a lag operation"""
    project_map = {}
    for col in ["cust_id", "a", "ts"]:
        project_map[col] = global_graph.add_operation(
            node_type=NodeType.PROJECT,
            node_params={"columns": [col]},
            node_output_type="series",
            input_nodes=[input_node],
        )

    lag_node = global_graph.add_operation(
        node_type=NodeType.LAG,
        node_params={
            "entity_columns": ["cust_id"],
            "timestamp_column": "ts",
            "offset": 1,
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[project_map["a"], project_map["cust_id"], project_map["ts"]],
    )
    return global_graph, lag_node


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
                "id": ObjectId(),
                "table": data_model,
                "parent": {
                    "key": "col_int",
                    "serving_name": "COL_INT",
                    "entity_id": ObjectId(),
                },
                "child": {
                    "key": "col_text",
                    "serving_name": "COL_TEXT",
                    "entity_id": ObjectId(),
                },
            }
        ],
        feature_store_details=feature_store_details,
    )
    return parent_serving_preparation


@pytest.fixture
def expected_pruned_graph_and_node_1(groupby_node_aggregation_id):
    """
    Fixture for the expected pruned graph and node
    """
    graph = QueryGraphModel(**{
        "edges": [
            {"source": "input_1", "target": "groupby_1"},
            {"source": "groupby_1", "target": "project_1"},
        ],
        "nodes": [
            {
                "name": "input_1",
                "type": "input",
                "output_type": "frame",
                "parameters": {
                    "columns": [
                        {"name": "ts", "dtype": "TIMESTAMP"},
                        {"name": "cust_id", "dtype": "INT"},
                        {"name": "biz_id", "dtype": "INT"},
                        {"name": "product_type", "dtype": "INT"},
                        {"name": "a", "dtype": "FLOAT"},
                        {"name": "b", "dtype": "FLOAT"},
                    ],
                    "table_details": {
                        "database_name": "db",
                        "schema_name": "public",
                        "table_name": "event_table",
                    },
                    "feature_store_details": {
                        "type": "snowflake",
                        "details": {
                            "account": "account",
                            "warehouse": "warehouse",
                            "database_name": "db",
                            "schema_name": "public",
                            "role_name": "role",
                        },
                    },
                    "type": "event_table",
                    "id": None,
                    "timestamp_column": "ts",
                    "id_column": None,
                    "event_timestamp_timezone_offset": None,
                    "event_timestamp_timezone_offset_column": None,
                },
            },
            {
                "name": "groupby_1",
                "type": "groupby",
                "output_type": "frame",
                "parameters": {
                    "keys": ["cust_id"],
                    "parent": "a",
                    "agg_func": "avg",
                    "value_by": None,
                    "serving_names": ["CUSTOMER_ID"],
                    "entity_ids": ["637516ebc9c18f5a277a78db"],
                    "windows": ["2h"],
                    "timestamp": "ts",
                    "feature_job_setting": {
                        "blind_spot": "900s",
                        "offset": "1800s",
                        "period": "3600s",
                    },
                    "names": ["a_2h_average"],
                    "tile_id": "TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
                    "aggregation_id": f"avg_{groupby_node_aggregation_id}",
                },
            },
            {
                "name": "project_1",
                "type": "project",
                "output_type": "series",
                "parameters": {"columns": ["a_2h_average"]},
            },
        ],
    })
    node = construct_node(**{
        "name": "project_1",
        "type": "project",
        "output_type": "series",
        "parameters": {"columns": ["a_2h_average"]},
    })
    return {
        "pruned_graph": graph,
        "pruned_node": node,
    }


@pytest.fixture
def expected_pruned_graph_and_node_2(groupby_node_aggregation_id):
    graph = QueryGraphModel(**{
        "edges": [
            {"source": "input_1", "target": "groupby_1"},
            {"source": "groupby_1", "target": "project_1"},
        ],
        "nodes": [
            {
                "name": "input_1",
                "type": "input",
                "output_type": "frame",
                "parameters": {
                    "columns": [
                        {"name": "ts", "dtype": "TIMESTAMP"},
                        {"name": "cust_id", "dtype": "INT"},
                        {"name": "biz_id", "dtype": "INT"},
                        {"name": "product_type", "dtype": "INT"},
                        {"name": "a", "dtype": "FLOAT"},
                        {"name": "b", "dtype": "FLOAT"},
                    ],
                    "table_details": {
                        "database_name": "db",
                        "schema_name": "public",
                        "table_name": "event_table",
                    },
                    "feature_store_details": {
                        "type": "snowflake",
                        "details": {
                            "account": "account",
                            "warehouse": "warehouse",
                            "database_name": "db",
                            "schema_name": "public",
                            "role_name": "role",
                        },
                    },
                    "type": "event_table",
                    "id": None,
                    "timestamp_column": "ts",
                    "id_column": None,
                    "event_timestamp_timezone_offset": None,
                    "event_timestamp_timezone_offset_column": None,
                },
            },
            {
                "name": "groupby_1",
                "type": "groupby",
                "output_type": "frame",
                "parameters": {
                    "keys": ["cust_id"],
                    "parent": "a",
                    "agg_func": "avg",
                    "value_by": None,
                    "serving_names": ["CUSTOMER_ID"],
                    "entity_ids": ["637516ebc9c18f5a277a78db"],
                    "windows": ["48h"],
                    "timestamp": "ts",
                    "feature_job_setting": {
                        "blind_spot": "900s",
                        "offset": "1800s",
                        "period": "3600s",
                    },
                    "names": ["a_48h_average"],
                    "tile_id": "TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
                    "aggregation_id": f"avg_{groupby_node_aggregation_id}",
                },
            },
            {
                "name": "project_1",
                "type": "project",
                "output_type": "series",
                "parameters": {"columns": ["a_48h_average"]},
            },
        ],
    })
    node = construct_node(**{
        "name": "project_1",
        "type": "project",
        "output_type": "series",
        "parameters": {"columns": ["a_48h_average"]},
    })
    return {
        "pruned_graph": graph,
        "pruned_node": node,
    }
