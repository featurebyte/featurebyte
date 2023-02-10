"""
This module contains tests for SDKCodeExtractor class.
"""
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.generic import InputNode
from featurebyte.query_graph.transform.sdk_code import SDKCodeExtractor


def _create_input_node(node_type, data_id):
    node_params = {
        "name": "input_1",
        "parameters": {
            "type": node_type,
            "id": data_id,
            "columns": [
                {"name": "ts", "dtype": DBVarType.TIMESTAMP},
                {"name": "cust_id", "dtype": DBVarType.INT},
                {"name": "a", "dtype": DBVarType.FLOAT},
                {"name": "b", "dtype": DBVarType.FLOAT},
            ],
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
        },
    }
    return InputNode(**node_params)


def _generate_sdk_output(graph, node):
    """Generate SDK output based on given graph & node"""
    pruned_graph, node_name_map = graph.prune(target_node=node, aggressive=True)
    sdk_code_output = SDKCodeExtractor(graph=pruned_graph).extract(
        node=pruned_graph.get_node_by_name(node_name_map[node.name])
    )
    return sdk_code_output.code_generator.generate()


def test_project_node__extract_sdk_codes(mixed_point_in_time_and_item_aggregations):
    """Test project node SDK codes generation logic"""
    graph, groupby_node, item_groupby_feature_node = mixed_point_in_time_and_item_aggregations
    for assign_node in graph.iterate_nodes(target_node=groupby_node, node_type=NodeType.ASSIGN):
        proj_assign_node = graph.add_operation(
            node_type=NodeType.PROJECT,
            node_params={"columns": [assign_node.parameters.name]},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[assign_node],
        )
        sdk_output = _generate_sdk_output(graph, proj_assign_node)
        assert sdk_output == (
            "event_data = EventData(...)\n"
            "event_view = EventView.from_event_data(event_data)\n"
            "event_view['c'] = event_view['a'] + event_view['b']\n"
            "output = event_view['c']"
        )
        break

    proj_groupby_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_2h_average"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[groupby_node],
    )
    sdk_output = _generate_sdk_output(graph, proj_groupby_node)
    assert sdk_output == (
        "event_data = EventData(...)\n"
        "event_view = EventView.from_event_data(event_data)\n"
        "grouped = event_view.groupby(by_keys=['cust_id'], category=None).aggregate_over("
        "value_column='a', method='avg', windows=['2h'], feature_names=['a_2h_average'], "
        "feature_job_setting={'blind_spot': '900s', 'frequency': '3600s', 'time_modulo_frequency': '1800s'})\n"
        "output = grouped['a_2h_average']"
    )

    sdk_output = _generate_sdk_output(graph, item_groupby_feature_node)
    assert sdk_output == (
        "item_data = ItemData(...)\n"
        "item_view = ItemView.from_item_data(item_data)\n"
        "feat = item_view.groupby(by_keys=['order_id'], category=None).aggregate("
        "value_column=None, method='count', feature_name='order_size')\n"
        "output = feat"
    )
