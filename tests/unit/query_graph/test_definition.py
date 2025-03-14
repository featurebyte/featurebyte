"""
Unit tests for query graph definition extractor
"""

import pytest
from bson import ObjectId

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.transform.definition import DefinitionHashExtractor
from tests.util.helper import add_groupby_operation, add_project_operation, compare_pydantic_obj


@pytest.fixture(name="query_graph_and_assign_nodes")
def query_graph_and_assign_nodes(query_graph_and_assign_node):
    """Fixture of a query graph with two assign nodes"""
    graph, assign_node = query_graph_and_assign_node
    input_node_names = graph.get_input_node_names(graph.get_node_by_name("add_1"))
    sum_inputs = [graph.get_node_by_name(input_node_name) for input_node_name in input_node_names]
    sum_node = graph.add_operation(
        node_type=NodeType.ADD,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=list(reversed(sum_inputs)),
    )
    input_node = graph.get_input_node(assign_node.name)
    another_assign_node = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "d"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node, sum_node],
    )
    return graph, assign_node, another_assign_node


@pytest.fixture(name="lookup_assign_feature_node")
def lookup_assign_feature_node_fixture(global_graph, dimension_table_input_node, entity_id):
    """Fixture of a lookup feature"""
    proj_cust_value_1 = add_project_operation(
        graph=global_graph, input_node=dimension_table_input_node, column_names=["cust_value_1"]
    )
    proj_cust_value_2 = add_project_operation(
        graph=global_graph, input_node=dimension_table_input_node, column_names=["cust_value_2"]
    )
    concat_node = global_graph.add_operation(
        node_type=NodeType.CONCAT,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_cust_value_1, proj_cust_value_2],
    )
    assign_concat_node = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "cust_attr"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[dimension_table_input_node, concat_node],
    )
    assign_entity_node = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "assign_cust_id", "value": entity_id},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_concat_node],
    )
    lookup_node = global_graph.add_operation(
        node_type=NodeType.LOOKUP,
        node_params={
            "input_column_names": ["cust_attr"],
            "feature_names": ["cust_attr"],
            "entity_column": "assign_cust_id",
            "serving_name": "CUSTOMER_ID",
            "entity_id": entity_id,
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_entity_node],
    )
    feature_node = add_project_operation(
        graph=global_graph, input_node=lookup_node, column_names=["cust_attr"]
    )
    return feature_node


@pytest.fixture(name="assign_join_feature_node")
def assign_join_feature_node_fixture(
    global_graph,
    item_table_input_node,
    event_table_input_node,
    join_node_params,
    groupby_node_params,
):
    """Fixture of a join feature"""
    assign_event_table_node = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "event_assign_col", "value": 1234},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[event_table_input_node],
    )
    assign_item_table_node = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "item_assign_col", "value": 1234},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[item_table_input_node],
    )
    join_node_params["left_input_columns"].append("event_assign_col")
    join_node_params["left_output_columns"].append("event_assign_col_left")
    join_node_params["right_input_columns"].append("item_assign_col")
    join_node_params["right_output_columns"].append("item_assign_col_right")
    join_node = global_graph.add_operation(
        node_type=NodeType.JOIN,
        node_params=join_node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_event_table_node, assign_item_table_node],
    )
    groupby_node_params["parent"] = "event_assign_col_left"
    groupby_node_params["value_by"] = "item_assign_col_right"
    groupby_node_params["names"] = ["event_item_type_count_30d"]
    groupby_node_params["windows"] = ["30d"]
    groupby_node = add_groupby_operation(
        graph=global_graph,
        groupby_node_params=groupby_node_params,
        input_node=join_node,
    )
    feature_node = add_project_operation(
        graph=global_graph, input_node=groupby_node, column_names=["event_item_type_count_30d"]
    )
    return feature_node


def prune_graph_and_check_definition(graph, first_target_node, second_target_node):
    """Prune the graph & check that the definition hash is the same"""
    first_pruned_graph, node_name_map = graph.prune(target_node=first_target_node)
    first_mapped_node = first_pruned_graph.get_node_by_name(node_name_map[first_target_node.name])

    second_pruned_graph, node_name_map = graph.prune(target_node=second_target_node)
    second_mapped_node = second_pruned_graph.get_node_by_name(
        node_name_map[second_target_node.name]
    )

    # check that the definition hash is the same
    first_definition_extractor = DefinitionHashExtractor(graph=first_pruned_graph)
    first_output = first_definition_extractor.extract(node=first_mapped_node)
    second_definition_extractor = DefinitionHashExtractor(graph=second_pruned_graph)
    second_output = second_definition_extractor.extract(node=second_mapped_node)
    assert first_output.definition_hash == second_output.definition_hash, (
        first_output,
        second_output,
    )


def test_extract_definition__simple(graph_three_nodes):
    """Test extract definition (without column name remap)"""
    graph, _, _, target_node = graph_three_nodes
    definition_extractor = DefinitionHashExtractor(graph=graph)
    output = definition_extractor.extract(node=target_node)
    expected_hash = graph.node_name_to_ref[target_node.name]
    assert output.definition_hash == expected_hash, output


def test_extract_definition__assign_column_remapped(query_graph_and_assign_nodes):
    """Test extract definition (with column name remap)"""
    graph, assign_node, another_assign_node = query_graph_and_assign_nodes
    first_target_node = add_project_operation(
        graph=graph, input_node=assign_node, column_names=["c"]
    )
    second_target_node = add_project_operation(
        graph=graph, input_node=another_assign_node, column_names=["d"]
    )

    # check definition
    prune_graph_and_check_definition(graph, first_target_node, second_target_node)


def test_extract_definition__join_feature_column_remapped(
    global_graph, order_size_feature_node, event_table_input_node, order_size_feature_join_node
):
    """Test extract definition (with column name remap)"""
    # create the first pruned graph
    first_target_node = add_project_operation(
        graph=global_graph, input_node=order_size_feature_join_node, column_names=["ord_size"]
    )

    # create the second pruned graph
    node_params = {
        "view_entity_column": "order_id",
        "feature_entity_column": "order_id",
        "name": "another_ord_size",
    }
    join_feature_node = global_graph.add_operation(
        node_type=NodeType.JOIN_FEATURE,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[event_table_input_node, order_size_feature_node],
    )
    second_target_node = add_project_operation(
        graph=global_graph, input_node=join_feature_node, column_names=["another_ord_size"]
    )

    # check definition
    prune_graph_and_check_definition(global_graph, first_target_node, second_target_node)


def test_extract_definition__alias_node_handling(global_graph, input_node):
    """Test extract definition for alias node"""
    proj_a = add_project_operation(graph=global_graph, input_node=input_node, column_names=["a"])
    alias_node = global_graph.add_operation(
        node_type=NodeType.ALIAS,
        node_params={"name": "renamed_a"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a],
    )
    first_sum_node = global_graph.add_operation(
        node_type=NodeType.ADD,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a, alias_node],
    )
    first_target_node = global_graph.add_operation(
        node_type=NodeType.ALIAS,
        node_params={"name": "renamed_sum"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[first_sum_node],
    )
    second_sum_node = global_graph.add_operation(
        node_type=NodeType.ADD,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a, proj_a],
    )
    second_target_node = global_graph.add_operation(
        node_type=NodeType.ALIAS,
        node_params={"name": "renamed_another_sum"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[second_sum_node],
    )

    # check definition
    prune_graph_and_check_definition(global_graph, first_target_node, second_target_node)


def test_extract_definition__filter_node(query_graph_and_assign_nodes):
    """Test extract definition for filtering node"""
    graph, assign_node, another_assign_node = query_graph_and_assign_nodes
    node_input = graph.get_input_node(assign_node.name)
    proj_cust_id = add_project_operation(
        graph=graph, input_node=node_input, column_names=["cust_id"]
    )
    node_eq = graph.add_operation(
        node_type=NodeType.EQ,
        node_params={"value": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_cust_id],
    )
    first_filter_node = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node, node_eq],
    )
    second_filter_node = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[another_assign_node, node_eq],
    )
    first_target_node = add_project_operation(
        graph=graph, input_node=first_filter_node, column_names=["c"]
    )
    second_target_node = add_project_operation(
        graph=graph, input_node=second_filter_node, column_names=["d"]
    )

    # check definition
    prune_graph_and_check_definition(graph, first_target_node, second_target_node)


def test_extract_definition__aggregate_over(query_graph_and_assign_nodes, groupby_node_params):
    """Test extract definition for aggregate over"""
    graph, assign_node, another_assign_node = query_graph_and_assign_nodes
    groupby_node_params["parent"] = "c"
    groupby_node_params["value_by"] = "c"
    groupby_node_params["names"] = ["feat_48h", "feat_24h"]
    groupby_node_params["windows"] = ["48h", "24h"]
    groupby_node = add_groupby_operation(
        graph=graph,
        groupby_node_params=groupby_node_params,
        input_node=assign_node,
    )
    first_target_node = add_project_operation(
        graph=graph, input_node=groupby_node, column_names=["feat_48h"]
    )

    groupby_node_params["parent"] = "d"
    groupby_node_params["value_by"] = "d"
    groupby_node_params["names"] = ["feat_1d", "feat_2d"]
    groupby_node_params["windows"] = ["1d", "2d"]
    another_groupby_node = add_groupby_operation(
        graph=graph,
        groupby_node_params=groupby_node_params,
        input_node=another_assign_node,
    )
    second_target_node = add_project_operation(
        graph=graph, input_node=another_groupby_node, column_names=["feat_2d"]
    )

    # check definition
    prune_graph_and_check_definition(graph, first_target_node, second_target_node)


def test_extract_definition__aggregate(query_graph_and_assign_nodes, entity_id):
    """Test extract definition for aggregate"""
    graph, assign_node, another_assign_node = query_graph_and_assign_nodes
    node_params = {
        "keys": ["order_id"],
        "serving_names": ["order_id"],
        "entity_ids": [entity_id],
        "parent": "c",
        "agg_func": "sum",
        "name": "some_feature_name",
    }
    first_item_groupby_node = graph.add_operation(
        node_type=NodeType.ITEM_GROUPBY,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node],
    )
    first_target_node = add_project_operation(
        graph=graph, input_node=first_item_groupby_node, column_names=["some_feature_name"]
    )

    node_params["parent"] = "d"
    node_params["name"] = "another_feature_name"
    second_item_groupby_node = graph.add_operation(
        node_type=NodeType.ITEM_GROUPBY,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[another_assign_node],
    )
    second_target_node = add_project_operation(
        graph=graph, input_node=second_item_groupby_node, column_names=["another_feature_name"]
    )

    # check definition
    prune_graph_and_check_definition(graph, first_target_node, second_target_node)


def test_extract_definition__lookup(
    global_graph, dimension_table_input_node, entity_id, lookup_feature_node
):
    """Test extract definition for lookup"""
    # construct another equivalent lookup feature with different user specified column names
    node_params = {
        "input_column_names": ["cust_value_2", "cust_value_1"],
        "feature_names": ["cust_attr_2", "cust_attr_1"],
        "entity_column": "cust_id",
        "serving_name": "CUSTOMER_ID",
        "entity_id": entity_id,
    }
    another_lookup_node = global_graph.add_operation(
        node_type=NodeType.LOOKUP,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[dimension_table_input_node],
    )
    feat_node_1 = add_project_operation(
        graph=global_graph, input_node=another_lookup_node, column_names=["cust_attr_1"]
    )
    feat_node_2 = add_project_operation(
        graph=global_graph, input_node=another_lookup_node, column_names=["cust_attr_2"]
    )
    another_lookup_feature_node = global_graph.add_operation(
        node_type=NodeType.ADD,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[feat_node_2, feat_node_1],
    )
    another_feature_alias_node = global_graph.add_operation(
        node_type=NodeType.ALIAS,
        node_params={"name": "another lookup feature"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[another_lookup_feature_node],
    )

    # check feature definition
    prune_graph_and_check_definition(
        graph=global_graph,
        first_target_node=lookup_feature_node,
        second_target_node=another_feature_alias_node,
    )


def test_extract_definition__join_with_groupby(
    global_graph,
    item_table_input_node,
    event_table_input_node,
    join_node_params,
    item_table_join_event_table_node,
):
    """Test extract definition for join"""
    groupby_node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": "item_type",
        "parent": "item_id",
        "agg_func": "sum",
        "feature_job_setting": {
            "offset": "1800s",  # 30m
            "period": "3600s",  # 1h
            "blind_spot": "900s",  # 15m
        },
        "timestamp": "ts",
        "names": ["aggregated_item_type_count"],
        "windows": [None],
    }
    groupby_node = add_groupby_operation(
        graph=global_graph,
        groupby_node_params=groupby_node_params,
        input_node=item_table_join_event_table_node,
    )
    feature_node = add_project_operation(
        graph=global_graph, input_node=groupby_node, column_names=["aggregated_item_type_count"]
    )

    # construct another equivalent groupby feature with different user specified column names
    join_node_params["left_output_columns"] = ["order_method_left", "cust_id", "ts"]
    join_node_params["right_output_columns"] = [
        "order_id_right",
        "item_id_right",
        "item_name_right",
        "item_type_right",
    ]
    another_join_node = global_graph.add_operation(
        node_type=NodeType.JOIN,
        node_params=join_node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[event_table_input_node, item_table_input_node],
    )
    groupby_node_params["parent"] = "item_id_right"
    groupby_node_params["value_by"] = "item_type_right"
    groupby_node_params["windows"] = [None]
    groupby_node_params["names"] = ["item_type_count_aggregated"]
    another_groupby_node = add_groupby_operation(
        graph=global_graph,
        groupby_node_params=groupby_node_params,
        input_node=another_join_node,
    )
    another_feature_node = add_project_operation(
        graph=global_graph,
        input_node=another_groupby_node,
        column_names=["item_type_count_aggregated"],
    )

    # check feature definition
    prune_graph_and_check_definition(
        graph=global_graph,
        first_target_node=feature_node,
        second_target_node=another_feature_node,
    )


def compute_aggregate_over_changes(
    graph, input_node, prev_col, next_col, diff_column, feat_name, groupby_node_params
):
    """Compute aggregate over changes"""
    proj_prev_node = add_project_operation(
        graph=graph, input_node=input_node, column_names=[prev_col]
    )
    proj_next_node = add_project_operation(
        graph=graph, input_node=input_node, column_names=[next_col]
    )
    subtract_node = graph.add_operation(
        node_type=NodeType.SUB,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_next_node, proj_prev_node],
    )
    assign_node = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": diff_column},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node, subtract_node],
    )
    groupby_node_params["parent"] = diff_column
    groupby_node_params["names"] = [feat_name]
    groupby_node_params["windows"] = ["30d"]
    groupby_node = add_groupby_operation(
        graph=graph,
        groupby_node_params=groupby_node_params,
        input_node=assign_node,
    )
    feature_node = add_project_operation(
        graph=graph, input_node=groupby_node, column_names=[feat_name]
    )
    return feature_node


def test_extract_definition__track_changes(
    global_graph, scd_table_input_details, groupby_node_params
):
    """Test extract definition for track changes"""
    scd_node_params = {
        "type": "scd_table",
        "columns": [
            {"name": "effective_ts", "dtype": DBVarType.TIMESTAMP},
            {"name": "cust_id", "dtype": DBVarType.INT},
            {"name": "membership_status", "dtype": DBVarType.VARCHAR},
            {"name": "active_days", "dtype": DBVarType.INT},
        ],
        "effective_timestamp_column": "effective_ts",
        "current_flag_column": "is_record_current",
    }

    scd_node_params.update(scd_table_input_details)
    scd_table_input_node = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=scd_node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    node_params = {
        "natural_key_column": "cust_id",
        "effective_timestamp_column": "effective_ts",
        "tracked_column": "active_days",
        "previous_tracked_column_name": "previous_active_days",
        "new_tracked_column_name": "new_active_days",
        "previous_valid_from_column_name": "previous_valid_from",
        "new_valid_from_column_name": "new_valid_from",
    }
    groupby_node_params["agg_func"] = "avg"
    track_changes_node = global_graph.add_operation(
        node_type=NodeType.TRACK_CHANGES,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[scd_table_input_node],
    )
    feat_node = compute_aggregate_over_changes(
        graph=global_graph,
        input_node=track_changes_node,
        prev_col="previous_active_days",
        next_col="new_active_days",
        diff_column="active_days_diff",
        feat_name="active_days_diff_avg_30d",
        groupby_node_params=groupby_node_params,
    )

    # construct another equivalent feature with different user specified column names
    node_params["previous_tracked_column_name"] = "old_active_days"
    node_params["new_tracked_column_name"] = "next_active_days"
    node_params["previous_valid_from_column_name"] = "old_valid_from"
    node_params["new_valid_from_column_name"] = "next_valid_from"
    another_track_changes_node = global_graph.add_operation(
        node_type=NodeType.TRACK_CHANGES,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[scd_table_input_node],
    )
    another_feat_node = compute_aggregate_over_changes(
        graph=global_graph,
        input_node=another_track_changes_node,
        prev_col="old_active_days",
        next_col="next_active_days",
        diff_column="another_active_days_diff",
        feat_name="another_active_days_diff_avg_30d",
        groupby_node_params=groupby_node_params,
    )

    # check feature definition
    prune_graph_and_check_definition(
        graph=global_graph,
        first_target_node=feat_node,
        second_target_node=another_feat_node,
    )


def test_extract_definition__aggregate_asat(
    global_graph, aggregate_asat_feature_node, scd_table_input_node
):
    """Test extract definition for aggregate asat"""
    asat_node = global_graph.get_node_by_name("aggregate_as_at_1")
    asat_node_params = asat_node.parameters.model_dump()
    asat_node_params["name"] = "another_asat_feature"
    another_asat_node = global_graph.add_operation(
        node_type=NodeType.AGGREGATE_AS_AT,
        node_params=asat_node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[scd_table_input_node],
    )
    another_feature_node = add_project_operation(
        graph=global_graph, input_node=another_asat_node, column_names=["another_asat_feature"]
    )

    # check feature definition
    prune_graph_and_check_definition(
        graph=global_graph,
        first_target_node=aggregate_asat_feature_node,
        second_target_node=another_feature_node,
    )


def test_extract_definition__lookup_on_assign(global_graph, lookup_assign_feature_node, entity_id):
    """Test extract definition for lookup on assign"""
    pruned_graph, node_name_map = global_graph.prune(target_node=lookup_assign_feature_node)
    mapped_node = global_graph.get_node_by_name(node_name_map[lookup_assign_feature_node.name])
    definition_extractor = DefinitionHashExtractor(graph=pruned_graph)
    output = definition_extractor.extract(node=mapped_node)
    definition_lookup_node = output.graph.get_node_by_name("lookup_1")

    # check that input_column_names, feature_names, entity_column are remapped
    compare_pydantic_obj(
        definition_lookup_node.parameters,
        expected={
            "input_column_names": ["column_c75864236e441dfade8987dcfaeec31166850b0c"],
            "feature_names": [
                "feat_97c9784cb6aee097a5fc6590c82aa72da6a46275_column_c75864236e441dfade8987dcfaeec31166850b0c"
            ],
            "entity_column": "column_2e659eb6bcbf83ef47f54dc5b316d5aa657b9fed",
            "serving_name": "CUSTOMER_ID",
            "entity_id": entity_id,
            "scd_parameters": None,
            "event_parameters": None,
        },
    )


def test_extract_definition__join_on_assign(global_graph, assign_join_feature_node):
    """Test extract definition for join on assign"""
    pruned_graph, node_name_map = global_graph.prune(target_node=assign_join_feature_node)
    mapped_node = global_graph.get_node_by_name(node_name_map[assign_join_feature_node.name])
    definition_extractor = DefinitionHashExtractor(graph=pruned_graph)
    output = definition_extractor.extract(node=mapped_node)
    definition_join_node = output.graph.get_node_by_name("join_1")

    # check that user specified column names are remapped
    compare_pydantic_obj(
        definition_join_node.parameters,
        expected={
            "join_type": "inner",
            "left_input_columns": [
                "column_e007333d31736e39a0c148e730a98bbc4cc6f823",
                "cust_id",
                "order_method",
                "ts",
            ],
            "left_on": "order_id",
            "left_output_columns": [
                "left_fa9b94eb2cdfbf4a9a33ddd987b51d9caaf9e506_column_e007333d31736e39a0c148e730a98bbc4cc6f823",
                "left_fa9b94eb2cdfbf4a9a33ddd987b51d9caaf9e506_cust_id",
                "left_fa9b94eb2cdfbf4a9a33ddd987b51d9caaf9e506_order_method",
                "left_fa9b94eb2cdfbf4a9a33ddd987b51d9caaf9e506_ts",
            ],
            "metadata": None,
            "right_input_columns": [
                "item_assign_col",
                "item_id",
                "item_name",
                "item_type",
                "order_id",
            ],
            "right_on": "order_id",
            "right_output_columns": [
                "right_fa9b94eb2cdfbf4a9a33ddd987b51d9caaf9e506_item_assign_col",
                "right_fa9b94eb2cdfbf4a9a33ddd987b51d9caaf9e506_item_id",
                "right_fa9b94eb2cdfbf4a9a33ddd987b51d9caaf9e506_item_name",
                "right_fa9b94eb2cdfbf4a9a33ddd987b51d9caaf9e506_item_type",
                "right_fa9b94eb2cdfbf4a9a33ddd987b51d9caaf9e506_order_id",
            ],
            "scd_parameters": None,
        },
    )


def test_extract_definition__lag(query_graph_with_lag_node):
    """Test extract definition for lag"""
    graph, lag_node = query_graph_with_lag_node
    definition_extractor = DefinitionHashExtractor(graph=graph)
    output = definition_extractor.extract(node=lag_node)
    definition_lag_node = output.graph.get_node_by_name("lag_1")

    # check that input column parameters are removed
    compare_pydantic_obj(
        definition_lag_node.parameters,
        expected={
            "entity_columns": [],
            "timestamp_column": "",
            "offset": 1,
        },
    )


def test_extract_definition__forward_aggregate(query_graph_and_assign_node, event_table_details):
    """Test extract definition for forward aggregate"""
    graph, assign_node = query_graph_and_assign_node
    assign_timestamp_node = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "assign_timestamp", "value": 1234},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node],
    )
    assign_entity_node = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "assign_entity", "value": "cust_id"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_timestamp_node],
    )
    forward_node = graph.add_operation(
        node_type=NodeType.FORWARD_AGGREGATE,
        node_params={
            "name": "target",
            "window": "7d",
            "table_details": event_table_details,
            "timestamp_col": "assign_timestamp",
            "keys": ["assign_entity"],
            "agg_func": "sum",
            "serving_names": ["CUST_ID"],
            "parent": "c",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_entity_node],
    )
    proj_target = add_project_operation(
        graph=graph, input_node=forward_node, column_names=["target"]
    )
    definition_extractor = DefinitionHashExtractor(graph=graph)
    output = definition_extractor.extract(node=proj_target)

    # check forward aggregate definition
    definition_forward_node = output.graph.get_node_by_name("forward_aggregate_1")
    compare_pydantic_obj(
        definition_forward_node.parameters,
        expected={
            "agg_func": "sum",
            "entity_ids": None,
            "keys": ["column_83f21aa2ec6d182f0f578241eda01d24e52ff4a2"],
            "name": "target_7b1c700f5d03c3f7d23e4bf630aaf576e5eceb0f",
            "offset": None,
            "parent": "column_41a32af12d0e8bc4d3405f31b9bf52034f7cb98e",
            "serving_names": ["CUST_ID"],
            "timestamp_col": "column_5430b65a132f391e1ada3fe6389bee6e856bdd77",
            "timestamp_metadata": None,
            "value_by": None,
            "window": "604800s",
        },
    )
    project_node = output.graph.get_node_by_name("project_3")
    assert project_node.parameters.model_dump() == {
        "columns": ["target_7b1c700f5d03c3f7d23e4bf630aaf576e5eceb0f"]
    }


def test_extract_definition__join_feature(
    global_graph, order_size_feature_node, event_table_input_node, order_size_feature_join_node
):
    """Test extract definition for join feature"""
    assign_entity_node = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "assign_entity", "value": "cust_id"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[event_table_input_node],
    )
    assign_point_in_time_node = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "assign_point_in_time", "value": 1234},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_entity_node],
    )
    node_params = {
        "view_entity_column": "assign_entity",
        "feature_entity_column": "assign_entity",
        "view_point_in_time_column": "assign_point_in_time",
        "name": "join_feat",
    }
    join_feature_node = global_graph.add_operation(
        node_type=NodeType.JOIN_FEATURE,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_point_in_time_node, order_size_feature_node],
    )
    proj_join_feature_node = add_project_operation(
        graph=global_graph, input_node=join_feature_node, column_names=["join_feat"]
    )
    definition_extractor = DefinitionHashExtractor(graph=global_graph)
    output = definition_extractor.extract(node=proj_join_feature_node)

    # check join feature definition
    definition_join_feature_node = output.graph.get_node_by_name("join_feature_1")
    compare_pydantic_obj(
        definition_join_feature_node.parameters,
        expected={
            "feature_entity_column": "column_e007333d31736e39a0c148e730a98bbc4cc6f823",
            "name": "column_6a498e40eed9cef70637059e0102a855fad060e1",
            "view_entity_column": "column_e007333d31736e39a0c148e730a98bbc4cc6f823",
            "view_point_in_time_column": "column_20c47fd6cb36426d79002d228b3ed70644b7800e",
        },
    )


def test_extract_definition__scd_join(global_graph, event_table_input_node, scd_table_input_node):
    """Test extract definition for scd join"""
    assign_event_timestamp = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "assign_event_timestamp", "value": 1234},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[event_table_input_node],
    )
    assign_effective_timestamp = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "assign_effective_timestamp", "value": 1234},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[scd_table_input_node],
    )
    node_params = {
        "left_on": "cust_id",
        "right_on": "cust_id",
        "left_input_columns": ["assign_event_timestamp", "cust_id"],
        "left_output_columns": ["event_timestamp", "cust_id"],
        "right_input_columns": ["membership_status"],
        "right_output_columns": ["latest_membership_status"],
        "join_type": "left",
        "scd_parameters": {
            "left_timestamp_column": "assign_event_timestamp",
            "effective_timestamp_column": "assign_effective_timestamp",
        },
    }
    join_node = global_graph.add_operation(
        node_type=NodeType.JOIN,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_event_timestamp, assign_effective_timestamp],
    )
    definition_extractor = DefinitionHashExtractor(graph=global_graph)
    output = definition_extractor.extract(node=join_node)

    # check scd join definition
    definition_scd_join_node = output.graph.get_node_by_name("join_1")
    compare_pydantic_obj(
        definition_scd_join_node.parameters,
        expected={
            "join_type": "left",
            "left_input_columns": ["column_e007333d31736e39a0c148e730a98bbc4cc6f823", "cust_id"],
            "left_on": "cust_id",
            "left_output_columns": [
                "left_c3e5bd7cfcfffa7b211527ab173e8e346091f3cb_column_e007333d31736e39a0c148e730a98bbc4cc6f823",
                "left_c3e5bd7cfcfffa7b211527ab173e8e346091f3cb_cust_id",
            ],
            "metadata": None,
            "right_input_columns": ["membership_status"],
            "right_on": "cust_id",
            "right_output_columns": [
                "right_c3e5bd7cfcfffa7b211527ab173e8e346091f3cb_membership_status"
            ],
            "scd_parameters": {
                "current_flag_column": None,
                "effective_timestamp_column": "column_9a6d0e6c2e504d026e3b3001c9480e7a992cb2bb",
                "end_timestamp_column": None,
                "left_timestamp_column": "column_e007333d31736e39a0c148e730a98bbc4cc6f823",
                "natural_key_column": None,
                "effective_timestamp_metadata": None,
                "end_timestamp_metadata": None,
                "left_timestamp_metadata": None,
            },
        },
    )


def test_extract_definition__scd_lookup(
    global_graph, scd_lookup_node_parameters, scd_table_input_node
):
    """Test extract definition for scd lookup"""
    assign_effective_timestamp = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "event_timestamp", "value": 1234},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[scd_table_input_node],
    )
    lookup_node = global_graph.add_operation(
        node_type=NodeType.LOOKUP,
        node_params=scd_lookup_node_parameters,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_effective_timestamp],
    )
    definition_extractor = DefinitionHashExtractor(graph=global_graph)
    output = definition_extractor.extract(node=lookup_node)

    # check scd lookup definition
    definition_scd_lookup_node = output.graph.get_node_by_name("lookup_1")
    compare_pydantic_obj(
        definition_scd_lookup_node.parameters,
        expected={
            "entity_column": "cust_id",
            "entity_id": ObjectId("63dbe68cd918ef71acffd127"),
            "event_parameters": None,
            "feature_names": ["feat_7508cdc215a29f57609fdb31d6221be00462fdcd_membership_status"],
            "input_column_names": ["membership_status"],
            "scd_parameters": {
                "current_flag_column": "is_record_current",
                "effective_timestamp_column": "column_9a6d0e6c2e504d026e3b3001c9480e7a992cb2bb",
                "effective_timestamp_metadata": None,
                "end_timestamp_column": None,
                "end_timestamp_metadata": None,
                "natural_key_column": "cust_id",
                "offset": None,
            },
            "serving_name": "CUSTOMER_ID",
        },
    )


def test_extract_definition__event_lookup(global_graph, event_table_input_node, entity_id):
    """Test extract definition for event lookup"""
    assign_event_timestamp = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "ts", "value": 1234},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[event_table_input_node],
    )
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
        input_nodes=[assign_event_timestamp],
    )
    definition_extractor = DefinitionHashExtractor(graph=global_graph)
    output = definition_extractor.extract(node=lookup_node)

    # check event lookup definition
    definition_event_lookup_node = output.graph.get_node_by_name("lookup_1")
    compare_pydantic_obj(
        definition_event_lookup_node.parameters,
        expected={
            "entity_column": "order_id",
            "entity_id": ObjectId("63dbe68cd918ef71acffd127"),
            "event_parameters": {
                "event_timestamp_column": "column_e007333d31736e39a0c148e730a98bbc4cc6f823",
                "event_timestamp_metadata": None,
            },
            "feature_names": ["feat_98d843176d15410f62195cdc6698331e04d6a1dd_order_method"],
            "input_column_names": ["order_method"],
            "scd_parameters": None,
            "serving_name": "ORDER_ID",
        },
    )
