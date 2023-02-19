"""
This module contains utility functions used in tests
"""
import sys
from contextlib import contextmanager
from unittest.mock import Mock

from featurebyte.api.database_table import AbstractTableData
from featurebyte.core.generic import QueryObject
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.util import get_aggregation_identifier, get_tile_table_identifier


def assert_equal_with_expected_fixture(actual, fixture_filename, update_fixture=False):
    """Utility to check that actual is the same as the pre-generated fixture

    To update all fixtures automatically, pass --update-fixtures option when invoking pytest.
    """
    if update_fixture:
        with open(fixture_filename, "w", encoding="utf-8") as f_handle:
            f_handle.write(actual)
            f_handle.write("\n")

    with open(fixture_filename, encoding="utf-8") as f_handle:
        expected = f_handle.read()

    assert actual.strip() == expected.strip()


@contextmanager
def patch_import_package(package_path):
    """
    Mock import statement
    """
    mock_package = Mock()
    original_module = sys.modules.get(package_path)
    sys.modules[package_path] = mock_package
    try:
        yield mock_package
    finally:
        if original_module:
            sys.modules[package_path] = original_module
        else:
            sys.modules.pop(package_path)


def get_lagged_series_pandas(df, column, timestamp, groupby_key):
    """
    Get lagged value for a column in a pandas DataFrame

    Parameters
    ----------
    df : DataFrame
        pandas DataFrame
    column : str
        Column name
    timestamp : str
        Timestamp column anme
    groupby_key : str
        Entity column to consider when getting the lag
    """
    df = df.copy()
    df_sorted = df.sort_values(timestamp)
    df[column] = df_sorted.groupby(groupby_key)[column].shift(1)
    return df[column]


def get_node(graph_dict, node_name):
    """Get node from the data dictionary"""
    return next(node for node in graph_dict["nodes"] if node["name"] == node_name)


def add_groupby_operation(graph, groupby_node_params, input_node):
    """
    Helper function to add a groupby node
    """
    node = graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params={
            **groupby_node_params,
            "tile_id": get_tile_table_identifier("deadbeef1234", groupby_node_params),
            "aggregation_id": get_aggregation_identifier(
                graph.node_name_to_ref[input_node.name], groupby_node_params
            ),
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node],
    )
    return node


def check_aggressively_pruned_graph(left_obj_dict, right_obj_dict):
    """
    Check aggressively pruned graph of the left & right graphs
    """
    # as serialization only perform non-aggressive pruning (all travelled nodes are kept)
    # here we need to perform aggressive pruning & compare the final graph to make sure they are the same
    left_graph = QueryGraph(**dict(left_obj_dict["graph"]))
    right_graph = QueryGraph(**dict(right_obj_dict["graph"]))
    left_pruned_graph, _ = left_graph.prune(
        target_node=left_graph.get_node_by_name(left_obj_dict["node_name"]), aggressive=True
    )
    right_pruned_graph, _ = right_graph.prune(
        target_node=right_graph.get_node_by_name(right_obj_dict["node_name"]),
        aggressive=True,
    )
    assert left_pruned_graph == right_pruned_graph


def _check_pruned_graph_and_nodes(
    pruned_graph, node, expected_pruned_graph, expected_node, sdk_code
):
    # helper method to check pruned graph & node
    assert pruned_graph == expected_pruned_graph, sdk_code
    assert node == expected_node, sdk_code


def check_sdk_code_generation(
    api_object, to_use_saved_data=False, data_id_to_info=None, to_format=True
):
    """Check SDK code generation"""
    if isinstance(api_object, AbstractTableData):
        query_object = api_object.frame
    elif isinstance(api_object, QueryObject):
        query_object = api_object
    else:
        raise ValueError("Unexpected api_object!!")

    # execute SDK code & generate output object
    local_vars = {}
    sdk_code = query_object._generate_code(
        to_format=to_format, to_use_saved_data=to_use_saved_data, data_id_to_info=data_id_to_info
    )
    exec(sdk_code, {}, local_vars)  # pylint: disable=exec-used
    output = local_vars["output"]
    if isinstance(output, AbstractTableData):
        output = output.frame

    # compare the output
    pruned_graph, node = output.extract_pruned_graph_and_node()
    expected_pruned_graph, expected_node = query_object.extract_pruned_graph_and_node()
    _check_pruned_graph_and_nodes(
        pruned_graph=pruned_graph,
        node=node,
        expected_pruned_graph=expected_pruned_graph,
        expected_node=expected_node,
        sdk_code=sdk_code,
    )


def compare_generated_api_object_sdk_code(
    api_object,
    data_id,
    fixture_path,
    update_fixtures,
    to_use_saved_data,
    data_id_to_info=None,
    to_format=True,
    **kwargs,
):
    """Compare generated SDK code for data object"""
    feature_store_id = api_object.feature_store.id
    if update_fixtures:
        formatted_sdk_code = api_object._generate_code(
            to_format=to_format,
            to_use_saved_data=to_use_saved_data,
            data_id_to_info=data_id_to_info,
        )
        formatted_sdk_code = formatted_sdk_code.replace(f"{feature_store_id}", "{feature_store_id}")
        if data_id:
            formatted_sdk_code = formatted_sdk_code.replace(f"{data_id}", "{data_id}")

        # for item data, there is an additional `event_data_id`. Replace the actual `event_data_id` value
        # to {event_data_id} placeholder through kwargs
        for key, value in kwargs.items():
            formatted_sdk_code = formatted_sdk_code.replace(f"{value}", "{key}".replace("key", key))

        with open(fixture_path, mode="w", encoding="utf-8") as file_handle:
            file_handle.write(formatted_sdk_code)

    sdk_code = api_object._generate_code(
        to_format=to_format,
        to_use_saved_data=to_use_saved_data,
        data_id_to_info=data_id_to_info,
    )
    with open(fixture_path, mode="r", encoding="utf-8") as file_handle:
        expected = file_handle.read().format(
            feature_store_id=feature_store_id, data_id=data_id, **kwargs
        )
        assert expected.strip() == sdk_code.strip(), sdk_code
