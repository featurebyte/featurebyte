"""
This module contains utility functions used in tests
"""
import json
import sys
from contextlib import contextmanager
from unittest.mock import Mock

import numpy as np
import pandas as pd
from pandas.core.dtypes.common import is_numeric_dtype

from featurebyte.api.database_table import AbstractTableData
from featurebyte.core.generic import QueryObject
from featurebyte.enum import AggFunc
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


def check_sdk_code_generation(  # pylint: disable=too-many-locals
    api_object,
    to_use_saved_data=False,
    data_id_to_info=None,
    to_format=True,
    fixture_path=None,
    update_fixtures=False,
    to_compare_generated_code=True,
    data_id=None,
    **kwargs,
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

    if fixture_path:
        feature_store_id = api_object.feature_store.id
        if update_fixtures:
            # escape certain characters so that jinja can handle it properly
            formatted_sdk_code = sdk_code.replace("{", "{{").replace("}", "}}")

            # convert the object id to a placeholder
            formatted_sdk_code = formatted_sdk_code.replace(
                f"{feature_store_id}", "{feature_store_id}"
            )
            if data_id:
                formatted_sdk_code = formatted_sdk_code.replace(f"{data_id}", "{data_id}")

            # for item data, there is an additional `event_data_id`. Replace the actual `event_data_id` value
            # to {event_data_id} placeholder through kwargs
            for key, value in kwargs.items():
                formatted_sdk_code = formatted_sdk_code.replace(
                    f"{value}", "{key}".replace("key", key)
                )

            with open(fixture_path, mode="w", encoding="utf-8") as file_handle:
                file_handle.write(formatted_sdk_code)
        elif to_compare_generated_code:
            with open(fixture_path, mode="r", encoding="utf-8") as file_handle:
                expected = file_handle.read().format(
                    feature_store_id=feature_store_id, data_id=data_id, **kwargs
                )
                assert expected.strip() == sdk_code.strip(), sdk_code


def assert_dict_equal(s1, s2):
    """
    Check two dict like columns are equal

    Parameters
    ----------
    s1 : Series
        First series
    s2 : Series
        Second series
    """

    def _json_normalize(x):
        # json conversion during preview changed None to nan
        if x is None or np.nan:
            return None
        return json.loads(x)

    s1 = s1.apply(_json_normalize)
    s2 = s2.apply(_json_normalize)
    pd.testing.assert_series_equal(s1, s2)


def fb_assert_frame_equal(df, df_expected, dict_like_columns=None):
    """
    Check that two DataFrames are equal

    Parameters
    ----------
    df : DataFrame
        DataFrame to check
    df_expected : DataFrame
        Reference DataFrame
    dict_like_columns : list | None
        List of dict like columns which will be compared accordingly, not just exact match
    """
    assert df.columns.tolist() == df_expected.columns.tolist()

    regular_columns = df.columns.tolist()
    if dict_like_columns is not None:
        assert isinstance(dict_like_columns, list)
        regular_columns = [col for col in regular_columns if col not in dict_like_columns]

    if regular_columns:
        for col in regular_columns:
            if is_numeric_dtype(df_expected[col]):
                df[col] = df[col].astype(float)
        pd.testing.assert_frame_equal(
            df[regular_columns], df_expected[regular_columns], check_dtype=False
        )

    if dict_like_columns:
        for col in dict_like_columns:
            assert_dict_equal(df[col], df_expected[col])


def assert_preview_result_equal(df_preview, expected_dict, dict_like_columns=None):
    """
    Compare feature / feature list preview result against the expected values
    """
    assert df_preview.shape[0] == 1

    df_expected = pd.DataFrame([pd.Series(expected_dict)])
    assert set(df_preview.columns) == set(df_expected.columns)

    df_expected = df_expected[df_preview.columns]
    fb_assert_frame_equal(df_preview, df_expected, dict_like_columns=dict_like_columns)


def iet_entropy(view, group_by_col, window, name, feature_job_setting=None):
    """
    Create feature to capture the entropy of inter-event interval time,
    this function is used to construct a test feature.
    """
    view = view.copy()
    ts_col = view[view.timestamp_column]
    a = view["a"] = (ts_col - ts_col.lag(group_by_col)).dt.day
    view["a * log(a)"] = a * (a + 0.1).log()  # add 0.1 to avoid log(0.0)
    b = view.groupby(group_by_col).aggregate_over(
        "a",
        method=AggFunc.SUM,
        windows=[window],
        feature_names=[f"sum(a) ({window})"],
        feature_job_setting=feature_job_setting,
    )[f"sum(a) ({window})"]

    feature = (
        view.groupby(group_by_col).aggregate_over(
            "a * log(a)",
            method=AggFunc.SUM,
            windows=[window],
            feature_names=["sum(a * log(a))"],
            feature_job_setting=feature_job_setting,
        )["sum(a * log(a))"]
        * -1
        / b
        + (b + 0.1).log()  # add 0.1 to avoid log(0.0)
    )

    feature.name = name
    return feature
