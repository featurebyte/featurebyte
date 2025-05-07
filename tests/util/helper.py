"""
This module contains utility functions used in tests
"""

from __future__ import annotations

import importlib
import json
import os
import re
import sys
import tempfile
import textwrap
from contextlib import ExitStack, asynccontextmanager, contextmanager
from pathlib import Path
from typing import Generator
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest
from bson import ObjectId, json_util
from pandas.core.dtypes.common import is_numeric_dtype
from pydantic import BaseModel
from sqlglot import expressions

import featurebyte as fb
from featurebyte import FeatureList, get_version
from featurebyte.api.deployment import Deployment
from featurebyte.api.source_table import AbstractTableData
from featurebyte.core.generic import QueryObject
from featurebyte.enum import AggFunc, DBVarType, SourceType
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalGraphState, GlobalQueryGraph, QueryGraph
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.query_graph.node.generic import GroupByNodeParameters
from featurebyte.query_graph.node.nested import OfflineStoreIngestQueryGraphNodeParameters
from featurebyte.query_graph.node.request import RequestColumnNode
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import get_fully_qualified_table_name, sql_to_string
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.transform.null_filling_value import NullFillingValueExtractor
from featurebyte.query_graph.transform.offline_store_ingest import (
    OfflineStoreIngestQueryGraphTransformer,
)
from featurebyte.query_graph.util import get_aggregation_identifier, get_tile_table_identifier_v1
from featurebyte.schema.context import ContextCreate
from featurebyte.schema.feature import FeatureServiceCreate
from featurebyte.schema.feature_list import FeatureListServiceCreate, OnlineFeaturesRequestPayload
from featurebyte.schema.use_case import UseCaseCreate
from featurebyte.worker.util.batch_feature_creator import set_environment_variable


def reset_global_graph():
    """
    Reset global graph state and get a new GlobalQueryGraph instance
    """
    GlobalGraphState.reset()
    # Resetting GlobalGraph invalidates the QueryObject's operation structure cache
    QueryObject._operation_structure_cache.clear()
    return GlobalQueryGraph()


def assert_equal_with_expected_fixture(actual, fixture_filename, update_fixture=False):
    """Utility to check that actual is the same as the pre-generated fixture

    To update all fixtures automatically, pass --update-fixtures option when invoking pytest.
    """
    if update_fixture:
        Path(os.path.dirname(fixture_filename)).mkdir(parents=True, exist_ok=True)
        with open(fixture_filename, "w", encoding="utf-8") as f_handle:
            f_handle.write(actual)
            f_handle.write("\n")

    with open(fixture_filename, encoding="utf-8") as f_handle:
        expected = f_handle.read()

    assert actual.strip() == expected.strip()


def write_json_fixture(fixture_filename, fixture_obj):
    """
    Write fixture to json file
    """
    Path(os.path.dirname(fixture_filename)).mkdir(parents=True, exist_ok=True)
    with open(fixture_filename, "w", encoding="utf-8") as f_handle:
        content = json_util.dumps(fixture_obj, indent=4, sort_keys=True)
        f_handle.write(content)
        f_handle.write("\n")


def assert_equal_json_fixture(obj, fixture_filename, update_fixtures):
    """
    Get or update fixture
    """
    if update_fixtures:
        write_json_fixture(fixture_filename, obj)
    with open(fixture_filename, encoding="utf-8") as f_handle:
        expected = json_util.loads(f_handle.read())
    assert obj == expected


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
    """Get node from the table dictionary"""
    return next(node for node in graph_dict["nodes"] if node["name"] == node_name)


def add_groupby_operation(
    graph, groupby_node_params, input_node, override_tile_id=None, override_aggregation_id=None
):
    """
    Helper function to add a groupby node
    """
    groupby_node_params = GroupByNodeParameters(**groupby_node_params).model_dump(by_alias=True)
    node = graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params={
            **groupby_node_params,
            "tile_id": (
                get_tile_table_identifier_v1(
                    "deadbeef1234",
                    groupby_node_params,
                )
                if override_tile_id is None
                else override_tile_id
            ),
            "aggregation_id": (
                get_aggregation_identifier(
                    graph.node_name_to_ref[input_node.name], groupby_node_params
                )
                if override_aggregation_id is None
                else override_aggregation_id
            ),
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node],
    )
    return node


def add_project_operation(graph, column_names, input_node, node_output_type=None):
    """
    Helper function to add a project node
    """
    node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": column_names},
        node_output_type=node_output_type or NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    return node


def check_aggressively_pruned_graph(left_obj_dict, right_obj_dict):
    """
    Check aggressively pruned graph of the left & right graphs
    """
    # as serialization only perform quick pruning (all travelled nodes are kept)
    # here we need to perform (aggressive) pruning & compare the final graph to make sure they are the same
    left_graph = QueryGraph(**dict(left_obj_dict["graph"]))
    right_graph = QueryGraph(**dict(right_obj_dict["graph"]))
    left_pruned_graph, _ = left_graph.prune(
        target_node=left_graph.get_node_by_name(left_obj_dict["node_name"])
    )
    right_pruned_graph, _ = right_graph.prune(
        target_node=right_graph.get_node_by_name(right_obj_dict["node_name"])
    )
    assert left_pruned_graph == right_pruned_graph


def _replace_view_mode_to_manual(pruned_graph):
    """Replace view mode to manual"""
    # to enable graph comparison, we need to replace view_mode to manual.
    # otherwise, the comparison will fail equality check even if the graphs are the same
    # (only difference is the view_mode in metadata).
    pruned_graph_dict = pruned_graph.model_dump()
    for node in pruned_graph_dict["nodes"]:
        if node["type"] == NodeType.GRAPH:
            if "view_mode" in node["parameters"]["metadata"]:
                node["parameters"]["metadata"]["view_mode"] = "manual"
    return QueryGraph(**pruned_graph_dict)


def check_sdk_code_generation(
    api_object,
    to_use_saved_data=False,
    table_id_to_info=None,
    to_format=True,
    fixture_path=None,
    update_fixtures=False,
    table_id=None,
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
        to_format=to_format,
        to_use_saved_data=to_use_saved_data,
        table_id_to_info=table_id_to_info,
    )
    with set_environment_variable("FEATUREBYTE_SDK_EXECUTION_MODE", "SERVER"):
        exec(sdk_code, {}, local_vars)

    output = local_vars["output"]
    if isinstance(output, AbstractTableData):
        output = output.frame

    # compare the output
    pruned_graph, node = output.extract_pruned_graph_and_node()
    expected_pruned_graph, expected_node = query_object.extract_pruned_graph_and_node()
    expected_pruned_graph = _replace_view_mode_to_manual(expected_pruned_graph)

    # check the final node hash value of two graph to make sure they are the same
    assert (
        pruned_graph.node_name_to_ref[node.name]
        == expected_pruned_graph.node_name_to_ref[expected_node.name]
    ), sdk_code

    if fixture_path:
        feature_store_id = api_object.feature_store.id
        if update_fixtures:
            # escape certain characters so that jinja can handle it properly
            sdk_version = get_version()
            formatted_sdk_code = sdk_code.replace("{", "{{").replace("}", "}}")
            formatted_sdk_code = formatted_sdk_code.replace(
                f"SDK version: {sdk_version}", "SDK version: {sdk_version}"
            )

            # convert the object id to a placeholder
            formatted_sdk_code = formatted_sdk_code.replace(
                f"{feature_store_id}", "{feature_store_id}"
            )
            if table_id:
                formatted_sdk_code = formatted_sdk_code.replace(f"{table_id}", "{table_id}")

            # for item table, there is an additional `event_table_id`. Replace the actual `event_table_id` value
            # to {event_table_id} placeholder through kwargs
            for key, value in kwargs.items():
                formatted_sdk_code = formatted_sdk_code.replace(
                    f"{value}", "{key}".replace("key", key)
                )

            with open(fixture_path, mode="w", encoding="utf-8") as file_handle:
                file_handle.write(formatted_sdk_code)
        else:
            with open(fixture_path, mode="r", encoding="utf-8") as file_handle:
                expected = file_handle.read().format(
                    sdk_version=get_version(),
                    feature_store_id=feature_store_id,
                    table_id=table_id,
                    **kwargs,
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


def truncate_timestamps(df):
    """
    Truncate datetime64[ns] dtype columns to datetime64[us] dtype

    Parameters
    ----------
    df: DataFrame
        DataFrame to truncate timestamps for

    Returns
    -------
    DataFrame
    """
    df = df.copy()
    for column in df.columns:
        if df[column].dtype.name == "datetime64[ns]":
            df[column] = df[column].astype("datetime64[us]")
    return df


def fb_assert_frame_equal(
    df,
    df_expected,
    dict_like_columns=None,
    sort_by_columns=None,
    ignore_columns=None,
):
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
    sort_by_columns : list | None
        List of columns to sort by before comparing
    ignore_columns : list | None
        Exclude these columns from comparison
    """
    df = truncate_timestamps(df)
    df_expected = truncate_timestamps(df_expected)
    assert df.columns.tolist() == df_expected.columns.tolist()

    if sort_by_columns is not None:
        df = df.sort_values(by=sort_by_columns).reset_index(drop=True)
        df_expected = df_expected.sort_values(by=sort_by_columns).reset_index(drop=True)

    if ignore_columns is not None:
        df = df.drop(columns=ignore_columns)
        df_expected = df_expected.drop(columns=ignore_columns)

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


def assert_sql_equal(actual, expected):
    """
    Compare sql potentially with multiple lines. Leading and trailing spaces and newlines are
    stripped before comparison.
    """
    actual = textwrap.dedent(actual).strip()
    expected = textwrap.dedent(expected).strip()
    assert actual == expected


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


def make_online_request(client, deployment, entity_serving_names):
    """
    Helper function to make an online request via REST API
    """
    data = OnlineFeaturesRequestPayload(entity_serving_names=entity_serving_names)
    res = client.post(
        f"/deployment/{deployment.id}/online_features",
        json=data.json_dict(),
    )
    return res


async def create_observation_table_from_dataframe(
    session, df, data_source, serving_names_mapping=None
):
    """
    Create an ObservationTable from a pandas DataFrame
    """
    unique_id = ObjectId()
    db_table_name = f"df_{unique_id}"
    await session.register_table(db_table_name, df)
    return data_source.get_source_table(
        db_table_name,
        database_name=session.database_name,
        schema_name=session.schema_name,
    ).create_observation_table(
        f"observation_table_{unique_id}", columns_rename_mapping=serving_names_mapping
    )


async def create_batch_request_table_from_dataframe(session, df, data_source):
    """
    Create a BatchRequestTable from a pandas DataFrame
    """
    unique_id = ObjectId()
    db_table_name = f"df_{unique_id}"
    await session.register_table(db_table_name, df)
    return data_source.get_source_table(
        db_table_name,
        database_name=session.database_name,
        schema_name=session.schema_name,
    ).create_batch_request_table(f"batch_request_table_{unique_id}")


async def get_dataframe_from_materialized_table(session, materialized_table):
    """
    Retrieve pandas DataFrame from a materialized table
    """
    query = sql_to_string(
        expressions.select("*").from_(
            get_fully_qualified_table_name(materialized_table.location.table_details.model_dump())
        ),
        source_type=session.source_type,
    )
    return await session.execute_query(query)


def create_observation_table_by_upload(df):
    """
    Create an Observation Table using the upload SDK method
    """
    with tempfile.NamedTemporaryFile() as temp_file:
        filename = temp_file.name + ".parquet"
        df.to_parquet(filename, index=False)
        observation_table = fb.ObservationTable.upload(filename, name=str(ObjectId()))
    return observation_table


async def compute_historical_feature_table_dataframe_helper(
    feature_list, df_observation_set, session, data_source, input_format, return_id=False, **kwargs
):
    """
    Helper to call compute_historical_feature_table using DataFrame as input, converted to an
    intermediate observation table
    """
    if input_format == "table":
        observation_table = await create_observation_table_from_dataframe(
            session,
            df_observation_set,
            data_source,
            serving_names_mapping=kwargs.get("serving_names_mapping"),
        )
    elif input_format == "uploaded_table":
        observation_table = create_observation_table_by_upload(df_observation_set)
    else:
        assert input_format == "dataframe"
        observation_table = df_observation_set

    historical_feature_table_name = f"historical_feature_table_{ObjectId()}"
    historical_feature_table = feature_list.compute_historical_feature_table(
        observation_table, historical_feature_table_name, **kwargs
    )
    df_historical_features = await get_dataframe_from_materialized_table(
        session, historical_feature_table
    )
    if not return_id:
        return df_historical_features
    return df_historical_features, historical_feature_table.id


def check_observation_table_creation_query(query, expected):
    """
    Helper function to check that the query to create an observation table is correct
    """
    # Check that the correct query was executed (the table name should be prefixed by
    # OBSERVATION_TABLE followed by an ObjectId)
    observation_table_name_pattern = r"OBSERVATION_TABLE_\w{24}"
    assert re.search(observation_table_name_pattern, query)

    # Remove the dynamic component before comparing
    query = re.sub(r"OBSERVATION_TABLE_\w{24}", "OBSERVATION_TABLE", query)
    expected = textwrap.dedent(expected).strip()
    assert query == expected


def get_preview_sql_for_series(series_obj, *args, **kwargs):
    """
    Helper function to get the preview SQL for a series
    """
    return series_obj.preview_sql(*args, **kwargs)


def check_decomposed_graph_output_node_hash(feature_model, output=None):
    """Check the output node hash of the decomposed graph is the same as the original graph"""
    if output is None:
        transformer = OfflineStoreIngestQueryGraphTransformer(graph=feature_model.graph)
        output = transformer.transform(
            target_node=feature_model.node,
            relationships_info=feature_model.relationships_info,
            feature_name=feature_model.name,
            feature_version=feature_model.version.to_str(),
        )

    if output.is_decomposed is False:
        assert output.node_name_map == {}
        return

    # check number of leaf nodes (node without incoming edges)
    decom_graph = output.graph
    leaf_nodes = set(decom_graph.nodes_map).difference(decom_graph.edges_map)
    assert len(leaf_nodes) == 1, "Decomposed graph should have only one leaf node"

    aggregation_nodes = {
        NodeType.GROUPBY,
        NodeType.ITEM_GROUPBY,
        NodeType.AGGREGATE_AS_AT,
        NodeType.LOOKUP,
        NodeType.LOOKUP_TARGET,
        NodeType.JOIN,
        NodeType.JOIN_FEATURE,
    }

    # check the graph node & it should contain at least one aggregation node
    for graph_node in decom_graph.iterate_sorted_graph_nodes(
        graph_node_types={GraphNodeType.OFFLINE_STORE_INGEST_QUERY}
    ):
        nested_graph = graph_node.parameters.graph
        has_agg_node = False
        for node in nested_graph.iterate_sorted_nodes():
            if node.type in aggregation_nodes:
                has_agg_node = True
                break
        assert has_agg_node, "Decomposed graph should contain at least one aggregation node"

    # check all the original graph's node_name can be found in node_name_map
    assert set(feature_model.graph.nodes_map.keys()) == set(output.node_name_map.keys())

    # check that no unexpected node type is present in the decomposed graph
    unexpected_node_types = aggregation_nodes.union({NodeType.INPUT})
    for node in output.graph.nodes:
        assert node.type not in unexpected_node_types, "Unexpected node type in decomposed graph"

    # flatten the original graph
    flattened_graph, node_name_map = feature_model.graph.flatten()
    flattened_node_name = node_name_map[feature_model.node_name]

    # flatten the decomposed graph
    decom_node_name = output.node_name_map[feature_model.node_name]
    flattened_decom_graph, node_name_map = QueryGraph(
        **output.graph.model_dump(by_alias=True)
    ).flatten()
    flattened_decom_node_name = node_name_map[decom_node_name]

    # check the output node hash is the same
    original_out_hash = flattened_graph.node_name_to_ref[flattened_node_name]
    decom_out_hash = flattened_decom_graph.node_name_to_ref[flattened_decom_node_name]
    assert original_out_hash == decom_out_hash


def generate_column_data(var_type, row_number=10, databricks_udf_input=False):
    """Generate data for a given var_type"""
    if var_type in DBVarType.supported_timestamp_types():
        return pd.date_range("2020-01-01", freq="1h", periods=row_number).astype(str)
    if var_type in DBVarType.FLOAT:
        return np.random.uniform(size=row_number)
    if var_type in DBVarType.INT:
        return np.random.randint(0, 10, size=row_number)
    if var_type in DBVarType.VARCHAR:
        return np.random.choice(["foo", "bar"], size=row_number)
    if var_type in DBVarType.BOOL:
        return np.random.choice([True, False], size=row_number)
    if var_type in DBVarType.dictionary_types().union([DBVarType.FLAT_DICT]):
        selections = [
            None,
            np.nan,
            {} if databricks_udf_input else json.dumps({}),
            {"foo": 1, "bar": 2} if databricks_udf_input else json.dumps({"foo": 1, "bar": 2}),
            {"你好": 1, "世界": 2} if databricks_udf_input else json.dumps({"你好": 1, "世界": 2}),
            (
                {"foo": 1, "bar": 2, "baz": 3}
                if databricks_udf_input
                else json.dumps({"foo": 1, "bar": 2, "baz": 3})
            ),
        ]
        return np.random.choice(selections, size=row_number)
    raise ValueError(f"Unsupported var_type: {var_type}")


@contextmanager
def add_sys_path(path: str) -> Generator[None, None, None]:
    """
    Temporarily add the given path to `sys.path`.

    Parameters
    ----------
    path: str
        Path to add to `sys.path`

    Yields
    ------
    None
        This context manager yields nothing and is used for its side effects only.
    """
    path = os.fspath(path)
    sys.path.insert(0, path)
    try:
        yield
    finally:
        sys.path.remove(path)


def get_on_demand_feature_function(codes, function_name):
    """Construct on demand feature function based on the input codes"""
    # create a temporary directory to write the generated code
    with tempfile.TemporaryDirectory() as temp_dir:
        with add_sys_path(temp_dir):
            # write the generated code to a file
            module_name = f"mod_{ObjectId()}"  # use a random module name to avoid conflict
            with open(os.path.join(temp_dir, f"{module_name}.py"), "w") as file_handle:
                file_handle.write(codes)

            # import the function & execute it on the input dataframe, check output column name
            module = importlib.import_module(module_name)
            return getattr(module, function_name)


def check_on_demand_feature_view_code_execution(odfv_codes, df, function_name):
    """Check on demand feature view code execution"""
    on_demand_feature_func = get_on_demand_feature_function(odfv_codes, function_name)
    try:
        output = on_demand_feature_func(df)
        return output
    except Exception as e:
        print("Error: ", e)
        print("Generated code: ", odfv_codes)
        raise e


def check_on_demand_feature_function_code_execution(udf_code_state, df):
    """Check on demand feature function code execution"""
    udf_codes = udf_code_state.generate_code()

    # get input parameters
    input_args = []
    for _, row in df.iterrows():
        input_kwargs = {
            input_arg_name: row[input_info.column_name]
            for input_arg_name, input_info in sorted(udf_code_state.input_var_name_to_info.items())
        }
        input_args.append(input_kwargs)

    user_defined_func = get_on_demand_feature_function(udf_codes, "user_defined_function")
    try:
        output = []
        for input_arg in input_args:
            output.append(user_defined_func(**input_arg))
        return pd.Series(output)
    except Exception as e:
        print("Error: ", e)
        print("Generated code: ", udf_codes)
        raise e


def check_on_demand_feature_code_generation(
    feature_model, sql_fixture_path=None, update_fixtures=False
):
    """Check on demand feature view code generation"""
    offline_store_info = feature_model.offline_store_info
    assert offline_store_info.is_decomposed, "OfflineStoreInfo is not decomposed"

    # construct input dataframe for testing
    df = pd.DataFrame()
    decomposed_graph = offline_store_info.graph
    target_node = decomposed_graph.get_node_by_name(offline_store_info.node_name)
    databricks_specific_inputs = set()
    for graph_node in decomposed_graph.iterate_nodes(
        target_node=target_node, node_type=NodeType.GRAPH
    ):
        assert isinstance(graph_node.parameters, OfflineStoreIngestQueryGraphNodeParameters)
        df[graph_node.parameters.output_column_name] = generate_column_data(
            graph_node.parameters.output_dtype
        )
        if graph_node.parameters.output_dtype in DBVarType.dictionary_types():
            databricks_specific_inputs.add(graph_node.parameters.output_column_name)

    for node in decomposed_graph.iterate_nodes(
        target_node=target_node, node_type=NodeType.REQUEST_COLUMN
    ):
        assert isinstance(node, RequestColumnNode)
        df[node.parameters.column_name] = generate_column_data(node.parameters.dtype)

    # add a timestamp column & a feature timestamp column
    if "POINT_IN_TIME" not in df.columns:
        df["POINT_IN_TIME"] = pd.date_range("2020-01-01", freq="1h", periods=df.shape[0])

    feat_event_ts = pd.to_datetime(df["POINT_IN_TIME"]) - pd.Timedelta(seconds=10)
    df["__feature_timestamp"] = feat_event_ts

    for col in df.columns:
        if col != "POINT_IN_TIME":
            df[f"{col}__ts"] = feat_event_ts.astype(int) // 1e9

    # introduce some missing values to test null handling for datetime
    if df.shape[0] > 1:
        df.loc[df.POINT_IN_TIME == df.POINT_IN_TIME.max(), "POINT_IN_TIME"] = None

    # generate on demand feature view code
    odfv_codes = offline_store_info.odfv_info.codes

    # check the generated code can be executed successfully
    odfv_output = check_on_demand_feature_view_code_execution(
        odfv_codes, df, function_name=offline_store_info.odfv_info.function_name
    )
    exp_col_name = feature_model.versioned_name
    assert odfv_output.columns == [exp_col_name]

    udf_code_state = offline_store_info._generate_databricks_user_defined_function_code(
        output_dtype=feature_model.dtype,
    )

    # check the generated code can be executed successfully
    for col in databricks_specific_inputs:
        df[col] = df[col].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
    udf_output = check_on_demand_feature_function_code_execution(udf_code_state, df)

    # check the consistency between on demand feature view & on demand feature function
    pd.testing.assert_series_equal(
        odfv_output[exp_col_name], udf_output, check_names=False, check_dtype=False
    )

    # check generated sql code
    if sql_fixture_path:
        udf_codes = offline_store_info.generate_databricks_user_defined_function_code(
            output_dtype=feature_model.dtype, to_sql=True
        )
        if update_fixtures:
            with open(sql_fixture_path, mode="w", encoding="utf-8") as file_handle:
                file_handle.write(udf_codes)
        else:
            with open(sql_fixture_path, mode="r", encoding="utf-8") as file_handle:
                expected = file_handle.read()
                assert expected.strip() == udf_codes.strip(), udf_codes


async def deploy_feature_list(
    app_container,
    feature_list_model,
    context_primary_entity_ids=None,
    deployment_name_override=None,
    deployment_id=None,
):
    """
    Helper function to deploy a feature list using services
    """
    if context_primary_entity_ids is None:
        context_primary_entity_ids = feature_list_model.primary_entity_ids
    data = ContextCreate(
        name=str(ObjectId()),
        primary_entity_ids=context_primary_entity_ids,
    )
    context_model = await app_container.context_service.create_document(data)
    data = UseCaseCreate(
        name=str(ObjectId()),
        context_id=context_model.id,
        target_namespace_id=ObjectId(),
    )
    use_case_model = await app_container.use_case_service.create_document(data)
    await app_container.deploy_service.create_deployment(
        feature_list_id=feature_list_model.id,
        deployment_id=deployment_id,
        deployment_name=(
            feature_list_model.name
            if deployment_name_override is None
            else deployment_name_override
        ),
        to_enable_deployment=True,
        use_case_id=use_case_model.id,
    )
    return await app_container.feature_list_service.get_document(feature_list_model.id)


async def deploy_feature_ids(
    app_container,
    feature_list_name,
    feature_ids,
    context_primary_entity_ids=None,
    deployment_id=None,
):
    """
    Helper function to deploy a list of features using services
    """
    for feature_id in feature_ids:
        # Update readiness to production ready
        await app_container.feature_readiness_service.update_feature(
            feature_id=feature_id, readiness="PRODUCTION_READY", ignore_guardrails=True
        )

    data = FeatureListServiceCreate(name=feature_list_name, feature_ids=feature_ids)
    feature_list_model = await app_container.feature_list_service.create_document(data)
    return await deploy_feature_list(
        app_container=app_container,
        feature_list_model=feature_list_model,
        context_primary_entity_ids=context_primary_entity_ids,
        deployment_id=deployment_id,
    )


async def deploy_feature(
    app_container,
    feature,
    return_type="feature",
    feature_list_name_override=None,
    context_primary_entity_ids=None,
    deployment_id=None,
):
    """
    Helper function to create deploy a single feature using services
    """
    assert return_type in {"feature", "feature_list"}

    # Create feature
    if not feature.saved:
        feature_create_payload = FeatureServiceCreate(**feature._get_create_payload())
        await app_container.feature_service.create_document(data=feature_create_payload)

    # Create feature list and deploy
    if feature_list_name_override is None:
        feature_list_name = f"{feature.name}_list"
    else:
        feature_list_name = feature_list_name_override
    feature_list_model = await deploy_feature_ids(
        app_container,
        feature_list_name,
        [feature.id],
        context_primary_entity_ids=context_primary_entity_ids,
        deployment_id=deployment_id,
    )
    if return_type == "feature":
        return await app_container.feature_service.get_document(feature.id)
    return await app_container.feature_list_service.get_document(feature_list_model.id)


def undeploy_feature(feature):
    """Helper function to undeploy a single feature"""
    deployment: Deployment = Deployment.get(f"{feature.name}_list")
    deployment.disable()


async def undeploy_feature_async(feature, app_container):
    """
    Helper function to undeploy a feature. This version can control storage used through the
    app_container parameter.
    """
    async for deployment in app_container.deployment_service.list_documents_iterator(
        query_filter={"name": f"{feature.name}_list"}
    ):
        await app_container.deploy_service.update_deployment(
            deployment_id=deployment.id, to_enable_deployment=False
        )


def tz_localize_if_needed(df, source_type):
    """
    Helper function to localize datetime columns for easier comparison.

    Parameters
    ----------
    df: DataFrame
        Result of feature requests (preview / historical)
    source_type: SourceType
        Source type
    """
    if source_type not in {SourceType.DATABRICKS, SourceType.DATABRICKS_UNITY}:
        # Only databricks needs timezone localization
        return

    df["POINT_IN_TIME"] = df["POINT_IN_TIME"].dt.tz_localize(None)


def assert_dict_approx_equal(actual, expected):
    """Assert two dicts are approximately equal"""
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key in expected:
            assert_dict_approx_equal(actual[key], expected[key])
    elif isinstance(expected, list):
        assert len(actual) == len(expected)
        for act_item, exp_item in zip(actual, expected):
            assert_dict_approx_equal(act_item, exp_item)
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected)
    else:
        assert actual == expected


def dict_to_tuple(d):
    """Recursively convert dictionary to a hashable tuple."""
    if isinstance(d, dict):
        return tuple((k, dict_to_tuple(v)) for k, v in sorted(d.items()))
    if isinstance(d, list):
        return tuple(sorted(dict_to_tuple(v) for v in d))
    return d


def assert_lists_of_dicts_equal(list1, list2):
    """Compare two lists of dictionaries without considering the order, handling nested structures."""
    set1 = {dict_to_tuple(d) for d in list1}
    set2 = {dict_to_tuple(d) for d in list2}
    assert set1 == set2, f"Expected {set1}, got {set2}"


def extract_session_executed_queries(mock_snowflake_session, func="execute_query_long_running"):
    """
    Helper to extract executed queries from mock_snowflake_session
    """
    assert func in {"execute_query_long_running", "execute_query"}
    queries = []
    for call_obj in getattr(mock_snowflake_session, func).call_args_list:
        args, _ = call_obj
        queries.append(args[0] + ";")
    return "\n\n".join(queries)


def deploy_features_through_api(features):
    """Create a feature list and deploy the feature list."""
    feature_list = FeatureList(features, name=f"feature_list_{ObjectId()}")
    feature_list.save()

    deployment = feature_list.deploy(make_production_ready=True, ignore_guardrails=True)
    deployment.enable()


async def get_relationship_info(app_container, child_entity_id, parent_entity_id):
    """
    Helper function to retrieve a relationship between two entities
    """
    async for info in app_container.relationship_info_service.list_documents_iterator(
        query_filter={"entity_id": child_entity_id, "related_entity_id": parent_entity_id}
    ):
        return EntityRelationshipInfo(**info.model_dump(by_alias=True))
    raise AssertionError("Relationship not found")


def check_null_filling_value(graph, node_name, expected_value):
    """Check that the null filling value is correctly extracted from the graph"""
    node = graph.get_node_by_name(node_name)
    state = NullFillingValueExtractor(graph=graph).extract(node=node)
    assert state.fill_value == expected_value, state


@asynccontextmanager
async def manage_document(doc_service, create_data, storage):
    """Asynchronously create and delete a document on exit."""
    doc = None
    try:
        # Asynchronously create the document and yield control back to the caller
        doc = await doc_service.create_document(data=create_data)

        # check remote paths are created
        for path in type(doc)._get_remote_attribute_paths(doc.model_dump(by_alias=True)):
            full_path = os.path.join(storage.base_path, path)
            assert os.path.exists(full_path), f"Remote path {full_path} not created"

        yield doc
    finally:
        if doc:
            # Ensure the document is deleted even if an exception occurs
            await doc_service.delete_document(document_id=doc.id)

            # check remote paths are deleted
            for path in type(doc)._get_remote_attribute_paths(doc.model_dump(by_alias=True)):
                full_path = os.path.join(storage.base_path, path)
                assert not os.path.exists(full_path), f"Remote path {full_path} not deleted"


def compare_pydantic_obj(maybe_pydantic_obj, expected):
    """Compare pydantic object with dict"""
    if isinstance(maybe_pydantic_obj, BaseModel):
        maybe_pydantic_obj = maybe_pydantic_obj.model_dump(by_alias=True)
    if isinstance(expected, BaseModel):
        expected = expected.model_dump(by_alias=True)

    if isinstance(maybe_pydantic_obj, list) and isinstance(expected, list):
        assert len(maybe_pydantic_obj) == len(expected)
        for left, right in zip(maybe_pydantic_obj, expected):
            compare_pydantic_obj(left, right)
    else:
        assert maybe_pydantic_obj == expected, f"Expected {expected}, got {maybe_pydantic_obj}"


def inject_request_side_effect(mock_request, client):
    """Inject side effect for mock_request to use the client for making requests"""

    def _request_func(*args, **kwargs):
        if "allow_redirects" in kwargs:
            kwargs["follow_redirects"] = kwargs.pop("allow_redirects")
        return client.request(*args, **kwargs)

    mock_request.side_effect = _request_func
    return mock_request


def get_sql_adapter_from_source_type(source_type):
    """
    Get SQL adapter from source type
    """
    return get_sql_adapter(
        SourceInfo(source_type=source_type, database_name="my_db", schema_name="my_schema")
    )


@contextmanager
def safe_freeze_time_for_mod(target_mod, frozen_datetime):
    """
    Manually patch datetime for a specific module without freezegun
    """
    with patch(target_mod) as patched_datetime:
        datetime_obj = pd.Timestamp(frozen_datetime).to_pydatetime()
        patched_datetime.utcnow.return_value = datetime_obj
        patched_datetime.today.return_value = datetime_obj.date()
        yield


@contextmanager
def safe_freeze_time(frozen_datetime):
    """
    Freeze time workaround due to freezegun not working well with pydantic in some cases
    """
    mod_lists = [
        "featurebyte.common.model_util.datetime",
        "featurebyte.service.feature_materialize.datetime",
        "featurebyte.worker.task.observation_table.datetime",
    ]
    with ExitStack() as stack:
        for mod in mod_lists:
            stack.enter_context(safe_freeze_time_for_mod(mod, frozen_datetime))
        yield


def feature_query_to_string(feature_query):
    """
    Helper function that given a FeatureQuery, return the full query string
    """
    queries = []
    for temp_table_query in feature_query.temp_table_queries:
        queries.append(temp_table_query.sql)
    queries.append(feature_query.feature_table_query.sql)
    return ";\n\n".join(queries)


def feature_query_set_to_string(
    feature_query_set,
    nodes,
    source_info,
    num_features_per_query=20,
    nodes_group_override=None,
):
    """
    Helper function that given a FeatureQuerySet, return the full query string concatenating all the
    feature queries and the final output query.
    """
    generator = feature_query_set.feature_query_generator
    node_names = [node.name for node in nodes]
    if nodes_group_override:
        nodes_group = nodes_group_override
    else:
        nodes_group = generator.split_nodes(node_names, num_features_per_query, source_info)
    queries = []
    for i, nodes in enumerate(nodes_group):
        feature_query = generator.generate_feature_query(
            [node.name for node in nodes], f"__TEMP_{i}"
        )
        for temp_table_query in feature_query.temp_table_queries:
            queries.append(temp_table_query.sql)
        queries.append(feature_query.feature_table_query.sql)
        feature_query_set.add_completed_feature_query(feature_query)
    queries.append(feature_query_set.construct_output_query(source_info))
    return ";\n\n".join(queries)
