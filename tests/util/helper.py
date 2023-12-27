"""
This module contains utility functions used in tests
"""
import importlib
import json
import os
import re
import sys
import tempfile
import textwrap
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest
from bson import ObjectId, json_util
from pandas.core.dtypes.common import is_numeric_dtype
from sqlglot import expressions

from featurebyte import get_version
from featurebyte.api.deployment import Deployment
from featurebyte.api.source_table import AbstractTableData
from featurebyte.common.env_util import add_sys_path
from featurebyte.core.generic import QueryObject
from featurebyte.core.mixin import SampleMixin
from featurebyte.enum import AggFunc, DBVarType
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalGraphState, GlobalQueryGraph, QueryGraph
from featurebyte.query_graph.node.nested import OfflineStoreIngestQueryGraphNodeParameters
from featurebyte.query_graph.node.request import RequestColumnNode
from featurebyte.query_graph.sql.common import get_fully_qualified_table_name, sql_to_string
from featurebyte.query_graph.transform.offline_store_ingest import (
    OfflineStoreIngestQueryGraphTransformer,
)
from featurebyte.query_graph.util import get_aggregation_identifier, get_tile_table_identifier
from featurebyte.schema.feature import FeatureServiceCreate
from featurebyte.schema.feature_list import FeatureListServiceCreate, OnlineFeaturesRequestPayload


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
    node = graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params={
            **groupby_node_params,
            "tile_id": get_tile_table_identifier("deadbeef1234", groupby_node_params)
            if override_tile_id is None
            else override_tile_id,
            "aggregation_id": get_aggregation_identifier(
                graph.node_name_to_ref[input_node.name], groupby_node_params
            )
            if override_aggregation_id is None
            else override_aggregation_id,
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
    pruned_graph_dict = pruned_graph.dict()
    for node in pruned_graph_dict["nodes"]:
        if node["type"] == NodeType.GRAPH:
            if "view_mode" in node["parameters"]["metadata"]:
                node["parameters"]["metadata"]["view_mode"] = "manual"
    return QueryGraph(**pruned_graph_dict)


def check_sdk_code_generation(  # pylint: disable=too-many-locals
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
    exec(sdk_code, {}, local_vars)  # pylint: disable=exec-used
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


def fb_assert_frame_equal(df, df_expected, dict_like_columns=None, sort_by_columns=None):
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
    """
    assert df.columns.tolist() == df_expected.columns.tolist()

    if sort_by_columns is not None:
        df = df.sort_values(by=sort_by_columns).reset_index(drop=True)
        df_expected = df_expected.sort_values(by=sort_by_columns).reset_index(drop=True)

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
    await session.register_table(db_table_name, df, temporary=False)
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
    await session.register_table(db_table_name, df, temporary=False)
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
            get_fully_qualified_table_name(materialized_table.location.table_details.dict())
        ),
        source_type=session.source_type,
    )
    return await session.execute_query(query)


async def compute_historical_feature_table_dataframe_helper(
    feature_list, df_observation_set, session, data_source, input_format, **kwargs
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
    else:
        observation_table = df_observation_set

    historical_feature_table_name = f"historical_feature_table_{ObjectId()}"
    historical_feature_table = feature_list.compute_historical_feature_table(
        observation_table, historical_feature_table_name, **kwargs
    )
    df_historical_features = await get_dataframe_from_materialized_table(
        session, historical_feature_table
    )
    return df_historical_features


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
    return SampleMixin.preview_sql(series_obj, *args, **kwargs)


def check_decomposed_graph_output_node_hash(feature_model, output=None):
    """Check the output node hash of the decomposed graph is the same as the original graph"""
    if output is None:
        transformer = OfflineStoreIngestQueryGraphTransformer(graph=feature_model.graph)
        output = transformer.transform(
            target_node=feature_model.node,
            entity_id_to_serving_name={},
            relationships_info=feature_model.relationships_info,
            feature_name=feature_model.name,
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
    flattened_decom_graph, node_name_map = QueryGraph(**output.graph.dict(by_alias=True)).flatten()
    flattened_decom_node_name = node_name_map[decom_node_name]

    # check the output node hash is the same
    original_out_hash = flattened_graph.node_name_to_ref[flattened_node_name]
    decom_out_hash = flattened_decom_graph.node_name_to_ref[flattened_decom_node_name]
    assert original_out_hash == decom_out_hash


def generate_column_data(var_type, row_number=10):
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
            json.dumps({}),
            json.dumps({"foo": 1, "bar": 2}),
            json.dumps({"你好": 1, "世界": 2}),
            json.dumps({"foo": 1, "bar": 2, "baz": 3}),
        ]
        return np.random.choice(selections, size=row_number)
    raise ValueError(f"Unsupported var_type: {var_type}")


def check_on_demand_feature_view_code_execution(odfv_codes, df):
    """Check on demand feature view code execution"""
    # create a temporary directory to write the generated code
    with tempfile.TemporaryDirectory() as temp_dir:
        with add_sys_path(temp_dir):
            # write the generated code to a file
            module_name = f"odfv_{ObjectId()}"  # use a random module name to avoid conflict
            with open(os.path.join(temp_dir, f"{module_name}.py"), "w") as file_handle:
                file_handle.write(odfv_codes)

            # import the function & execute it on the input dataframe, check output column name
            module = importlib.import_module(module_name)
            try:
                output = module.on_demand_feature_view(df)
                return output
            except Exception as e:
                print("Error: ", e)
                print("Generated code: ", odfv_codes)
                raise e


def check_on_demand_feature_view_code_generation(feature_model):
    """Check on demand feature view code generation"""
    offline_store_info = feature_model.offline_store_info
    assert offline_store_info.is_decomposed, "OfflineStoreInfo is not decomposed"

    # construct input dataframe for testing
    df = pd.DataFrame()
    decomposed_graph = offline_store_info.graph
    target_node = decomposed_graph.get_node_by_name(offline_store_info.node_name)
    for graph_node in decomposed_graph.iterate_nodes(
        target_node=target_node, node_type=NodeType.GRAPH
    ):
        assert isinstance(graph_node.parameters, OfflineStoreIngestQueryGraphNodeParameters)
        df[graph_node.parameters.output_column_name] = generate_column_data(
            graph_node.parameters.output_dtype
        )

    for node in decomposed_graph.iterate_nodes(
        target_node=target_node, node_type=NodeType.REQUEST_COLUMN
    ):
        assert isinstance(node, RequestColumnNode)
        df[node.parameters.column_name] = generate_column_data(node.parameters.dtype)

    # generate on demand feature view code
    odfv_codes = offline_store_info.generate_on_demand_feature_view_code(
        feature_name=feature_model.name
    )

    # check the generated code can be executed successfully
    output = check_on_demand_feature_view_code_execution(odfv_codes, df)
    assert output.columns == [feature_model.name]


async def deploy_feature(app_container, feature, return_type="feature"):
    """
    Helper function to create deploy a single feature using services
    """
    assert return_type in {"feature", "feature_list"}

    # Create feature and make production ready
    feature_create_payload = FeatureServiceCreate(**feature._get_create_payload())
    await app_container.feature_service.create_document(data=feature_create_payload)
    await app_container.feature_readiness_service.update_feature(
        feature_id=feature.id, readiness="PRODUCTION_READY", ignore_guardrails=True
    )

    # Create feature list and deploy
    data = FeatureListServiceCreate(
        name=f"{feature.name}_list",
        feature_ids=[feature.id],
    )
    feature_list_model = await app_container.feature_list_service.create_document(data)
    await app_container.deploy_service.create_deployment(
        feature_list_id=feature_list_model.id,
        deployment_id=ObjectId(),
        deployment_name=feature_list_model.name,
        to_enable_deployment=True,
    )

    if return_type == "feature":
        return await app_container.feature_service.get_document(feature.id)
    return await app_container.feature_list_service.get_document(feature_list_model.id)


def undeploy_feature(feature):
    """
    Helper function to undeploy a single feature
    """
    deployment: Deployment = Deployment.get(f"{feature.name}_list")
    deployment.disable()  # pylint: disable=no-member


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
