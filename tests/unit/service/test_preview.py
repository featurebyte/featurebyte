"""
Test preview service module
"""

import numpy as np
import pandas as pd
import pytest
from bson import ObjectId
from sqlglot import parse_one

from featurebyte import FeatureStore
from featurebyte.common.utils import dataframe_from_json
from featurebyte.exception import (
    DescribeQueryExecutionError,
    MissingPointInTimeColumnError,
    RequiredEntityNotProvidedError,
)
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_list import FeatureCluster
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.feature_list import FeatureListPreview
from featurebyte.schema.feature_store import FeatureStorePreview, FeatureStoreSample
from featurebyte.schema.preview import FeatureOrTargetPreview
from featurebyte.warning import QueryNoLimitWarning
from tests.util.helper import assert_equal_with_expected_fixture, extract_session_executed_queries


@pytest.fixture(name="empty_graph")
def empty_graph_fixture():
    """Fake graph"""
    return {
        "nodes": [],
        "edges": [],
    }


@pytest.fixture(name="feature_store_preview")
def feature_store_preview_fixture(feature_store):
    """
    Fixture for a FeatureStorePreview
    """
    return FeatureStorePreview(
        graph={
            "edges": [{"source": "input_1", "target": "project_1"}],
            "nodes": [
                {
                    "name": "input_1",
                    "output_type": "frame",
                    "parameters": {
                        "type": "event_table",
                        "id": "6332fdb21e8f0eeccc414512",
                        "columns": [
                            {"name": "col_int", "dtype": "INT"},
                            {"name": "col_float", "dtype": "FLOAT"},
                            {"name": "col_char", "dtype": "CHAR"},
                            {"name": "col_text", "dtype": "VARCHAR"},
                            {"name": "col_binary", "dtype": "BINARY"},
                            {"name": "col_boolean", "dtype": "BOOL"},
                            {"name": "event_timestamp", "dtype": "TIMESTAMP"},
                            {"name": "created_at", "dtype": "TIMESTAMP"},
                            {"name": "cust_id", "dtype": "VARCHAR"},
                        ],
                        "table_details": {
                            "database_name": "sf_database",
                            "schema_name": "sf_schema",
                            "table_name": "sf_table",
                        },
                        "feature_store_details": feature_store.json_dict(),
                        "timestamp_column": "event_timestamp",
                        "id_column": "event_timestamp",
                    },
                    "type": "input",
                },
                {
                    "name": "project_1",
                    "output_type": "frame",
                    "parameters": {
                        "columns": [
                            "col_int",
                            "col_float",
                            "col_char",
                            "col_text",
                            "col_binary",
                            "col_boolean",
                            "event_timestamp",
                            "created_at",
                            "cust_id",
                        ]
                    },
                    "type": "project",
                },
            ],
        },
        node_name="project_1",
    )


@pytest.fixture(name="feature_store_preview_project_column")
def feature_store_preview_project_column_fixture(feature_store_preview, project_column):
    """
    Fixture for a FeatureStorePreview with a project column
    """
    project_node = next(
        node for node in feature_store_preview.graph.nodes if node.type == "project"
    )
    project_node.parameters.columns = [project_column]
    return feature_store_preview


@pytest.fixture(name="feature_store_sample")
def feature_store_sample_fixture(feature_store_preview):
    """
    Fixture for a FeatureStoreSample
    """
    return FeatureStoreSample(**feature_store_preview.model_dump())


@pytest.mark.asyncio
async def test_preview_feature__time_based_feature_without_point_in_time_errors(
    feature_preview_service, float_feature
):
    """
    Test preview feature
    """
    feature_preview = FeatureOrTargetPreview(
        feature_store_name="feature_store_name",
        point_in_time_and_serving_name_list=[{}],
        graph=float_feature.graph,
        node_name=float_feature.node_name,
    )
    with pytest.raises(MissingPointInTimeColumnError) as exc:
        await feature_preview_service.preview_target_or_feature(feature_preview)
    assert "Point in time column not provided" in str(exc)


@pytest.mark.asyncio
async def test_preview_feature__non_time_based_feature_without_point_in_time_doesnt_error(
    feature_preview_service, transaction_entity, non_time_based_feature, insert_credential
):
    """
    Test preview feature
    """
    _ = transaction_entity, insert_credential
    feature_preview = FeatureOrTargetPreview(
        feature_store_name="sf_featurestore",
        point_in_time_and_serving_name_list=[
            {
                "transaction_id": 1,
            }
        ],
        graph=non_time_based_feature.graph,
        node_name=non_time_based_feature.node_name,
        feature_store_id=non_time_based_feature.tabular_source.feature_store_id,
    )
    await feature_preview_service.preview_target_or_feature(feature_preview)


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_preview_feature__missing_entity(feature_preview_service, production_ready_feature):
    """
    Test preview feature but without providing the required entity
    """
    feature_preview = FeatureOrTargetPreview(
        feature_store_name="sf_featurestore",
        point_in_time_and_serving_name_list=[
            {
                "POINT_IN_TIME": "2022-05-01",
                "abc": 1,
            }
        ],
        graph=production_ready_feature.graph,
        node_name=production_ready_feature.node_name,
        feature_store_id=production_ready_feature.tabular_source.feature_store_id,
    )
    with pytest.raises(RequiredEntityNotProvidedError) as exc:
        await feature_preview_service.preview_target_or_feature(feature_preview)
    expected = (
        'Required entities are not provided in the request: customer (serving name: "cust_id")'
    )
    assert str(exc.value) == expected


@pytest.mark.asyncio
async def test_preview_featurelist__time_based_feature_errors_without_time(
    feature_preview_service, float_feature
):
    """
    Test preview featurelist
    """
    feature_list_preview = FeatureListPreview(
        feature_store_name="sf_featurestore",
        feature_clusters=[
            FeatureCluster(
                feature_store_id=PydanticObjectId(ObjectId()),
                graph=float_feature.graph,
                node_names=[float_feature.node_name],
            ),
        ],
        point_in_time_and_serving_name_list=[
            {
                "event_id_col": 1,
            }
        ],
    )
    with pytest.raises(MissingPointInTimeColumnError) as exc:
        await feature_preview_service.preview_featurelist(feature_list_preview)
    assert "Point in time column not provided" in str(exc)


@pytest.mark.asyncio
async def test_preview_featurelist__non_time_based_feature_no_error_without_time(
    feature_preview_service, transaction_entity, non_time_based_feature, insert_credential
):
    """
    Test preview featurelist
    """
    _ = transaction_entity, insert_credential
    store = FeatureStore.get("sf_featurestore")
    feature_list_preview = FeatureListPreview(
        feature_clusters=[
            FeatureCluster(
                feature_store_id=store.id,
                graph=non_time_based_feature.graph,
                node_names=[non_time_based_feature.node_name],
            ),
        ],
        point_in_time_and_serving_name_list=[
            {
                "transaction_id": 1,
            }
        ],
    )
    await feature_preview_service.preview_featurelist(feature_list_preview)


@pytest.mark.asyncio
async def test_preview_featurelist__missing_entity(
    feature_preview_service, production_ready_feature_list
):
    """
    Test preview featurelist but without providing the required entity
    """
    feature_list_preview = FeatureListPreview(
        feature_store_name="sf_featurestore",
        feature_clusters=production_ready_feature_list.feature_clusters,
        point_in_time_and_serving_name_list=[
            {
                "POINT_IN_TIME": "2022-05-01",
                "abc": 1,
            }
        ],
    )
    with pytest.raises(RequiredEntityNotProvidedError) as exc:
        await feature_preview_service.preview_featurelist(feature_list_preview)
    expected = (
        'Required entities are not provided in the request: customer (serving name: "cust_id")'
    )
    assert str(exc.value) == expected


async def mock_execute_query(query):
    """Execute query mock"""
    if "%missing" not in query:
        return pd.DataFrame({"count": [100]})
    result_column_names = [
        col.alias_or_name for col in parse_one(query, read="snowflake").expressions
    ]
    # Make %missing stats null in all columns
    data = {col: ["some_value"] if "%missing" not in col else [None] for col in result_column_names}
    return pd.DataFrame(data)


@pytest.mark.parametrize("drop_all_null_stats", [False, True])
@pytest.mark.asyncio
async def test_describe_drop_all_null_stats(
    preview_service,
    feature_store_sample,
    mock_snowflake_session,
    drop_all_null_stats,
):
    """
    Test describe
    """

    mock_snowflake_session.execute_query.side_effect = mock_execute_query
    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query
    result = dataframe_from_json(
        await preview_service.describe(
            feature_store_sample,
            size=5000,
            seed=0,
            drop_all_null_stats=drop_all_null_stats,
        )
    )
    if drop_all_null_stats:
        assert "%missing" not in result.index
    else:
        assert "%missing" in result.index


@pytest.mark.asyncio
async def test_describe_dynamic_batching(
    preview_service,
    feature_store_sample,
    mock_snowflake_session,
    update_fixtures,
):
    """
    Test describe with dynamic batching. Expect to see increasing simplified queries being executed
    as execute_query_long_running for stats computation fails.
    """

    async def execute_query_always_fail_for_stats(query):
        if "CREATE TABLE" in query:
            return
        raise RuntimeError("Failing the query on purpose!")

    mock_snowflake_session.execute_query_long_running.side_effect = (
        execute_query_always_fail_for_stats
    )

    with pytest.raises(DescribeQueryExecutionError):
        await preview_service.describe(
            feature_store_sample,
            size=0,
            seed=0,
        )

    queries = extract_session_executed_queries(mock_snowflake_session)
    fixture_filename = (
        "tests/fixtures/preview_service/expected_describe_dynamic_batching_queries.sql"
    )
    assert_equal_with_expected_fixture(queries, fixture_filename, update_fixtures)


@pytest.mark.asyncio
async def test_value_counts(
    preview_service,
    feature_store_preview,
    mock_snowflake_session,
):
    """
    Test value counts
    """
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame({
        "key": ["1", "2", None],
        "count": [100, 50, 3],
    })

    callback_call_args = []

    async def completion_callback(n):
        callback_call_args.append(n)

    result = await preview_service.value_counts(
        feature_store_preview,
        column_names=["col_int", "col_float", "col_char"],
        num_rows=100000,
        num_categories_limit=500,
        completion_callback=completion_callback,
    )
    assert result == {
        "col_int": {1: 100, 2: 50, None: 3},
        "col_float": {1.0: 100, 2.0: 50, None: 3},
        "col_char": {"1": 100, "2": 50, None: 3},
    }
    assert callback_call_args == [1, 2, 3]


@pytest.mark.parametrize(
    "project_column,keys,expected",
    [
        (
            "col_int",
            [1, 2, None],
            {1: 100, 2: 50, None: 3},
        ),
        (
            "col_float",
            np.float32([1.0, 2.0, np.nan]),
            {1.0: 100, 2.0: 50, None: 3},
        ),
        (
            "col_text",
            ["a", "b", None],
            {"a": 100, "b": 50, None: 3},
        ),
    ],
)
@pytest.mark.asyncio
async def test_value_counts_not_convert_keys_to_string(
    preview_service,
    feature_store_preview_project_column,
    project_column,
    keys,
    expected,
    mock_snowflake_session,
):
    """
    Test value counts
    """
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame({
        "key": keys,
        "count": [100, 50, 3],
    })
    result = await preview_service.value_counts(
        feature_store_preview_project_column,
        num_rows=100000,
        column_names=[project_column],
        num_categories_limit=500,
    )
    assert result == {project_column: expected}


@pytest.mark.asyncio
async def test_query_no_limit_warning_is_raised(
    preview_service,
    feature_store_sample,
    feature_store_preview,
    mock_snowflake_session,
):
    """Test query no limit warning is raised"""

    # mock execute query & check warning
    mock_snowflake_session.execute_query.side_effect = mock_execute_query
    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query
    with pytest.warns(QueryNoLimitWarning):
        await preview_service.describe(feature_store_sample, size=0, seed=0)

    with pytest.warns(QueryNoLimitWarning):
        await preview_service.sample(feature_store_sample, size=0, seed=0)

    with pytest.warns(QueryNoLimitWarning):
        await preview_service.preview(feature_store_preview, limit=0)


@pytest.mark.asyncio
async def test_sample_does_not_change_underlying_graph(
    preview_service,
    feature_store_sample,
    feature_store_preview,
    mock_snowflake_session,
):
    """Test sample does not change underlying graph"""

    payload_dict = feature_store_sample.model_dump(by_alias=True)
    graph = QueryGraph(**payload_dict["graph"])
    payload = FeatureStoreSample(**{**payload_dict, "graph": graph})
    before_sample = graph.model_dump_json(by_alias=True)

    mock_snowflake_session.execute_query.side_effect = mock_execute_query
    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query

    await preview_service.sample(payload, size=0, seed=0, sample_on_primary_table=True)
    after_sample = graph.model_dump_json(by_alias=True)
    assert before_sample == after_sample
