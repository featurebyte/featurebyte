"""
Test preview service module
"""
from unittest.mock import patch

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte import FeatureStore
from featurebyte.exception import MissingPointInTimeColumnError, RequiredEntityNotProvidedError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_list import FeatureCluster
from featurebyte.schema.feature_list import FeatureListPreview
from featurebyte.schema.feature_store import FeatureStorePreview
from featurebyte.schema.preview import FeatureOrTargetPreview


@pytest.fixture(name="empty_graph")
def empty_graph_fixture():
    """Fake graph"""
    return {
        "nodes": [],
        "edges": [],
    }


@pytest.fixture(name="mock_get_feature_store_session")
def mock_get_feature_store_session_fixture():
    """Mock get_feature_store_session method"""
    with patch(
        "featurebyte.service.online_enable.SessionManagerService.get_feature_store_session"
    ) as mock_get_feature_store_session:
        yield mock_get_feature_store_session


@pytest.fixture(name="feature_store_preview")
def feature_store_preview_fixture(feature_store):
    """
    Fixture for a FeatureStorePreview
    """
    return FeatureStorePreview(
        feature_store_name=feature_store.name,
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


@pytest.mark.asyncio
async def test_value_counts(
    preview_service,
    feature_store_preview,
    mock_snowflake_session,
):
    """
    Test value counts
    """
    mock_snowflake_session.execute_query.return_value = pd.DataFrame(
        {
            "key": ["a", "b"],
            "count": [100, 50],
        }
    )
    result = await preview_service.value_counts(
        feature_store_preview,
        num_rows=100000,
        num_categories_limit=500,
    )
    assert result == {"a": 100, "b": 50}
