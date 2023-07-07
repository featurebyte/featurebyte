"""
Test preview service module
"""
from unittest.mock import AsyncMock, patch

import pytest
from bson import ObjectId

from featurebyte import FeatureStore
from featurebyte.exception import MissingPointInTimeColumnError, RequiredEntityNotProvidedError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_list import FeatureCluster
from featurebyte.schema.feature_list import FeatureListPreview
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


@pytest.mark.asyncio
async def test_preview_feature__time_based_feature_without_point_in_time_errors(
    preview_service, float_feature
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
        await preview_service.preview_target_or_feature(feature_preview, AsyncMock())
    assert "Point in time column not provided" in str(exc)


@pytest.mark.asyncio
async def test_preview_feature__non_time_based_feature_without_point_in_time_doesnt_error(
    preview_service, transaction_entity, non_time_based_feature, get_credential
):
    """
    Test preview feature
    """
    _ = transaction_entity
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
    await preview_service.preview_target_or_feature(feature_preview, get_credential)


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_preview_feature__missing_entity(
    preview_service, production_ready_feature, get_credential
):
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
        await preview_service.preview_target_or_feature(feature_preview, get_credential)
    expected = (
        'Required entities are not provided in the request: customer (serving name: "cust_id")'
    )
    assert str(exc.value) == expected


@pytest.mark.asyncio
async def test_preview_featurelist__time_based_feature_errors_without_time(
    preview_service, float_feature, get_credential
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
        await preview_service.preview_featurelist(feature_list_preview, get_credential)
    assert "Point in time column not provided" in str(exc)


@pytest.mark.asyncio
async def test_preview_featurelist__non_time_based_feature_no_error_without_time(
    preview_service, transaction_entity, non_time_based_feature, get_credential
):
    """
    Test preview featurelist
    """
    _ = transaction_entity
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
    await preview_service.preview_featurelist(feature_list_preview, get_credential)


@pytest.mark.asyncio
async def test_preview_featurelist__missing_entity(
    preview_service, production_ready_feature_list, get_credential
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
        await preview_service.preview_featurelist(feature_list_preview, get_credential)
    expected = (
        'Required entities are not provided in the request: customer (serving name: "cust_id")'
    )
    assert str(exc.value) == expected
