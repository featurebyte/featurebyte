"""
Test preview service module
"""
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId

from featurebyte import FeatureStore
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_list import FeatureCluster
from featurebyte.schema.feature import FeaturePreview
from featurebyte.schema.feature_list import FeatureListPreview


@pytest.fixture(name="empty_graph")
def empty_graph_fixture():
    """Fake graph"""
    return {
        "nodes": [],
        "edges": [],
    }


@pytest.mark.asyncio
async def test_preview_feature__time_based_feature_without_point_in_time_errors(
    preview_service, float_feature
):
    """
    Test preview feature
    """
    feature_preview = FeaturePreview(
        feature_store_name="feature_store_name",
        point_in_time_and_serving_name={},
        graph=float_feature.graph,
        node_name=float_feature.node_name,
    )
    with pytest.raises(KeyError) as exc:
        await preview_service.preview_feature(feature_preview, AsyncMock())
    assert "Point in time column not provided" in str(exc)


@pytest.mark.asyncio
async def test_preview_feature__non_time_based_feature_without_point_in_time_doesnt_error(
    preview_service, cust_id_entity, non_time_based_feature, get_credential
):
    """
    Test preview feature
    """
    _ = cust_id_entity
    feature_preview = FeaturePreview(
        feature_store_name="sf_featurestore",
        point_in_time_and_serving_name={
            "event_id_col": 1,
        },
        graph=non_time_based_feature.graph,
        node_name=non_time_based_feature.node_name,
    )
    await preview_service.preview_feature(feature_preview, get_credential)


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
        point_in_time_and_serving_name={
            "event_id_col": 1,
        },
    )
    with pytest.raises(KeyError) as exc:
        await preview_service.preview_featurelist(feature_list_preview, get_credential)
    assert "Point in time column not provided" in str(exc)


@pytest.mark.asyncio
async def test_preview_featurelist__non_time_based_feature_no_error_without_time(
    preview_service, cust_id_entity, non_time_based_feature, get_credential
):
    """
    Test preview featurelist
    """
    _ = cust_id_entity
    store = FeatureStore.get("sf_featurestore")
    feature_list_preview = FeatureListPreview(
        feature_clusters=[
            FeatureCluster(
                feature_store_id=store.id,
                graph=non_time_based_feature.graph,
                node_names=[non_time_based_feature.node_name],
            ),
        ],
        point_in_time_and_serving_name={
            "event_id_col": 1,
        },
    )
    await preview_service.preview_featurelist(feature_list_preview, get_credential)
