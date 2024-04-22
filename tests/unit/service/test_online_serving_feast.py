"""
Tests for OnlineServingService feast implementation
"""

import pytest
import pytest_asyncio
from bson import ObjectId

import featurebyte as fb
from featurebyte.exception import RequiredEntityNotProvidedError
from tests.util.helper import deploy_feature


@pytest.fixture(name="always_enable_feast_integration", autouse=True)
def always_enable_feast_integration_fixture(enable_feast_integration, catalog_with_online_store):
    """
    Enable feast integration for all tests in this module
    """
    _ = enable_feast_integration
    _ = catalog_with_online_store


@pytest.fixture(name="float_feat_deployment_id")
def float_feat_deployment_id_fixture():
    """
    Fixture for float feature deployment id
    """
    return ObjectId()


@pytest.fixture(name="fl_with_point_in_time_request_column_deployment_id")
def fl_with_point_in_time_request_column_deployment_id_fixture():
    """
    Fixture for float feature deployment id
    """
    return ObjectId()


@pytest_asyncio.fixture
async def deployed_feature_list_with_float_feature(
    app_container,
    float_feature,
    float_feat_deployment_id,
    mock_offline_store_feature_manager_dependencies,
    mock_update_data_warehouse,
):
    """
    Fixture for deployed float feature
    """
    _ = mock_offline_store_feature_manager_dependencies
    _ = mock_update_data_warehouse
    return await deploy_feature(
        app_container,
        float_feature,
        return_type="feature_list",
        deployment_id=float_feat_deployment_id,
    )


@pytest_asyncio.fixture
async def deployed_feature_list_with_point_in_time_request_column_feature(
    app_container,
    float_feature,
    fl_with_point_in_time_request_column_deployment_id,
    mock_offline_store_feature_manager_dependencies,
    mock_update_data_warehouse,
):
    """
    Fixture for deployed point in time request column feature
    """
    _ = mock_offline_store_feature_manager_dependencies
    _ = mock_update_data_warehouse
    new_feature = float_feature * fb.RequestColumn.point_in_time().dt.day
    new_feature.name = "feature_with_point_in_time_request_column"
    return await deploy_feature(
        app_container,
        new_feature,
        return_type="feature_list",
        deployment_id=fl_with_point_in_time_request_column_deployment_id,
    )


async def get_feast_feature_store(app_container, deployment_id):
    """
    Fixture for a feast feature store
    """

    deployment = await app_container.deployment_service.get_document(document_id=deployment_id)
    feast_feature_store_service = app_container.feast_feature_store_service
    feast_feature_store = await feast_feature_store_service.get_feast_feature_store_for_deployment(
        deployment=deployment
    )
    return feast_feature_store


async def get_deployment_from_feature_list(app_container, feature_list_id):
    """
    Helper function to get deployment from feature list
    """
    async for deployment in app_container.deployment_service.list_documents_iterator(
        query_filter={"feature_list_id": feature_list_id}
    ):
        return deployment


@pytest.mark.asyncio
async def test_feature_no_point_in_time(
    app_container,
    online_serving_service,
    deployed_feature_list_with_float_feature,
    float_feat_deployment_id,
):
    """
    Test online serving feast without point in time
    """
    request_data = [{"cust_id": "a"}]
    feast_feature_store = await get_feast_feature_store(app_container, float_feat_deployment_id)
    deployment = await get_deployment_from_feature_list(
        app_container, deployed_feature_list_with_float_feature.id
    )
    result = await online_serving_service.get_online_features_by_feast(
        deployed_feature_list_with_float_feature, deployment, feast_feature_store, request_data
    )
    assert result.dict() == {"features": [{"cust_id": "a", "sum_1d": None}]}


@pytest.mark.asyncio
async def test_feature_with_point_in_time(
    app_container,
    online_serving_service,
    deployed_feature_list_with_point_in_time_request_column_feature,
    fl_with_point_in_time_request_column_deployment_id,
):
    """
    Test online serving feast with point in time
    """
    request_data = [{"cust_id": "a"}]
    feast_feature_store = await get_feast_feature_store(
        app_container, fl_with_point_in_time_request_column_deployment_id
    )
    deployment = await get_deployment_from_feature_list(
        app_container, deployed_feature_list_with_point_in_time_request_column_feature.id
    )
    result = await online_serving_service.get_online_features_by_feast(
        deployed_feature_list_with_point_in_time_request_column_feature,
        deployment,
        feast_feature_store,
        request_data,
    )
    assert result.dict() == {
        "features": [{"cust_id": "a", "feature_with_point_in_time_request_column": None}]
    }


@pytest.mark.asyncio
async def test_validate_required_serving_names(
    app_container,
    online_serving_service,
    deployed_feature_list_with_point_in_time_request_column_feature,
    fl_with_point_in_time_request_column_deployment_id,
):
    """
    Test validation for missing required serving names
    """
    feast_feature_store = await get_feast_feature_store(
        app_container, fl_with_point_in_time_request_column_deployment_id
    )
    request_data = [{"cust_idz": "a"}]
    deployment = await get_deployment_from_feature_list(
        app_container, deployed_feature_list_with_point_in_time_request_column_feature.id
    )
    with pytest.raises(RequiredEntityNotProvidedError) as exc_info:
        await online_serving_service.get_online_features_by_feast(
            deployed_feature_list_with_point_in_time_request_column_feature,
            deployment,
            feast_feature_store,
            request_data,
        )
    assert (
        str(exc_info.value)
        == 'Required entities are not provided in the request: customer (serving name: "cust_id")'
    )


@pytest.mark.asyncio
async def test_feature_requiring_parent_serving(
    app_container,
    online_serving_service,
    deployed_feature_list_requiring_parent_serving,
    fl_requiring_parent_serving_deployment_id,
):
    """
    Test online serving feast with feature requiring parent serving
    """
    feast_feature_store = await get_feast_feature_store(
        app_container, fl_requiring_parent_serving_deployment_id
    )
    request_data = [{"cust_id": "a"}]
    deployment = await get_deployment_from_feature_list(
        app_container, deployed_feature_list_requiring_parent_serving.id
    )
    result = await online_serving_service.get_online_features_by_feast(
        deployed_feature_list_requiring_parent_serving,
        deployment,
        feast_feature_store,
        request_data,
    )
    assert result.dict() == {
        "features": [{"cust_id": "a", "feature_requiring_parent_serving": None}]
    }
