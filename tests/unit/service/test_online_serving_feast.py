"""
Tests for OnlineServingService feast implementation
"""

import pytest
import pytest_asyncio

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


@pytest_asyncio.fixture
async def deployed_feature_list_with_float_feature(
    app_container,
    float_feature,
    mock_offline_store_feature_manager_dependencies,
    mock_update_data_warehouse,
):
    """
    Fixture for deployed float feature
    """
    _ = mock_offline_store_feature_manager_dependencies
    _ = mock_update_data_warehouse
    return await deploy_feature(app_container, float_feature, return_type="feature_list")


@pytest_asyncio.fixture
async def deployed_feature_list_with_point_in_time_request_column_feature(
    app_container,
    float_feature,
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
    return await deploy_feature(app_container, new_feature, return_type="feature_list")


@pytest_asyncio.fixture
async def feast_feature_store(app_container):
    """
    Fixture for a feast feature store
    """
    return await app_container.feast_feature_store_service.get_feast_feature_store_for_catalog()


@pytest.mark.asyncio
async def test_feature_no_point_in_time(
    online_serving_service,
    deployed_feature_list_with_float_feature,
    feast_feature_store,
):
    """
    Test online serving feast without point in time
    """
    request_data = [{"cust_id": "a"}]
    result = await online_serving_service.get_online_features_by_feast(
        deployed_feature_list_with_float_feature, feast_feature_store, request_data
    )
    assert result.dict() == {"features": [{"cust_id": "a", "sum_1d": None}]}


@pytest.mark.asyncio
async def test_feature_with_point_in_time(
    online_serving_service,
    deployed_feature_list_with_point_in_time_request_column_feature,
    feast_feature_store,
):
    """
    Test online serving feast with point in time
    """
    request_data = [{"cust_id": "a"}]
    result = await online_serving_service.get_online_features_by_feast(
        deployed_feature_list_with_point_in_time_request_column_feature,
        feast_feature_store,
        request_data,
    )
    assert result.dict() == {
        "features": [{"cust_id": "a", "feature_with_point_in_time_request_column": None}]
    }


@pytest.mark.asyncio
async def test_validate_required_serving_names(
    online_serving_service,
    deployed_feature_list_with_point_in_time_request_column_feature,
    feast_feature_store,
):
    """
    Test validation for missing required serving names
    """
    request_data = [{"cust_idz": "a"}]
    with pytest.raises(RequiredEntityNotProvidedError) as exc_info:
        await online_serving_service.get_online_features_by_feast(
            deployed_feature_list_with_point_in_time_request_column_feature,
            feast_feature_store,
            request_data,
        )
    assert (
        str(exc_info.value)
        == 'Required entities are not provided in the request: customer (serving name: "cust_id")'
    )


@pytest.mark.asyncio
async def test_feature_requiring_parent_serving(
    online_serving_service,
    deployed_feature_list_requiring_parent_serving,
    feast_feature_store,
):
    """
    Test online serving feast with feature requiring parent serving
    """
    request_data = [{"cust_id": "a"}]
    result = await online_serving_service.get_online_features_by_feast(
        deployed_feature_list_requiring_parent_serving,
        feast_feature_store,
        request_data,
    )
    assert result.dict() == {
        "features": [{"cust_id": "a", "feature_requiring_parent_serving": None}]
    }
