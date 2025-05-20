"""
Unit tests for DeployedTileTableManagerService
"""

import pytest
import pytest_asyncio

from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from tests.util.helper import deploy_feature


@pytest.fixture(name="deployed_tile_table_service")
def deployed_tile_table_service_fixture(app_container):
    """
    Fixture for DeployedTileTableService
    """
    return app_container.deployed_tile_table_service


@pytest.fixture(name="deployed_tile_table_manager_service")
def deployed_tile_table_manager_service_fixture(app_container, deployed_tile_table_service):
    """
    Fixture for DeployedTileTableManagerService
    """
    return app_container.deployed_tile_table_manager_service


@pytest_asyncio.fixture(name="multiple_deployed_features")
async def multiple_features_fixture(
    snowflake_event_view_with_entity,
    app_container,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for multiple deployed features
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies

    features = []

    # A group of features whose tile queries can be combined
    for method in ["sum", "min", "max"]:
        feature_job_setting = FeatureJobSetting(blind_spot="10m", period="30m", offset="5m")
        feature = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
            value_column="col_float",
            method=method,
            windows=["1d"],
            feature_job_setting=feature_job_setting,
            feature_names=[f"{method}_1d_10m"],
        )[f"{method}_1d_10m"]
        features.append(feature)

    # Another group of features that cannot be combined with the above features
    for method in ["avg", "std"]:
        feature_job_setting = FeatureJobSetting(blind_spot="5m", period="30m", offset="5m")
        feature = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
            value_column="col_float",
            method=method,
            windows=["1d"],
            feature_job_setting=feature_job_setting,
            feature_names=[f"{method}_1d_5m"],
        )[f"{method}_1d_5m"]
        features.append(feature)

    # Deploy the features
    deployed_features = []
    for feature in features:
        deployed_feature = await deploy_feature(app_container, feature)
        deployed_features.append(deployed_feature)
    return deployed_features


async def get_deployed_tile_table_models(deployed_tile_table_service):
    """
    Get deployed tile table models
    """
    deployed_tile_table_models = []
    async for deployed_tile_table_model in deployed_tile_table_service.list_documents_iterator(
        query_filter={}
    ):
        deployed_tile_table_models.append(deployed_tile_table_model)
    return sorted(deployed_tile_table_models, key=lambda x: x.id)


@pytest.mark.asyncio
async def test_handle_online_enabled_features(
    deployed_tile_table_service,
    deployed_tile_table_manager_service,
    multiple_deployed_features,
    source_info,
):
    """
    Test handle_online_enabled_features
    """
    await deployed_tile_table_manager_service.handle_online_enabled_features(
        multiple_deployed_features, source_info
    )
    deployed_tile_table_models = await get_deployed_tile_table_models(deployed_tile_table_service)
    assert len(deployed_tile_table_models) == 2
