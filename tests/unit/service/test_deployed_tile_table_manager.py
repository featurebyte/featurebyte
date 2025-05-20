"""
Unit tests for DeployedTileTableManagerService
"""

import pytest
import pytest_asyncio

from featurebyte import SourceType
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.sql.common import sql_to_string
from tests.util.helper import (
    assert_equal_with_expected_fixture,
    deploy_feature,
    undeploy_feature,
)


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


@pytest.fixture(name="feature_set_1")
def feature_set_1_fixture(snowflake_event_view_with_entity):
    """
    A group of features whose tile queries can be combined
    """
    features = []
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
    return features


@pytest.fixture(name="feature_set_2")
def feature_set_2_fixture(snowflake_event_view_with_entity):
    """
    Another group of features that cannot be combined with the above features
    """
    features = []
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
    return features


@pytest.fixture(name="feature_set_3")
def feature_set_3_fixture(feature_set_1):
    """
    A group of features which share the same tiles as feature_set_1
    """
    features = []
    for feature in feature_set_1:
        new_feature = feature + 123
        new_feature.name = f"{feature.name}_plus_123"
        features.append(new_feature)
    return features


@pytest_asyncio.fixture(name="multiple_deployed_features")
async def multiple_features_fixture(
    feature_set_1,
    feature_set_2,
    app_container,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for multiple deployed features
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies

    features = feature_set_1 + feature_set_2

    # Deploy the features
    deployed_features = {}
    for feature in features:
        deployed_feature = await deploy_feature(app_container, feature)
        deployed_features[feature.name] = deployed_feature
    return deployed_features


@pytest_asyncio.fixture(name="deployed_feature_set_1_then_feature_set_3")
async def deployed_feature_set_1_then_feature_set_3_fixture(
    feature_set_1,
    feature_set_3,
    app_container,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
    deployed_tile_table_manager_service,
    source_info,
):
    """
    Fixture for deployed feature set 1 then feature set 3
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies

    # Deploy the features in feature_set_1
    deployed_features_1 = {}
    for feature in feature_set_1:
        deployed_feature = await deploy_feature(app_container, feature)
        deployed_features_1[feature.name] = deployed_feature
    await deployed_tile_table_manager_service.handle_online_enabled_features(
        list(deployed_features_1.values()),
        source_info=source_info,
    )

    # Deploy the features in feature_set_3
    deployed_features_3 = {}
    for feature in feature_set_3:
        deployed_feature = await deploy_feature(app_container, feature)
        deployed_features_3[feature.name] = deployed_feature
    await deployed_tile_table_manager_service.handle_online_enabled_features(
        list(deployed_features_3.values()),
        source_info=source_info,
    )

    deployed_features = {}
    deployed_features.update(deployed_features_1)
    deployed_features.update(deployed_features_3)
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


def check_deployed_tile_table_model(
    deployed_tile_table_model,
    expected_deployed_tile_table_dict,
    tile_sql_fixture_filename,
    update_fixtures,
):
    """
    Check deployed tile table model
    """
    tile_sql = sql_to_string(
        deployed_tile_table_model.tile_compute_query.get_combined_query_expr(), SourceType.SNOWFLAKE
    )
    deployed_tile_table_dict = deployed_tile_table_model.model_dump(
        by_alias=True,
        exclude={
            "created_at",
            "updated_at",
            "id",
            "user_id",
            "catalog_id",
            "tile_compute_query",
        },
    )
    assert_equal_with_expected_fixture(tile_sql, tile_sql_fixture_filename, update_fixtures)
    assert deployed_tile_table_dict == expected_deployed_tile_table_dict


@pytest.mark.asyncio
async def test_handle_online_enabled_features(
    deployed_tile_table_service,
    deployed_tile_table_manager_service,
    multiple_deployed_features,
    source_info,
    update_fixtures,
):
    """
    Test handle_online_enabled_features
    """
    await deployed_tile_table_manager_service.handle_online_enabled_features(
        list(multiple_deployed_features.values()), source_info
    )
    deployed_tile_table_models = await get_deployed_tile_table_models(deployed_tile_table_service)
    assert len(deployed_tile_table_models) == 2

    check_deployed_tile_table_model(
        deployed_tile_table_models[0],
        {
            "backfill_metadata": None,
            "blind_spot_second": 600,
            "block_modification_by": [],
            "description": None,
            "entity_column_names": ["cust_id"],
            "frequency_minute": 30,
            "is_deleted": False,
            "last_run_metadata_offline": None,
            "last_run_metadata_online": None,
            "name": None,
            "table_name": "__FB_DEPLOYED_TILE_TABLE_000000000000000000000000",
            "tile_identifiers": [
                {
                    "aggregation_id": "sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
                    "tile_id": "TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295",
                },
                {
                    "aggregation_id": "min_9f45e3acb4c92c3e7965894a8f4ae4fbd7c01dda",
                    "tile_id": "TILE_MIN_9F45E3ACB4C92C3E7965894A8F4AE4FBD7C01DDA",
                },
                {
                    "aggregation_id": "max_8386bc5866b03e1c3bc2de69717e050b965edd31",
                    "tile_id": "TILE_MAX_8386BC5866B03E1C3BC2DE69717E050B965EDD31",
                },
            ],
            "time_modulo_frequency_second": 300,
            "value_column_names": [
                "value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
                "value_min_9f45e3acb4c92c3e7965894a8f4ae4fbd7c01dda",
                "value_max_8386bc5866b03e1c3bc2de69717e050b965edd31",
            ],
            "value_column_types": ["FLOAT", "FLOAT", "FLOAT"],
        },
        "tests/fixtures/deployed_tile_table/expected_tile_sql_1.sql",
        update_fixtures,
    )
    check_deployed_tile_table_model(
        deployed_tile_table_models[1],
        {
            "backfill_metadata": None,
            "blind_spot_second": 300,
            "block_modification_by": [],
            "description": None,
            "entity_column_names": ["cust_id"],
            "frequency_minute": 30,
            "is_deleted": False,
            "last_run_metadata_offline": None,
            "last_run_metadata_online": None,
            "name": None,
            "table_name": "__FB_DEPLOYED_TILE_TABLE_000000000000000000000001",
            "tile_identifiers": [
                {
                    "aggregation_id": "avg_ce0c9886ef9c14b43e37879200b1410d9d97e460",
                    "tile_id": "TILE_AVG_CE0C9886EF9C14B43E37879200B1410D9D97E460",
                },
                {
                    "aggregation_id": "std_06e1272c0c3f2d9fe71c0a171e70e6360bf00c65",
                    "tile_id": "TILE_STD_06E1272C0C3F2D9FE71C0A171E70E6360BF00C65",
                },
            ],
            "time_modulo_frequency_second": 300,
            "value_column_names": [
                "sum_value_avg_ce0c9886ef9c14b43e37879200b1410d9d97e460",
                "count_value_avg_ce0c9886ef9c14b43e37879200b1410d9d97e460",
                "sum_value_squared_std_06e1272c0c3f2d9fe71c0a171e70e6360bf00c65",
                "sum_value_std_06e1272c0c3f2d9fe71c0a171e70e6360bf00c65",
                "count_value_std_06e1272c0c3f2d9fe71c0a171e70e6360bf00c65",
            ],
            "value_column_types": ["FLOAT", "FLOAT", "FLOAT", "FLOAT", "FLOAT"],
        },
        "tests/fixtures/deployed_tile_table/expected_tile_sql_2.sql",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_handle_online_disabled_features(
    deployed_tile_table_service,
    deployed_tile_table_manager_service,
    multiple_deployed_features,
    source_info,
):
    """
    Test handle_online_disabled_features
    """
    await deployed_tile_table_manager_service.handle_online_enabled_features(
        list(multiple_deployed_features.values()), source_info
    )
    deployed_tile_table_models = await get_deployed_tile_table_models(deployed_tile_table_service)
    assert len(deployed_tile_table_models) == 2

    # Undeploy the first feature. The deployed tile table should be retained since it's still used
    # by other features.
    undeploy_feature(multiple_deployed_features["sum_1d_10m"])
    await deployed_tile_table_manager_service.handle_online_disabled_features()
    deployed_tile_table_models = await get_deployed_tile_table_models(deployed_tile_table_service)
    assert len(deployed_tile_table_models) == 2

    # Undeploy more features. The first deployed tile table should be removed since it's no longer
    # used by any features.
    undeploy_feature(multiple_deployed_features["min_1d_10m"])
    undeploy_feature(multiple_deployed_features["max_1d_10m"])
    await deployed_tile_table_manager_service.handle_online_disabled_features()
    deployed_tile_table_models = await get_deployed_tile_table_models(deployed_tile_table_service)
    assert len(deployed_tile_table_models) == 1
    assert (
        deployed_tile_table_models[0].table_name
        == "__FB_DEPLOYED_TILE_TABLE_000000000000000000000001"
    )

    # Undeploy remaining features
    undeploy_feature(multiple_deployed_features["avg_1d_5m"])
    undeploy_feature(multiple_deployed_features["std_1d_5m"])
    await deployed_tile_table_manager_service.handle_online_disabled_features()
    deployed_tile_table_models = await get_deployed_tile_table_models(deployed_tile_table_service)
    assert len(deployed_tile_table_models) == 0


@pytest.mark.asyncio
async def test_deployed_feature_set_1_then_feature_set_3(
    deployed_tile_table_service,
    deployed_feature_set_1_then_feature_set_3,
    update_fixtures,
):
    """
    Test deployed feature set 1 then feature set 3
    """
    _ = deployed_feature_set_1_then_feature_set_3

    # There should be only one deployed tile table since the features in feature_set_3 share the
    # same tiles as the features in feature_set_1.
    deployed_tile_table_models = await get_deployed_tile_table_models(deployed_tile_table_service)
    assert len(deployed_tile_table_models) == 1

    check_deployed_tile_table_model(
        deployed_tile_table_models[0],
        {
            "backfill_metadata": None,
            "blind_spot_second": 600,
            "block_modification_by": [],
            "description": None,
            "entity_column_names": ["cust_id"],
            "frequency_minute": 30,
            "is_deleted": False,
            "last_run_metadata_offline": None,
            "last_run_metadata_online": None,
            "name": None,
            "table_name": "__FB_DEPLOYED_TILE_TABLE_000000000000000000000000",
            "tile_identifiers": [
                {
                    "aggregation_id": "sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
                    "tile_id": "TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295",
                },
                {
                    "aggregation_id": "min_9f45e3acb4c92c3e7965894a8f4ae4fbd7c01dda",
                    "tile_id": "TILE_MIN_9F45E3ACB4C92C3E7965894A8F4AE4FBD7C01DDA",
                },
                {
                    "aggregation_id": "max_8386bc5866b03e1c3bc2de69717e050b965edd31",
                    "tile_id": "TILE_MAX_8386BC5866B03E1C3BC2DE69717E050B965EDD31",
                },
            ],
            "time_modulo_frequency_second": 300,
            "value_column_names": [
                "value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
                "value_min_9f45e3acb4c92c3e7965894a8f4ae4fbd7c01dda",
                "value_max_8386bc5866b03e1c3bc2de69717e050b965edd31",
            ],
            "value_column_types": ["FLOAT", "FLOAT", "FLOAT"],
        },
        "tests/fixtures/deployed_tile_table/expected_tile_sql_1.sql",
        update_fixtures,
    )
