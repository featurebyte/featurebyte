"""
Tests for OfflineStoreFeatureTableManagerService
"""
from typing import Dict

from unittest.mock import patch

import pytest
import pytest_asyncio
from bson import ObjectId

import featurebyte as fb
from featurebyte.models.feature import FeatureModel
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from tests.util.helper import assert_equal_json_fixture


async def deploy_feature(app_container, feature) -> FeatureModel:
    """
    Helper function to create deploy a single feature
    """
    feature_list = fb.FeatureList([feature], name=f"{feature.name}_list")
    feature_list.save()
    deployment = feature_list.deploy(
        deployment_name=feature_list.name, make_production_ready=True, ignore_guardrails=True
    )
    deployment.enable()
    return await app_container.feature_service.get_document(feature.id)


async def undeploy_feature(feature):
    """
    Helper function to undeploy a single feature
    """
    deployment: fb.Deployment = fb.Deployment.get(f"{feature.name}_list")
    deployment.disable()  # pylint: disable=no-member


@pytest_asyncio.fixture
async def deployed_float_feature(app_container, float_feature):
    """
    Fixture for deployed float feature
    """
    return await deploy_feature(app_container, float_feature)


@pytest_asyncio.fixture
async def deployed_float_feature_post_processed(app_container, float_feature) -> FeatureModel:
    """
    Fixture for deployed feature that is post processed from float feature
    """
    feature = float_feature + 123
    feature.name = f"{float_feature.name}_plus_123"
    return await deploy_feature(app_container, feature)


@pytest_asyncio.fixture
async def deployed_float_feature_different_job_setting(
    app_container, float_feature_different_job_setting
):
    """
    Fixture for deployed float feature with different job setting
    """
    return await deploy_feature(app_container, float_feature_different_job_setting)


@pytest.fixture
def document_service(app_container):
    """
    Fixture for OfflineStoreFeatureTableService
    """
    return app_container.offline_store_feature_table_service


@pytest.fixture
def manager_service(app_container):
    """
    Fixture for OfflineStoreFeatureTableManagerService
    """
    return app_container.offline_store_feature_table_manager_service


async def get_all_feature_tables(document_service) -> Dict[str, OfflineStoreFeatureTableModel]:
    """
    Helper function to get all feature tables keyed by feature table name
    """
    feature_tables = {}
    async for feature_table in document_service.list_documents_iterator(query_filter={}):
        feature_tables[feature_table.name] = feature_table
    return feature_tables


@pytest.fixture(name="mock_initialize_new_columns")
def mock_initialize_new_columns_fixture():
    """
    Fixture to mock FeatureMaterializeService.initialize_new_columns
    """
    with patch(
        "featurebyte.service.offline_store_feature_table_manager.FeatureMaterializeService.initialize_new_columns"
    ) as patched:
        yield patched


async def has_scheduled_task(periodic_task_service, feature_table):
    """
    Helper function to check if there is a scheduled task
    """
    async for periodic_task in periodic_task_service.list_documents_iterator(
        query_filter={"kwargs.command": "SCHEDULED_FEATURE_MATERIALIZE"}
    ):
        if periodic_task.kwargs["offline_store_feature_table_id"] == str(feature_table.id):
            return True
    return False


async def check_feast_registry(app_container, expected_feature_views, expected_feature_services):
    """
    Helper function to check feast registry
    """
    feast_registry = await app_container.feast_registry_service.get_feast_registry_for_catalog()
    assert feast_registry is not None
    feature_store = await app_container.feast_feature_store_service.get_feast_feature_store(
        feast_registry.id
    )
    assert {fv.name for fv in feature_store.list_feature_views()} == expected_feature_views
    assert {fs.name for fs in feature_store.list_feature_services()} == expected_feature_services


@pytest.mark.asyncio
async def test_feature_table_one_feature_deployed(
    app_container,
    document_service,
    manager_service,
    periodic_task_service,
    deployed_float_feature,
    mock_initialize_new_columns,
    update_fixtures,
):
    """
    Test feature table creation when one feature is deployed
    """
    await manager_service.handle_online_enabled_feature(deployed_float_feature)

    assert mock_initialize_new_columns.call_count == 1

    feature_tables = await get_all_feature_tables(document_service)
    assert len(feature_tables) == 1
    feature_table = feature_tables["fb_entity_cust_id_fjs_1800_300_600_ttl"]

    feature_table_dict = feature_table.dict(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    feature_cluster = feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "entity_universe": {
            "aggregate_result_table_names": [
                "online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c"
            ],
            "serving_names": ["cust_id"],
            "type": "window_aggregate",
        },
        "feature_ids": [deployed_float_feature.id],
        "feature_job_setting": {
            "blind_spot": "600s",
            "frequency": "1800s",
            "time_modulo_frequency": "300s",
        },
        "has_ttl": True,
        "last_materialized_at": None,
        "name": "fb_entity_cust_id_fjs_1800_300_600_ttl",
        "output_column_names": ["sum_1d"],
        "output_dtypes": ["FLOAT"],
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
    }
    assert_equal_json_fixture(
        feature_cluster,
        "tests/fixtures/offline_store_feature_table/feature_cluster_one_feature.json",
        update_fixtures,
    )

    assert await has_scheduled_task(periodic_task_service, feature_table)

    await check_feast_registry(
        app_container,
        expected_feature_views={"fb_entity_cust_id_fjs_1800_300_600_ttl"},
        expected_feature_services={"sum_1d_list"},
    )


@pytest.mark.asyncio
async def test_feature_table_two_features_deployed(
    app_container,
    document_service,
    manager_service,
    periodic_task_service,
    deployed_float_feature,
    deployed_float_feature_post_processed,
    mock_initialize_new_columns,
    update_fixtures,
):
    """
    Test feature table creation and update when two features are deployed
    """
    await manager_service.handle_online_enabled_feature(deployed_float_feature)
    await manager_service.handle_online_enabled_feature(deployed_float_feature_post_processed)

    assert mock_initialize_new_columns.call_count == 2

    feature_tables = await get_all_feature_tables(document_service)
    assert len(feature_tables) == 1
    feature_table = feature_tables["fb_entity_cust_id_fjs_1800_300_600_ttl"]

    feature_table_dict = feature_table.dict(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    feature_cluster = feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "entity_universe": {
            "aggregate_result_table_names": [
                "online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c"
            ],
            "serving_names": ["cust_id"],
            "type": "window_aggregate",
        },
        "feature_ids": [deployed_float_feature.id, deployed_float_feature_post_processed.id],
        "feature_job_setting": {
            "blind_spot": "600s",
            "frequency": "1800s",
            "time_modulo_frequency": "300s",
        },
        "has_ttl": True,
        "last_materialized_at": None,
        "name": "fb_entity_cust_id_fjs_1800_300_600_ttl",
        "output_column_names": ["sum_1d", "sum_1d_plus_123"],
        "output_dtypes": ["FLOAT", "FLOAT"],
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
    }
    assert_equal_json_fixture(
        feature_cluster,
        "tests/fixtures/offline_store_feature_table/feature_cluster_two_features.json",
        update_fixtures,
    )

    assert await has_scheduled_task(periodic_task_service, feature_table)

    await check_feast_registry(
        app_container,
        expected_feature_views={"fb_entity_cust_id_fjs_1800_300_600_ttl"},
        expected_feature_services={"sum_1d_list", "sum_1d_plus_123_list"},
    )


@pytest.mark.asyncio
async def test_feature_table_undeploy(
    app_container,
    document_service,
    manager_service,
    periodic_task_service,
    deployed_float_feature,
    deployed_float_feature_post_processed,
    mock_initialize_new_columns,
    update_fixtures,
):
    """
    Test feature table creation and update when two features are deployed
    """
    # Simulate online enabling two features then online disable one
    await manager_service.handle_online_enabled_feature(deployed_float_feature)
    await manager_service.handle_online_enabled_feature(deployed_float_feature_post_processed)

    await undeploy_feature(deployed_float_feature)
    await manager_service.handle_online_disabled_feature(deployed_float_feature)

    assert mock_initialize_new_columns.call_count == 2

    feature_tables = await get_all_feature_tables(document_service)
    assert len(feature_tables) == 1
    feature_table = feature_tables["fb_entity_cust_id_fjs_1800_300_600_ttl"]

    feature_table_dict = feature_table.dict(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    feature_cluster = feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "entity_universe": {
            "aggregate_result_table_names": [
                "online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c"
            ],
            "serving_names": ["cust_id"],
            "type": "window_aggregate",
        },
        "feature_ids": [deployed_float_feature_post_processed.id],
        "feature_job_setting": {
            "blind_spot": "600s",
            "frequency": "1800s",
            "time_modulo_frequency": "300s",
        },
        "has_ttl": True,
        "last_materialized_at": None,
        "name": "fb_entity_cust_id_fjs_1800_300_600_ttl",
        "output_column_names": ["sum_1d_plus_123"],
        "output_dtypes": ["FLOAT"],
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
    }
    assert_equal_json_fixture(
        feature_cluster,
        "tests/fixtures/offline_store_feature_table/feature_cluster_disabled_one_feature.json",
        update_fixtures,
    )

    # Check online disabling the last feature deletes the feature table
    await undeploy_feature(deployed_float_feature_post_processed)
    await manager_service.handle_online_disabled_feature(deployed_float_feature_post_processed)
    feature_tables = await get_all_feature_tables(document_service)
    assert len(feature_tables) == 0
    assert not await has_scheduled_task(periodic_task_service, feature_table)

    await check_feast_registry(
        app_container,
        expected_feature_views=set(),
        expected_feature_services=set(),
    )


@pytest.mark.asyncio
async def test_feature_table_two_features_different_feature_job_settings_deployed(
    app_container,
    document_service,
    manager_service,
    periodic_task_service,
    deployed_float_feature,
    deployed_float_feature_different_job_setting,
    mock_initialize_new_columns,
):
    """
    Test feature table creation and update when two features are deployed
    """
    await manager_service.handle_online_enabled_feature(deployed_float_feature)
    await manager_service.handle_online_enabled_feature(
        deployed_float_feature_different_job_setting
    )

    assert mock_initialize_new_columns.call_count == 2

    feature_tables = await get_all_feature_tables(document_service)
    assert len(feature_tables) == 2

    # Check customer entity feature table
    feature_table = feature_tables["fb_entity_cust_id_fjs_1800_300_600_ttl"]
    feature_table_dict = feature_table.dict(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    _ = feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "entity_universe": {
            "aggregate_result_table_names": [
                "online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c"
            ],
            "serving_names": ["cust_id"],
            "type": "window_aggregate",
        },
        "feature_ids": [deployed_float_feature.id],
        "feature_job_setting": {
            "blind_spot": "600s",
            "frequency": "1800s",
            "time_modulo_frequency": "300s",
        },
        "has_ttl": True,
        "last_materialized_at": None,
        "name": "fb_entity_cust_id_fjs_1800_300_600_ttl",
        "output_column_names": ["sum_1d"],
        "output_dtypes": ["FLOAT"],
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
    }
    assert await has_scheduled_task(periodic_task_service, feature_table)

    # Check item entity feature table
    feature_table = feature_tables["fb_entity_cust_id_fjs_10800_5_900_ttl"]
    feature_table_dict = feature_table.dict(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    _ = feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "entity_universe": {
            "aggregate_result_table_names": [
                "online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c"
            ],
            "serving_names": ["cust_id"],
            "type": "window_aggregate",
        },
        "feature_ids": [deployed_float_feature_different_job_setting.id],
        "feature_job_setting": {
            "blind_spot": "900s",
            "frequency": "10800s",
            "time_modulo_frequency": "5s",
        },
        "has_ttl": True,
        "last_materialized_at": None,
        "name": "fb_entity_cust_id_fjs_10800_5_900_ttl",
        "output_column_names": ["sum_24h_every_3h"],
        "output_dtypes": ["FLOAT"],
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
    }
    assert await has_scheduled_task(periodic_task_service, feature_table)

    await check_feast_registry(
        app_container,
        expected_feature_views={
            "fb_entity_cust_id_fjs_10800_5_900_ttl",
            "fb_entity_cust_id_fjs_1800_300_600_ttl",
        },
        expected_feature_services={"sum_24h_every_3h_list", "sum_1d_list"},
    )