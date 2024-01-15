"""
Tests for OfflineStoreFeatureTableManagerService
"""
from typing import Dict

import os
from unittest.mock import patch

import pytest
import pytest_asyncio
from bson import ObjectId, json_util

from featurebyte.models.feature import FeatureModel
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.service.offline_store_feature_table_manager import OfflineIngestGraphContainer
from tests.util.helper import (
    assert_equal_json_fixture,
    assert_equal_with_expected_fixture,
    deploy_feature,
    undeploy_feature,
)


@pytest.fixture(autouse=True)
def mock_service_get_version():
    """
    Mock get version
    """
    with patch("featurebyte.service.base_feature_service.get_version", return_value="V231227"):
        yield


@pytest.fixture(name="always_enable_feast_integration", autouse=True)
def always_enable_feast_integration_fixture(enable_feast_integration):
    """
    Enable feast integration for all tests in this module
    """
    _ = enable_feast_integration


@pytest_asyncio.fixture
async def deployed_float_feature(
    app_container, float_feature, mock_update_data_warehouse, mock_feature_materialize_service
):
    """
    Fixture for deployed float feature
    """
    _ = mock_update_data_warehouse
    out = await deploy_feature(app_container, float_feature)
    assert mock_feature_materialize_service["initialize_new_columns"].call_count == 1
    return out


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


@pytest_asyncio.fixture
async def deployed_feature_without_entity(
    app_container,
    feature_without_entity,
    mock_update_data_warehouse,
    mock_feature_materialize_service,
):
    """
    Fixture for deployed feature without entity
    """
    _ = mock_update_data_warehouse
    _ = mock_feature_materialize_service
    return await deploy_feature(app_container, feature_without_entity)


@pytest_asyncio.fixture
async def deployed_scd_lookup_feature(
    app_container,
    scd_lookup_feature,
    mock_update_data_warehouse,
    mock_feature_materialize_service,
):
    """
    Fixture for deployed scd lookup feature
    """
    _ = mock_update_data_warehouse
    _ = mock_feature_materialize_service
    return await deploy_feature(app_container, scd_lookup_feature)


@pytest_asyncio.fixture
async def deployed_aggregate_asat_feature(
    app_container,
    aggregate_asat_feature,
    mock_update_data_warehouse,
    mock_feature_materialize_service,
):
    """
    Fixture for deployed aggregate asat feature
    """
    _ = mock_update_data_warehouse
    _ = mock_feature_materialize_service
    return await deploy_feature(app_container, aggregate_asat_feature)


@pytest_asyncio.fixture
async def deployed_item_aggregate_feature(
    app_container,
    non_time_based_feature,
    mock_update_data_warehouse,
    mock_feature_materialize_service,
):
    """
    Fixture for deployed item aggregate feature
    """
    _ = mock_update_data_warehouse
    _ = mock_feature_materialize_service
    return await deploy_feature(app_container, non_time_based_feature)


@pytest_asyncio.fixture
async def deployed_complex_feature(
    app_container,
    non_time_based_feature,
    feature_without_entity,
    mock_update_data_warehouse,
    mock_feature_materialize_service,
):
    """
    Fixture for a complex feature with multiple parts in the same feature table
    """
    _ = mock_update_data_warehouse
    _ = mock_feature_materialize_service
    f1 = non_time_based_feature
    f2 = feature_without_entity
    new_feature = f1 + f2 + f1
    new_feature.name = "Complex Feature"
    return await deploy_feature(app_container, new_feature)


@pytest_asyncio.fixture
async def deployed_feature_list_when_all_features_already_deployed(
    app_container,
    float_feature,
    deployed_float_feature,
):
    """
    Fixture for a deployment created when all the underlying features are already deployed
    """
    _ = deployed_float_feature
    out = await deploy_feature(
        app_container,
        float_feature,
        feature_list_name_override="my_new_feature_list",
        return_type="feature_list",
    )
    return out


@pytest.fixture
def document_service(app_container):
    """
    Fixture for OfflineStoreFeatureTableService
    """
    return app_container.offline_store_feature_table_service


async def get_all_feature_tables(document_service) -> Dict[str, OfflineStoreFeatureTableModel]:
    """
    Helper function to get all feature tables keyed by feature table name
    """
    feature_tables = {}
    async for feature_table in document_service.list_documents_iterator(query_filter={}):
        feature_tables[feature_table.name] = feature_table
    return feature_tables


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
    assert feature_store.project == str(app_container.catalog_id)
    assert {fv.name for fv in feature_store.list_feature_views()} == expected_feature_views
    assert {fs.name for fs in feature_store.list_feature_services()} == expected_feature_services


@pytest.mark.asyncio
async def test_feature_table_one_feature_deployed(
    app_container,
    document_service,
    periodic_task_service,
    deployed_float_feature,
    update_fixtures,
):
    """
    Test feature table creation when one feature is deployed
    """
    feature_tables = await get_all_feature_tables(document_service)
    assert len(feature_tables) == 1
    feature_table = feature_tables[f"fb_230525_271fb0_cust_id_30m_5m_10m_ttl"]

    feature_table_dict = feature_table.dict(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    feature_cluster = feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "block_modification_by": [],
        "catalog_id": app_container.catalog_id,
        "description": None,
        "entity_universe": {
            "query_template": {
                "formatted_expression": "SELECT "
                "DISTINCT\n"
                '  "cust_id"\n'
                "FROM "
                "online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c"
            }
        },
        "feature_ids": [deployed_float_feature.id],
        "feature_job_setting": {
            "blind_spot": "600s",
            "frequency": "1800s",
            "time_modulo_frequency": "300s",
        },
        "has_ttl": True,
        "last_materialized_at": None,
        "name": "fb_230525_271fb0_cust_id_30m_5m_10m_ttl",
        "output_column_names": ["sum_1d_V231227"],
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
        expected_feature_views={"fb_230525_271fb0_cust_id_30m_5m_10m_ttl"},
        expected_feature_services={"sum_1d_list"},
    )


@pytest.mark.asyncio
async def test_feature_table_two_features_deployed(
    app_container,
    document_service,
    periodic_task_service,
    deployed_float_feature,
    deployed_float_feature_post_processed,
    update_fixtures,
):
    """
    Test feature table creation and update when two features are deployed
    """
    feature_tables = await get_all_feature_tables(document_service)
    assert len(feature_tables) == 1
    feature_table = feature_tables["fb_230525_271fb0_cust_id_30m_5m_10m_ttl"]

    feature_table_dict = feature_table.dict(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    feature_cluster = feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "entity_universe": {
            "query_template": {
                "formatted_expression": "SELECT "
                "DISTINCT\n"
                '  "cust_id"\n'
                "FROM "
                "online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c"
            }
        },
        "feature_ids": [deployed_float_feature.id, deployed_float_feature_post_processed.id],
        "feature_job_setting": {
            "blind_spot": "600s",
            "frequency": "1800s",
            "time_modulo_frequency": "300s",
        },
        "has_ttl": True,
        "last_materialized_at": None,
        "name": f"fb_230525_271fb0_cust_id_30m_5m_10m_ttl",
        "output_column_names": ["sum_1d_V231227", "sum_1d_plus_123_V231227"],
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
        expected_feature_views={"fb_230525_271fb0_cust_id_30m_5m_10m_ttl"},
        expected_feature_services={"sum_1d_list", "sum_1d_plus_123_list"},
    )


@pytest.mark.asyncio
async def test_feature_table_undeploy(
    app_container,
    document_service,
    periodic_task_service,
    deployed_float_feature,
    deployed_float_feature_post_processed,
    mock_feature_materialize_service,
    update_fixtures,
):
    """
    Test feature table creation and update when two features are deployed
    """
    # Simulate online enabling two features then online disable one
    undeploy_feature(deployed_float_feature)

    feature_tables = await get_all_feature_tables(document_service)
    assert len(feature_tables) == 1
    feature_table = feature_tables[f"fb_230525_271fb0_cust_id_30m_5m_10m_ttl"]

    feature_table_dict = feature_table.dict(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    feature_cluster = feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "entity_universe": {
            "query_template": {
                "formatted_expression": "SELECT "
                "DISTINCT\n"
                '  "cust_id"\n'
                "FROM "
                "online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c"
            }
        },
        "feature_ids": [deployed_float_feature_post_processed.id],
        "feature_job_setting": {
            "blind_spot": "600s",
            "frequency": "1800s",
            "time_modulo_frequency": "300s",
        },
        "has_ttl": True,
        "last_materialized_at": None,
        "name": "fb_230525_271fb0_cust_id_30m_5m_10m_ttl",
        "output_column_names": ["sum_1d_plus_123_V231227"],
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

    # Check drop_columns called
    args, _ = mock_feature_materialize_service["drop_columns"].call_args
    assert args[0].name == "fb_230525_271fb0_cust_id_30m_5m_10m_ttl"
    assert args[1] == ["sum_1d_V231227"]

    # Check online disabling the last feature deletes the feature table
    undeploy_feature(deployed_float_feature_post_processed)
    feature_tables = await get_all_feature_tables(document_service)
    assert len(feature_tables) == 0
    assert not await has_scheduled_task(periodic_task_service, feature_table)

    args, _ = mock_feature_materialize_service["drop_table"].call_args
    assert args[0].name == "fb_230525_271fb0_cust_id_30m_5m_10m_ttl"
    await check_feast_registry(
        app_container,
        expected_feature_views=set(),
        expected_feature_services=set(),
    )


@pytest.mark.asyncio
async def test_feature_table_two_features_different_feature_job_settings_deployed(
    app_container,
    document_service,
    periodic_task_service,
    deployed_float_feature,
    deployed_float_feature_different_job_setting,
):
    """
    Test feature table creation and update when two features are deployed
    """
    feature_tables = await get_all_feature_tables(document_service)
    assert len(feature_tables) == 2

    # Check customer entity feature table
    feature_table = feature_tables["fb_230525_271fb0_cust_id_30m_5m_10m_ttl"]
    feature_table_dict = feature_table.dict(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    _ = feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "entity_universe": {
            "query_template": {
                "formatted_expression": "SELECT "
                "DISTINCT\n"
                '  "cust_id"\n'
                "FROM "
                "online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c"
            }
        },
        "feature_ids": [deployed_float_feature.id],
        "feature_job_setting": {
            "blind_spot": "600s",
            "frequency": "1800s",
            "time_modulo_frequency": "300s",
        },
        "has_ttl": True,
        "last_materialized_at": None,
        "name": "fb_230525_271fb0_cust_id_30m_5m_10m_ttl",
        "output_column_names": ["sum_1d_V231227"],
        "output_dtypes": ["FLOAT"],
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
    }
    assert await has_scheduled_task(periodic_task_service, feature_table)

    # Check item entity feature table
    feature_table = feature_tables["fb_230525_271fb0_cust_id_3h_5s_15m_ttl"]
    feature_table_dict = feature_table.dict(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    _ = feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "entity_universe": {
            "query_template": {
                "formatted_expression": "SELECT "
                "DISTINCT\n"
                '  "cust_id"\n'
                "FROM "
                "online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c"
            }
        },
        "feature_ids": [deployed_float_feature_different_job_setting.id],
        "feature_job_setting": {
            "blind_spot": "900s",
            "frequency": "10800s",
            "time_modulo_frequency": "5s",
        },
        "has_ttl": True,
        "last_materialized_at": None,
        "name": "fb_230525_271fb0_cust_id_3h_5s_15m_ttl",
        "output_column_names": ["sum_24h_every_3h_V231227"],
        "output_dtypes": ["FLOAT"],
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
    }
    assert await has_scheduled_task(periodic_task_service, feature_table)

    await check_feast_registry(
        app_container,
        expected_feature_views={
            "fb_230525_271fb0_cust_id_30m_5m_10m_ttl",
            "fb_230525_271fb0_cust_id_3h_5s_15m_ttl",
        },
        expected_feature_services={"sum_24h_every_3h_list", "sum_1d_list"},
    )


@pytest.mark.asyncio
async def test_feature_table_without_entity(
    app_container,
    document_service,
    periodic_task_service,
    deployed_feature_without_entity,
):
    """
    Test feature table creation when feature has no entity
    """
    feature_tables = await get_all_feature_tables(document_service)
    assert len(feature_tables) == 1
    feature_table = feature_tables[f"fb_230525_271fb0_1d_1h_2h_ttl"]

    feature_table_dict = feature_table.dict(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    _ = feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "entity_universe": {
            "query_template": {"formatted_expression": "SELECT\n" "  1 AS " '"dummy_entity"'}
        },
        "feature_ids": [deployed_feature_without_entity.id],
        "feature_job_setting": {
            "blind_spot": "7200s",
            "frequency": "86400s",
            "time_modulo_frequency": "3600s",
        },
        "has_ttl": True,
        "last_materialized_at": None,
        "name": "fb_230525_271fb0_1d_1h_2h_ttl",
        "output_column_names": ["count_1d_V231227"],
        "output_dtypes": ["INT"],
        "primary_entity_ids": [],
        "serving_names": [],
        "user_id": ObjectId("63f9506dd478b94127123456"),
    }
    assert await has_scheduled_task(periodic_task_service, feature_table)
    await check_feast_registry(
        app_container,
        expected_feature_views={"fb_230525_271fb0_1d_1h_2h_ttl"},
        expected_feature_services={"count_1d_list"},
    )


@pytest.mark.asyncio
async def test_lookup_feature(
    app_container,
    document_service,
    periodic_task_service,
    deployed_scd_lookup_feature,
    update_fixtures,
):
    """
    Test feature table creation with lookup feature
    """
    feature_tables = await get_all_feature_tables(document_service)
    assert len(feature_tables) == 1
    feature_table = feature_tables["fb_230525_271fb0_cust_id_1d_0s_0s"]

    feature_table_dict = feature_table.dict(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    _ = feature_table_dict.pop("feature_cluster")
    entity_universe = feature_table_dict.pop("entity_universe")
    assert_equal_with_expected_fixture(
        entity_universe["query_template"]["formatted_expression"],
        "tests/fixtures/offline_store_feature_table/lookup_feature_universe.sql",
        update_fixtures,
    )
    assert feature_table_dict == {
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "feature_ids": [deployed_scd_lookup_feature.id],
        "feature_job_setting": {
            "blind_spot": "0s",
            "frequency": "1d",
            "time_modulo_frequency": "0s",
        },
        "has_ttl": False,
        "last_materialized_at": None,
        "name": "fb_230525_271fb0_cust_id_1d_0s_0s",
        "output_column_names": ["some_lookup_feature_V231227"],
        "output_dtypes": ["BOOL"],
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
    }
    assert await has_scheduled_task(periodic_task_service, feature_table)
    await check_feast_registry(
        app_container,
        expected_feature_views={"fb_230525_271fb0_cust_id_1d_0s_0s"},
        expected_feature_services={"some_lookup_feature_list"},
    )


@pytest.mark.asyncio
async def test_aggregate_asat_feature(
    app_container,
    document_service,
    periodic_task_service,
    deployed_aggregate_asat_feature,
    update_fixtures,
):
    """
    Test feature table creation with aggregate asat feature
    """
    feature_tables = await get_all_feature_tables(document_service)
    assert len(feature_tables) == 1
    feature_table = feature_tables["fb_230525_271fb0_gender_1d_0s_0s"]

    feature_table_dict = feature_table.dict(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    _ = feature_table_dict.pop("feature_cluster")
    entity_universe = feature_table_dict.pop("entity_universe")
    assert_equal_with_expected_fixture(
        entity_universe["query_template"]["formatted_expression"],
        "tests/fixtures/offline_store_feature_table/aggregate_asat_feature_universe.sql",
        update_fixtures,
    )

    assert feature_table_dict == {
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "feature_ids": [deployed_aggregate_asat_feature.id],
        "feature_job_setting": {
            "blind_spot": "0s",
            "frequency": "1d",
            "time_modulo_frequency": "0s",
        },
        "has_ttl": False,
        "last_materialized_at": None,
        "name": "fb_230525_271fb0_gender_1d_0s_0s",
        "output_column_names": ["asat_gender_count_V231227"],
        "output_dtypes": ["INT"],
        "primary_entity_ids": deployed_aggregate_asat_feature.primary_entity_ids,
        "serving_names": ["gender"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
    }
    assert await has_scheduled_task(periodic_task_service, feature_table)
    await check_feast_registry(
        app_container,
        expected_feature_views={f"fb_230525_271fb0_gender_1d_0s_0s"},
        expected_feature_services={"asat_gender_count_list"},
    )


@pytest.mark.asyncio
async def test_new_deployment_when_all_features_already_deployed(
    app_container,
    deployed_feature_list_when_all_features_already_deployed,
):
    """
    Test enabling a new deployment when all the underlying features are already deployed
    """
    catalog_id = app_container.catalog_id
    await check_feast_registry(
        app_container,
        expected_feature_views={"fb_230525_271fb0_cust_id_30m_5m_10m_ttl"},
        expected_feature_services={
            "sum_1d_list",
            deployed_feature_list_when_all_features_already_deployed.name,
        },
    )


@pytest.mark.asyncio
async def test_multiple_parts_in_same_feature_table(test_dir):
    """
    Test feature table creation when a feature has multiple parts in the same table
    """
    fixture_filename = os.path.join(
        test_dir,
        "fixtures/feature/CUSTOMER_Age_Z_Score_to_FRENCHSTATE_invoice_customer_Age_4w.json",
    )
    with open(fixture_filename) as file_handle:
        feature_dict = json_util.loads(file_handle.read())
    feature_model = FeatureModel(**feature_dict)
    feature_model.initialize_offline_store_info(entity_id_to_serving_name={})

    offline_ingest_graph_container = await OfflineIngestGraphContainer.build([feature_model])
    offline_store_table_name_to_feature_ids = {
        table_name: [feat.id for feat in features]
        for (
            table_name,
            features,
        ) in offline_ingest_graph_container.offline_store_table_name_to_features.items()
    }
    assert offline_store_table_name_to_feature_ids == {
        "fb_240109_e0a7d1_659ccffb8c6f3c0e0a7d1e42_1h_2m_2m_ttl": [
            ObjectId("659cd19b7e511ad3fcdec2fe")
        ],
        "fb_240109_e0a7d1_659ccffa8c6f3c0e0a7d1e3d": [ObjectId("659cd19b7e511ad3fcdec2fe")],
    }
