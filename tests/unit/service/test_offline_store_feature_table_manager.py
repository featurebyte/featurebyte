"""
Tests for OfflineStoreFeatureTableManagerService
"""

import os
import pickle
import textwrap
from typing import Dict
from unittest.mock import patch

import pytest
import pytest_asyncio
from bson import ObjectId, json_util

from featurebyte.common.model_util import get_version
from featurebyte.exception import DocumentCreationError
from featurebyte.models.feature import FeatureModel
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.models.offline_store_ingest_query import OfflineStoreIngestQueryGraph
from featurebyte.models.precomputed_lookup_feature_table import get_lookup_steps_unique_identifier
from featurebyte.models.relationship import RelationshipType
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.registry import app_container_config
from featurebyte.schema.catalog import CatalogCreate
from featurebyte.service.offline_store_feature_table_manager import OfflineIngestGraphContainer
from tests.util.helper import (
    assert_equal_json_fixture,
    assert_equal_with_expected_fixture,
    deploy_feature,
    deploy_feature_list,
    get_relationship_info,
    undeploy_feature_async,
)


@pytest.fixture(autouse=True)
def mock_service_get_version():
    """
    Mock get version
    """
    with patch("featurebyte.service.base_feature_service.get_version", return_value="V231227"):
        yield


@pytest.fixture(name="float_feat_deployment_id")
def float_feat_deployment_id_fixture():
    """
    Deployment id fixture
    """
    return ObjectId()


@pytest.fixture(name="float_feat_post_processed_deployment_id")
def float_feat_post_processed_deployment_id_fixture():
    """
    Deployment id fixture
    """
    return ObjectId()


@pytest.fixture(name="float_feat_diff_fjs_deployment_id")
def float_feat_diff_fjs_deployment_id_fixture():
    """
    Deployment id fixture
    """
    return ObjectId()


@pytest.fixture(name="feat_without_entity_deployment_id")
def feat_without_entity_deployment_id_fixture():
    """
    Deployment id fixture
    """
    return ObjectId()


@pytest.fixture(name="scd_lookup_deployment_id")
def scd_lookup_deployment_id_fixture():
    """
    Deployment id fixture
    """
    return ObjectId()


@pytest.fixture(name="aggregate_asat_deployment_id")
def aggregate_asat_deployment_id_fixture():
    """
    Deployment id fixture
    """
    return ObjectId()


@pytest.fixture(name="complex_feat_deployment_id")
def complex_feat_deployment_id_fixture():
    """
    Deployment id fixture
    """
    return ObjectId()


@pytest.fixture(name="feat_with_internal_parent_child_relationships_deployment_id")
def feat_with_internal_parent_child_relationships_deployment_id_fixture():
    """
    Deployment id fixture
    """
    return ObjectId()


@pytest.fixture(name="latest_aggregation_with_window_deployment_id")
def latest_aggregation_with_window_deployment_id_fixture():
    """
    Deployment id fixture
    """
    return ObjectId()


@pytest.fixture(name="latest_aggregation_without_window_deployment_id")
def latest_aggregation_without_window_deployment_id_fixture():
    """
    Deployment id fixture
    """
    return ObjectId()


@pytest.fixture(name="feat_list_with_all_feats_deployed_deployment_id")
def feat_list_with_all_features_deployed_deployment_id_fixture():
    """
    Deployment id fixture
    """
    return ObjectId()


@pytest.fixture(name="count_distinct_window_aggregate_deployment_id")
def count_distinct_window_aggregate_deployment_id_fixture():
    """
    Deployment id fixture
    """
    return ObjectId()


@pytest_asyncio.fixture
async def deployed_float_feature_list(
    app_container,
    float_feature,
    transaction_entity,
    float_feat_deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for a deployed feature list with float feature
    """
    _ = mock_update_data_warehouse
    feature_list = await deploy_feature(
        app_container,
        float_feature,
        context_primary_entity_ids=[transaction_entity.id],
        return_type="feature_list",
        deployment_id=float_feat_deployment_id,
    )
    assert mock_offline_store_feature_manager_dependencies["initialize_new_columns"].call_count == 1
    assert mock_offline_store_feature_manager_dependencies["apply_comments"].call_count == 1
    return feature_list


@pytest_asyncio.fixture
async def deployed_float_feature(app_container, deployed_float_feature_list):
    """
    Fixture for a deployed float feature
    """
    feature = await app_container.feature_service.get_document(
        deployed_float_feature_list.feature_ids[0]
    )
    return feature


@pytest_asyncio.fixture
async def deployed_float_feature_list_cust_id_use_case(
    app_container,
    float_feature,
    float_feat_deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed float feature for custotmer use case
    """
    _ = mock_update_data_warehouse
    feature_list = await deploy_feature(
        app_container,
        float_feature,
        return_type="feature_list",
        deployment_id=float_feat_deployment_id,
    )
    assert mock_offline_store_feature_manager_dependencies["initialize_new_columns"].call_count == 1
    assert mock_offline_store_feature_manager_dependencies["apply_comments"].call_count == 1
    return feature_list


@pytest_asyncio.fixture
async def deployed_float_feature_post_processed(
    app_container, float_feature, transaction_entity, float_feat_post_processed_deployment_id
) -> FeatureModel:
    """
    Fixture for deployed feature that is post processed from float feature
    """
    feature = float_feature + 123
    feature.name = f"{float_feature.name}_plus_123"
    return await deploy_feature(
        app_container,
        feature,
        context_primary_entity_ids=[transaction_entity.id],
        deployment_id=float_feat_post_processed_deployment_id,
    )


@pytest_asyncio.fixture
async def deployed_float_feature_different_job_setting(
    app_container, float_feature_different_job_setting, float_feat_diff_fjs_deployment_id
):
    """
    Fixture for deployed float feature with different job setting
    """
    return await deploy_feature(
        app_container,
        float_feature_different_job_setting,
        deployment_id=float_feat_diff_fjs_deployment_id,
    )


@pytest_asyncio.fixture
async def deployed_feature_without_entity(
    app_container,
    feature_without_entity,
    feat_without_entity_deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed feature without entity
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    return await deploy_feature(
        app_container, feature_without_entity, deployment_id=feat_without_entity_deployment_id
    )


@pytest_asyncio.fixture
async def deployed_scd_lookup_feature(
    app_container,
    scd_lookup_feature,
    scd_lookup_deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed scd lookup feature
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    feature_list = await deploy_feature(
        app_container,
        scd_lookup_feature,
        return_type="feature_list",
        deployment_id=scd_lookup_deployment_id,
    )
    feature = await app_container.feature_service.get_document(feature_list.feature_ids[0])
    return feature


@pytest_asyncio.fixture
async def deployed_aggregate_asat_feature_list(
    app_container,
    aggregate_asat_feature,
    cust_id_entity,
    aggregate_asat_deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed aggregate asat feature
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    return await deploy_feature(
        app_container,
        aggregate_asat_feature,
        context_primary_entity_ids=[cust_id_entity.id],
        return_type="feature_list",
        deployment_id=aggregate_asat_deployment_id,
    )


@pytest_asyncio.fixture
async def deployed_aggregate_asat_feature(app_container, deployed_aggregate_asat_feature_list):
    """
    Fixture for deployed aggregate asat feature
    """
    return await app_container.feature_service.get_document(
        deployed_aggregate_asat_feature_list.feature_ids[0]
    )


@pytest_asyncio.fixture
async def deployed_item_aggregate_feature(
    app_container,
    non_time_based_feature,
    float_feat_deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed item aggregate feature
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    return await deploy_feature(
        app_container, non_time_based_feature, deployment_id=float_feat_deployment_id
    )


@pytest_asyncio.fixture
async def deployed_item_view_window_aggregate_feature(
    app_container,
    item_view_window_aggregate_feature,
    item_entity,
    float_feat_deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed item view window aggregate feature
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    return await deploy_feature(
        app_container,
        item_view_window_aggregate_feature,
        context_primary_entity_ids=[item_entity.id],
        deployment_id=float_feat_deployment_id,
    )


@pytest_asyncio.fixture
async def deployed_complex_feature(
    app_container,
    non_time_based_feature,
    feature_without_entity,
    complex_feat_deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for a complex feature with multiple parts in the same feature table
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    f1 = non_time_based_feature
    f2 = feature_without_entity
    new_feature = f1 + f2 + f1
    new_feature.name = "Complex Feature"
    return await deploy_feature(
        app_container, new_feature, deployment_id=complex_feat_deployment_id
    )


@pytest_asyncio.fixture
async def deployed_feature_with_internal_parent_child_relationships(
    app_container,
    feature_with_internal_parent_child_relationships,
    feat_with_internal_parent_child_relationships_deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for a complex feature with internal parent child relationships
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    return await deploy_feature(
        app_container,
        feature_with_internal_parent_child_relationships,
        deployment_id=feat_with_internal_parent_child_relationships_deployment_id,
    )


@pytest_asyncio.fixture
async def deployed_latest_aggregation_features(
    app_container,
    latest_event_timestamp_feature,
    latest_event_timestamp_unbounded_feature,
    latest_aggregation_with_window_deployment_id,
    latest_aggregation_without_window_deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed latest aggregation features
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    feature_1 = await deploy_feature(
        app_container,
        latest_event_timestamp_feature,
        deployment_id=latest_aggregation_with_window_deployment_id,
    )
    feature_2 = await deploy_feature(
        app_container,
        latest_event_timestamp_unbounded_feature,
        deployment_id=latest_aggregation_without_window_deployment_id,
    )
    return [feature_1, feature_2]


@pytest_asyncio.fixture
async def deployed_count_distinct_window_aggregate_feature(
    app_container,
    count_distinct_window_aggregate_feature,
    count_distinct_window_aggregate_deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for a deployed feature using count distinct aggregation
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies
    return await deploy_feature(
        app_container,
        count_distinct_window_aggregate_feature,
        deployment_id=count_distinct_window_aggregate_deployment_id,
    )


@pytest_asyncio.fixture
async def deployed_feature_list_when_all_features_already_deployed(
    app_container,
    float_feature,
    deployed_float_feature,
    feat_list_with_all_feats_deployed_deployment_id,
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
        deployment_id=feat_list_with_all_feats_deployed_deployment_id,
    )
    return out


@pytest.fixture(name="expected_feast_registry_mapping")
def expected_feast_registry_mapping_fixture(
    float_feat_deployment_id,
    float_feat_post_processed_deployment_id,
    float_feat_diff_fjs_deployment_id,
    feat_without_entity_deployment_id,
    scd_lookup_deployment_id,
    aggregate_asat_deployment_id,
    complex_feat_deployment_id,
    feat_with_internal_parent_child_relationships_deployment_id,
    feat_list_with_all_feats_deployed_deployment_id,
    latest_aggregation_with_window_deployment_id,
    latest_aggregation_without_window_deployment_id,
    count_distinct_window_aggregate_deployment_id,
):
    """Fixture for expected feast registry mapping"""
    fl_version = get_version()
    return {
        float_feat_deployment_id: {
            "feature_views": {
                "cat1_cust_id_30m",
                "cat1_cust_id_30m_via_transaction_id_{expected_suffix}",
            },
            "feature_services": {f"sum_1d_list_{fl_version}"},
        },
        float_feat_post_processed_deployment_id: {
            "feature_views": {
                "cat1_cust_id_30m",
                "cat1_cust_id_30m_via_transaction_id_{expected_suffix}",
            },
            "feature_services": {f"sum_1d_plus_123_list_{fl_version}"},
        },
        float_feat_diff_fjs_deployment_id: {
            "feature_views": {"cat1_cust_id_3h"},
            "feature_services": {f"sum_24h_every_3h_list_{fl_version}"},
        },
        feat_without_entity_deployment_id: {
            "feature_views": {"cat1__no_entity_1d"},
            "feature_services": {f"count_1d_list_{fl_version}"},
        },
        scd_lookup_deployment_id: {
            "feature_views": {"cat1_cust_id_1d"},
            "feature_services": {f"some_lookup_feature_list_{fl_version}"},
        },
        aggregate_asat_deployment_id: {
            "feature_views": {
                "cat1_gender_1d",
                "cat1_gender_1d_via_cust_id_{expected_suffix}",
            },
            "feature_services": {f"asat_gender_count_list_{fl_version}"},
        },
        complex_feat_deployment_id: {
            "feature_views": {"cat1__no_entity_1d", "cat1_transaction_id_1d"},
            "feature_services": {f"Complex Feature_list_{fl_version}"},
        },
        feat_with_internal_parent_child_relationships_deployment_id: {
            "feature_views": {"cat1_cust_id_1d"},
            "feature_services": {f"complex_parent_child_feature_list_{fl_version}"},
        },
        feat_list_with_all_feats_deployed_deployment_id: {
            "feature_views": {"cat1_cust_id_30m"},
            "feature_services": {f"my_new_feature_list_{fl_version}"},
        },
        latest_aggregation_with_window_deployment_id: {
            "feature_views": {"cat1_cust_id_30m"},
            "feature_services": {f"latest_event_timestamp_90d_list_{fl_version}"},
        },
        latest_aggregation_without_window_deployment_id: {
            "feature_views": {"cat1_cust_id_30m_1"},
            "feature_services": {f"latest_event_timestamp_list_{fl_version}"},
        },
        count_distinct_window_aggregate_deployment_id: {
            "feature_views": {"cat1_cust_id_30m"},
            "feature_services": {f"col_int_distinct_count_48h_list_{fl_version}"},
        },
    }


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


async def get_all_scheduled_tasks(periodic_task_service):
    """
    Helper function to check if there is a scheduled task
    """
    tasks = {}
    async for periodic_task in periodic_task_service.list_documents_iterator(
        query_filter={"kwargs.command": "FEATURE_MATERIALIZE_SYNC"}
    ):
        tasks[periodic_task.id] = periodic_task
    return tasks


async def has_scheduled_task(periodic_task_service, feature_table):
    """
    Helper function to check if there is a scheduled task
    """
    for periodic_task in (await get_all_scheduled_tasks(periodic_task_service)).values():
        if periodic_task.kwargs["offline_store_feature_table_id"] == str(feature_table.id):
            return True
    return False


async def check_feast_registry(
    app_container,
    deployment_id,
    expected_feast_registry_mapping,
    enabled_deployment,
    **format_kwargs,
):
    """
    Helper function to check feast registry
    """
    deployment = await app_container.deployment_service.get_document(document_id=deployment_id)
    assert deployment.registry_info is not None
    feast_registry = await app_container.feast_registry_service.get_document(
        document_id=deployment.registry_info.registry_id
    )
    assert feast_registry is not None
    feature_store = await app_container.feast_feature_store_service.get_feast_feature_store(
        feast_registry
    )

    # check feast feature store properties
    expected_project_name = str(deployment.catalog_id)[-7:]
    assert feature_store.project == expected_project_name
    if enabled_deployment:
        expected_feature_views = expected_feast_registry_mapping[deployment_id]["feature_views"]
        expected_feature_services = expected_feast_registry_mapping[deployment_id][
            "feature_services"
        ]
    else:
        expected_feature_views = set()
        expected_feature_services = set()

    # format expected feature views and feature services
    expected_feature_views = {fv.format(**format_kwargs) for fv in expected_feature_views}
    expected_feature_services = {fs.format(**format_kwargs) for fs in expected_feature_services}

    assert {fv.name for fv in feature_store.list_feature_views()} == expected_feature_views
    assert {fs.name for fs in feature_store.list_feature_services()} == expected_feature_services


@pytest_asyncio.fixture
async def transaction_to_customer_relationship_info(
    app_container, transaction_entity, cust_id_entity
):
    """
    Fixture for the relationship info id between transaction and customer entities
    """
    return await get_relationship_info(
        app_container,
        child_entity_id=transaction_entity.id,
        parent_entity_id=cust_id_entity.id,
    )


@pytest_asyncio.fixture
async def customer_to_gender_relationship_info(
    app_container,
    cust_id_entity,
    gender_entity,
):
    """
    Fixture for the relationship info id between customer and gender entities
    """
    return await get_relationship_info(
        app_container,
        child_entity_id=cust_id_entity.id,
        parent_entity_id=gender_entity.id,
    )


@pytest_asyncio.fixture
async def item_id_to_item_type_relationship_info(
    app_container,
    item_entity,
    item_type_entity,
):
    """
    Fixture for the relationship info id between item and item type entities
    """
    return await get_relationship_info(
        app_container,
        child_entity_id=item_entity.id,
        parent_entity_id=item_type_entity.id,
    )


@pytest_asyncio.fixture
async def deprecated_entity_lookup_feature_tables(app_container):
    """
    Fixture for a deprecated entity lookup feature table
    """
    table_id = ObjectId()
    model = OfflineStoreFeatureTableModel(
        _id=table_id,
        name="entity_lookup_table_name",
        feature_ids=[],
        primary_entity_ids=[ObjectId()],
        serving_names=["customer_id"],
        has_ttl=False,
        output_column_names=["city_id"],
        output_dtypes=["VARCHAR"],
        entity_lookup_info=EntityRelationshipInfo(
            relationship_type=RelationshipType.CHILD_PARENT,
            entity_id=ObjectId(),
            related_entity_id=ObjectId(),
            relation_table_id=ObjectId(),
        ),
        catalog_id=app_container.catalog_id,
    )
    doc = await app_container.offline_store_feature_table_service.create_document(model)
    return doc


@pytest.mark.usefixtures("deprecated_entity_lookup_feature_tables")
@pytest.mark.asyncio
async def test_feature_table_one_feature_deployed(
    app_container,
    document_service,
    periodic_task_service,
    deployed_float_feature,
    transaction_to_customer_relationship_info,
    float_feat_deployment_id,
    storage,
    expected_feast_registry_mapping,
    update_fixtures,
):
    """
    Test feature table creation when one feature is deployed
    """
    catalog_id = app_container.catalog_id
    feature_tables = await get_all_feature_tables(document_service)
    expected_suffix = get_lookup_steps_unique_identifier([
        transaction_to_customer_relationship_info
    ])
    assert set(feature_tables.keys()) == {
        "cat1_cust_id_30m",
        f"cat1_cust_id_30m_via_transaction_id_{expected_suffix}",
    }
    feature_table = feature_tables["cat1_cust_id_30m"]

    feature_table_dict = feature_table.model_dump(
        by_alias=True, exclude={"created_at", "updated_at"}
    )
    feature_table_id = feature_table_dict.pop("_id")
    feature_cluster = feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "aggregation_ids": ["sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"],
        "block_modification_by": [],
        "catalog_id": catalog_id,
        "description": None,
        "entity_universe": {
            "query_template": {
                "formatted_expression": textwrap.dedent(
                    """
                    SELECT DISTINCT
                      CAST("cust_id" AS BIGINT) AS "cust_id"
                    FROM (
                      SELECT
                        "cust_id" AS "cust_id"
                      FROM "sf_database"."sf_schema"."sf_table"
                      WHERE
                        "event_timestamp" >= CAST(FLOOR((
                          DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 300
                        ) / 1800) * 1800 + 300 - 600 - 86400 AS TIMESTAMP)
                        AND "event_timestamp" < CAST(FLOOR((
                          DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 300
                        ) / 1800) * 1800 + 300 - 600 AS TIMESTAMP)
                    )
                    WHERE
                      "cust_id" IS NOT NULL
                    """
                ).strip(),
                "source_type": "snowflake",
            }
        },
        "feature_ids": [deployed_float_feature.id],
        "feature_job_setting": {
            "blind_spot": "600s",
            "period": "1800s",
            "offset": "300s",
            "execution_buffer": "0s",
        },
        "has_ttl": True,
        "entity_lookup_info": None,
        "last_materialized_at": None,
        "name": "cat1_cust_id_30m",
        "base_name": "cust_id_30m",
        "name_prefix": "cat1",
        "name_suffix": None,
        "online_stores_last_materialized_at": [],
        "output_column_names": ["sum_1d_V231227"],
        "output_dtypes": ["FLOAT"],
        "precomputed_lookup_feature_table_info": None,
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
        "feature_store_id": feature_table_dict["feature_store_id"],
        "feature_cluster_path": feature_table_dict["feature_cluster_path"],
        "deployment_ids": [float_feat_deployment_id],
        "is_deleted": False,
    }
    assert_equal_json_fixture(
        feature_cluster,
        "tests/fixtures/offline_store_feature_table/feature_cluster_one_feature.json",
        update_fixtures,
    )

    assert await has_scheduled_task(periodic_task_service, feature_table)

    await check_feast_registry(
        app_container,
        deployment_id=float_feat_deployment_id,
        expected_feast_registry_mapping=expected_feast_registry_mapping,
        enabled_deployment=True,
        entity_rel_id1=transaction_to_customer_relationship_info.id,
        expected_suffix=expected_suffix,
    )

    # check that feature cluster file exists
    full_feature_cluster_path = os.path.join(
        storage.base_path, feature_table_dict["feature_cluster_path"]
    )
    assert os.path.exists(full_feature_cluster_path)

    # check precomputed lookup feature table
    feature_table = feature_tables[f"cat1_cust_id_30m_via_transaction_id_{expected_suffix}"]
    feature_table_dict = feature_table.model_dump(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    entity_universe = feature_table_dict.pop("entity_universe")
    assert feature_table_dict == {
        "aggregation_ids": [],
        "block_modification_by": [],
        "catalog_id": catalog_id,
        "description": None,
        "entity_lookup_info": None,
        "feature_cluster": None,
        "feature_cluster_path": None,
        "feature_ids": [],
        "feature_job_setting": None,
        "feature_store_id": deployed_float_feature.tabular_source.feature_store_id,
        "has_ttl": True,
        "last_materialized_at": None,
        "name": f"cat1_cust_id_30m_via_transaction_id_{expected_suffix}",
        "base_name": None,
        "name_prefix": None,
        "name_suffix": None,
        "online_stores_last_materialized_at": [],
        "output_column_names": [],
        "output_dtypes": [],
        "precomputed_lookup_feature_table_info": {
            "lookup_steps": [transaction_to_customer_relationship_info.model_dump(by_alias=True)],
            "lookup_mapping": [
                {
                    "lookup_feature_table_serving_name": "transaction_id",
                    "source_feature_table_serving_name": "cust_id",
                }
            ],
            "source_feature_table_id": feature_table_id,
        },
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379204")],
        "serving_names": ["transaction_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
        "deployment_ids": [float_feat_deployment_id],
        "is_deleted": False,
    }
    assert_equal_with_expected_fixture(
        entity_universe["query_template"]["formatted_expression"],
        "tests/fixtures/offline_store_feature_table/transaction_id_to_cust_id_universe.sql",
        update_fixture=update_fixtures,
    )


@pytest.mark.asyncio
async def test_feature_table_two_features_deployed(
    app_container,
    document_service,
    periodic_task_service,
    deployed_float_feature,
    deployed_float_feature_post_processed,
    transaction_to_customer_relationship_info,
    float_feat_deployment_id,
    float_feat_post_processed_deployment_id,
    expected_feast_registry_mapping,
    update_fixtures,
):
    """
    Test feature table creation and update when two features are deployed
    """
    feature_tables = await get_all_feature_tables(document_service)
    expected_suffix = get_lookup_steps_unique_identifier([
        transaction_to_customer_relationship_info
    ])
    assert set(feature_tables.keys()) == {
        "cat1_cust_id_30m",
        f"cat1_cust_id_30m_via_transaction_id_{expected_suffix}",
    }
    feature_table = feature_tables["cat1_cust_id_30m"]

    feature_table_dict = feature_table.model_dump(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    feature_cluster = feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "aggregation_ids": ["sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"],
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "entity_universe": {
            "query_template": {
                "formatted_expression": textwrap.dedent(
                    """
                    SELECT DISTINCT
                      CAST("cust_id" AS BIGINT) AS "cust_id"
                    FROM (
                      SELECT
                        "cust_id" AS "cust_id"
                      FROM "sf_database"."sf_schema"."sf_table"
                      WHERE
                        "event_timestamp" >= CAST(FLOOR((
                          DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 300
                        ) / 1800) * 1800 + 300 - 600 - 86400 AS TIMESTAMP)
                        AND "event_timestamp" < CAST(FLOOR((
                          DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 300
                        ) / 1800) * 1800 + 300 - 600 AS TIMESTAMP)
                    )
                    WHERE
                      "cust_id" IS NOT NULL
                    """
                ).strip(),
                "source_type": "snowflake",
            }
        },
        "feature_ids": [deployed_float_feature.id, deployed_float_feature_post_processed.id],
        "feature_job_setting": {
            "blind_spot": "600s",
            "period": "1800s",
            "offset": "300s",
            "execution_buffer": "0s",
        },
        "has_ttl": True,
        "entity_lookup_info": None,
        "last_materialized_at": None,
        "name": "cat1_cust_id_30m",
        "base_name": "cust_id_30m",
        "name_prefix": "cat1",
        "name_suffix": None,
        "online_stores_last_materialized_at": [],
        "output_column_names": ["sum_1d_V231227", "sum_1d_plus_123_V231227"],
        "output_dtypes": ["FLOAT", "FLOAT"],
        "precomputed_lookup_feature_table_info": None,
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
        "feature_store_id": feature_table_dict["feature_store_id"],
        "feature_cluster_path": feature_table_dict["feature_cluster_path"],
        "deployment_ids": sorted([
            float_feat_deployment_id,
            float_feat_post_processed_deployment_id,
        ]),
        "is_deleted": False,
    }
    assert_equal_json_fixture(
        feature_cluster,
        "tests/fixtures/offline_store_feature_table/feature_cluster_two_features.json",
        update_fixtures,
    )

    assert await has_scheduled_task(periodic_task_service, feature_table)

    # check two deployments' feast registry
    for deployment_id in [float_feat_deployment_id, float_feat_post_processed_deployment_id]:
        await check_feast_registry(
            app_container,
            deployment_id=deployment_id,
            expected_feast_registry_mapping=expected_feast_registry_mapping,
            enabled_deployment=True,
            entity_rel_id1=transaction_to_customer_relationship_info.id,
            expected_suffix=expected_suffix,
        )


@pytest.mark.asyncio
async def test_feature_table_undeploy(
    app_container,
    document_service,
    periodic_task_service,
    deployed_float_feature,
    deployed_float_feature_post_processed,
    transaction_to_customer_relationship_info,
    mock_offline_store_feature_manager_dependencies,
    storage,
    float_feat_deployment_id,
    float_feat_post_processed_deployment_id,
    expected_feast_registry_mapping,
    update_fixtures,
):
    """
    Test feature table creation and update when two features are deployed
    """
    # Simulate online enabling two features then online disable one
    await undeploy_feature_async(deployed_float_feature, app_container)

    feature_tables = await get_all_feature_tables(document_service)
    expected_suffix = get_lookup_steps_unique_identifier([
        transaction_to_customer_relationship_info
    ])
    assert set(feature_tables.keys()) == {
        "cat1_cust_id_30m",
        f"cat1_cust_id_30m_via_transaction_id_{expected_suffix}",
    }
    feature_table = feature_tables["cat1_cust_id_30m"]

    feature_table_dict = feature_table.model_dump(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    feature_cluster = feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "aggregation_ids": ["sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"],
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "entity_universe": {
            "query_template": {
                "formatted_expression": textwrap.dedent(
                    """
                    SELECT DISTINCT
                      CAST("cust_id" AS BIGINT) AS "cust_id"
                    FROM (
                      SELECT
                        "cust_id" AS "cust_id"
                      FROM "sf_database"."sf_schema"."sf_table"
                      WHERE
                        "event_timestamp" >= CAST(FLOOR((
                          DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 300
                        ) / 1800) * 1800 + 300 - 600 - 86400 AS TIMESTAMP)
                        AND "event_timestamp" < CAST(FLOOR((
                          DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 300
                        ) / 1800) * 1800 + 300 - 600 AS TIMESTAMP)
                    )
                    WHERE
                      "cust_id" IS NOT NULL
                    """
                ).strip(),
                "source_type": "snowflake",
            }
        },
        "feature_ids": [deployed_float_feature_post_processed.id],
        "feature_job_setting": {
            "blind_spot": "600s",
            "period": "1800s",
            "offset": "300s",
            "execution_buffer": "0s",
        },
        "has_ttl": True,
        "entity_lookup_info": None,
        "last_materialized_at": None,
        "name": "cat1_cust_id_30m",
        "base_name": "cust_id_30m",
        "name_prefix": "cat1",
        "name_suffix": None,
        "online_stores_last_materialized_at": [],
        "output_column_names": ["sum_1d_plus_123_V231227"],
        "output_dtypes": ["FLOAT"],
        "precomputed_lookup_feature_table_info": None,
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
        "feature_store_id": feature_table_dict["feature_store_id"],
        "feature_cluster_path": feature_table_dict["feature_cluster_path"],
        "deployment_ids": [float_feat_post_processed_deployment_id],
        "is_deleted": False,
    }
    assert_equal_json_fixture(
        feature_cluster,
        "tests/fixtures/offline_store_feature_table/feature_cluster_disabled_one_feature.json",
        update_fixtures,
    )

    # Check the feature cluster file exists
    full_feature_cluster_path = os.path.join(
        storage.base_path, feature_table_dict["feature_cluster_path"]
    )
    assert os.path.exists(full_feature_cluster_path)

    # Check drop_columns called
    args, _ = mock_offline_store_feature_manager_dependencies["drop_columns"].call_args_list[0]
    assert args[0].name == "cat1_cust_id_30m"
    assert args[1] == ["sum_1d_V231227"]

    args, _ = mock_offline_store_feature_manager_dependencies["drop_columns"].call_args_list[1]
    assert args[0].name == f"cat1_cust_id_30m_via_transaction_id_{expected_suffix}"
    assert args[1] == ["sum_1d_V231227"]

    # Check feast registry after first undeploy
    for deployment_id, enabled_deployment in [
        (float_feat_deployment_id, False),
        (float_feat_post_processed_deployment_id, True),
    ]:
        await check_feast_registry(
            app_container,
            deployment_id=deployment_id,
            expected_feast_registry_mapping=expected_feast_registry_mapping,
            enabled_deployment=enabled_deployment,
            entity_rel_id1=transaction_to_customer_relationship_info.id,
            expected_suffix=expected_suffix,
        )

    # Check online disabling the last feature deletes the feature table
    await undeploy_feature_async(deployed_float_feature_post_processed, app_container)
    feature_tables = await get_all_feature_tables(document_service)
    assert len(feature_tables) == 0
    assert not await has_scheduled_task(periodic_task_service, feature_table)

    # Check the feature cluster file is deleted
    assert not os.path.exists(full_feature_cluster_path)

    drop_table_calls = mock_offline_store_feature_manager_dependencies["drop_table"].call_args_list
    assert {c.args[0].name for c in drop_table_calls} == {
        "cat1_cust_id_30m",
        f"cat1_cust_id_30m_via_transaction_id_{expected_suffix}",
    }

    # Check feast registry after both undeploy
    for deployment_id in [float_feat_deployment_id, float_feat_post_processed_deployment_id]:
        await check_feast_registry(
            app_container,
            deployment_id=deployment_id,
            expected_feast_registry_mapping=expected_feast_registry_mapping,
            enabled_deployment=False,
            entity_rel_id1=transaction_to_customer_relationship_info.id,
            expected_suffix=expected_suffix,
        )


@pytest.mark.asyncio
async def test_feature_table_two_features_different_feature_job_settings_deployed(
    app_container,
    document_service,
    periodic_task_service,
    deployed_float_feature,
    deployed_float_feature_different_job_setting,
    transaction_to_customer_relationship_info,
    float_feat_deployment_id,
    float_feat_diff_fjs_deployment_id,
    expected_feast_registry_mapping,
):
    """
    Test feature table creation and update when two features are deployed
    """
    feature_tables = await get_all_feature_tables(document_service)
    expected_suffix = get_lookup_steps_unique_identifier([
        transaction_to_customer_relationship_info
    ])
    # The table cat1_cust_id_3h doesn't have a precomputed lookup feature table because the
    # deployment is expected to be served using cust_id entity
    assert set(feature_tables.keys()) == {
        "cat1_cust_id_30m",
        "cat1_cust_id_3h",
        f"cat1_cust_id_30m_via_transaction_id_{expected_suffix}",
    }

    # Check customer entity feature table
    feature_table = feature_tables["cat1_cust_id_30m"]
    feature_table_dict = feature_table.model_dump(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    _ = feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "aggregation_ids": ["sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"],
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "entity_universe": {
            "query_template": {
                "formatted_expression": textwrap.dedent(
                    """
                    SELECT DISTINCT
                      CAST("cust_id" AS BIGINT) AS "cust_id"
                    FROM (
                      SELECT
                        "cust_id" AS "cust_id"
                      FROM "sf_database"."sf_schema"."sf_table"
                      WHERE
                        "event_timestamp" >= CAST(FLOOR((
                          DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 300
                        ) / 1800) * 1800 + 300 - 600 - 86400 AS TIMESTAMP)
                        AND "event_timestamp" < CAST(FLOOR((
                          DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 300
                        ) / 1800) * 1800 + 300 - 600 AS TIMESTAMP)
                    )
                    WHERE
                      "cust_id" IS NOT NULL
                    """
                ).strip(),
                "source_type": "snowflake",
            }
        },
        "feature_ids": [deployed_float_feature.id],
        "feature_job_setting": {
            "blind_spot": "600s",
            "period": "1800s",
            "offset": "300s",
            "execution_buffer": "0s",
        },
        "has_ttl": True,
        "entity_lookup_info": None,
        "last_materialized_at": None,
        "name": "cat1_cust_id_30m",
        "base_name": "cust_id_30m",
        "name_prefix": "cat1",
        "name_suffix": None,
        "online_stores_last_materialized_at": [],
        "output_column_names": ["sum_1d_V231227"],
        "output_dtypes": ["FLOAT"],
        "precomputed_lookup_feature_table_info": None,
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
        "feature_store_id": feature_table_dict["feature_store_id"],
        "feature_cluster_path": feature_table_dict["feature_cluster_path"],
        "deployment_ids": [float_feat_deployment_id],
        "is_deleted": False,
    }
    assert await has_scheduled_task(periodic_task_service, feature_table)

    # Check item entity feature table
    feature_table = feature_tables["cat1_cust_id_3h"]
    feature_table_dict = feature_table.model_dump(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    _ = feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "aggregation_ids": ["sum_420f46a4414d6fc926c85a1349835967a96bf4c2"],
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "entity_universe": {
            "query_template": {
                "formatted_expression": textwrap.dedent(
                    """
                    SELECT DISTINCT
                      CAST("cust_id" AS BIGINT) AS "cust_id"
                    FROM (
                      SELECT
                        "cust_id" AS "cust_id"
                      FROM "sf_database"."sf_schema"."sf_table"
                      WHERE
                        "event_timestamp" >= CAST(FLOOR((
                          DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 5
                        ) / 10800) * 10800 + 5 - 900 - 86400 AS TIMESTAMP)
                        AND "event_timestamp" < CAST(FLOOR((
                          DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 5
                        ) / 10800) * 10800 + 5 - 900 AS TIMESTAMP)
                    )
                    WHERE
                      "cust_id" IS NOT NULL
                    """
                ).strip(),
                "source_type": "snowflake",
            }
        },
        "feature_ids": [deployed_float_feature_different_job_setting.id],
        "feature_job_setting": {
            "blind_spot": "900s",
            "period": "10800s",
            "offset": "5s",
            "execution_buffer": "0s",
        },
        "has_ttl": True,
        "entity_lookup_info": None,
        "last_materialized_at": None,
        "name": "cat1_cust_id_3h",
        "base_name": "cust_id_3h",
        "name_prefix": "cat1",
        "name_suffix": None,
        "online_stores_last_materialized_at": [],
        "output_column_names": ["sum_24h_every_3h_V231227"],
        "output_dtypes": ["FLOAT"],
        "precomputed_lookup_feature_table_info": None,
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
        "feature_store_id": feature_table_dict["feature_store_id"],
        "feature_cluster_path": feature_table_dict["feature_cluster_path"],
        "deployment_ids": [float_feat_diff_fjs_deployment_id],
        "is_deleted": False,
    }
    assert await has_scheduled_task(periodic_task_service, feature_table)

    # Check feast registry after both deployments
    for deployment_id in [float_feat_deployment_id, float_feat_diff_fjs_deployment_id]:
        await check_feast_registry(
            app_container,
            deployment_id=deployment_id,
            expected_feast_registry_mapping=expected_feast_registry_mapping,
            enabled_deployment=True,
            entity_rel_id1=transaction_to_customer_relationship_info.id,
            expected_suffix=expected_suffix,
        )


@pytest.mark.asyncio
async def test_feature_table_without_entity(
    app_container,
    document_service,
    periodic_task_service,
    deployed_feature_without_entity,
    feat_without_entity_deployment_id,
    expected_feast_registry_mapping,
):
    """
    Test feature table creation when feature has no entity
    """
    feature_tables = await get_all_feature_tables(document_service)
    assert len(feature_tables) == 1
    feature_table = feature_tables["cat1__no_entity_1d"]

    feature_table_dict = feature_table.model_dump(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    _ = feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "aggregation_ids": ["count_3178e5d8142ed182c5db45462cb780d18205bd64"],
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "entity_universe": {
            "query_template": {
                "formatted_expression": 'SELECT\n  1 AS "dummy_entity"',
                "source_type": "snowflake",
            }
        },
        "feature_ids": [deployed_feature_without_entity.id],
        "feature_job_setting": {
            "blind_spot": "7200s",
            "period": "86400s",
            "offset": "3600s",
            "execution_buffer": "0s",
        },
        "has_ttl": True,
        "entity_lookup_info": None,
        "last_materialized_at": None,
        "name": "cat1__no_entity_1d",
        "base_name": "_no_entity_1d",
        "name_prefix": "cat1",
        "name_suffix": None,
        "online_stores_last_materialized_at": [],
        "output_column_names": ["count_1d_V231227"],
        "output_dtypes": ["INT"],
        "precomputed_lookup_feature_table_info": None,
        "primary_entity_ids": [],
        "serving_names": [],
        "user_id": ObjectId("63f9506dd478b94127123456"),
        "feature_store_id": feature_table_dict["feature_store_id"],
        "feature_cluster_path": feature_table_dict["feature_cluster_path"],
        "deployment_ids": [feat_without_entity_deployment_id],
        "is_deleted": False,
    }
    assert await has_scheduled_task(periodic_task_service, feature_table)
    await check_feast_registry(
        app_container,
        deployment_id=feat_without_entity_deployment_id,
        expected_feast_registry_mapping=expected_feast_registry_mapping,
        enabled_deployment=True,
    )


@pytest.mark.asyncio
async def test_lookup_feature(
    app_container,
    document_service,
    periodic_task_service,
    deployed_scd_lookup_feature,
    scd_lookup_deployment_id,
    expected_feast_registry_mapping,
    update_fixtures,
):
    """
    Test feature table creation with lookup feature
    """
    feature_tables = await get_all_feature_tables(document_service)
    assert len(feature_tables) == 1
    feature_table = feature_tables["cat1_cust_id_1d"]

    feature_table_dict = feature_table.model_dump(
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
        "aggregation_ids": [],
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "feature_ids": [deployed_scd_lookup_feature.id],
        "feature_job_setting": {
            "blind_spot": "0s",
            "period": "86400s",
            "offset": "0s",
            "execution_buffer": "0s",
        },
        "has_ttl": False,
        "entity_lookup_info": None,
        "last_materialized_at": None,
        "name": "cat1_cust_id_1d",
        "base_name": "cust_id_1d",
        "name_prefix": "cat1",
        "name_suffix": None,
        "online_stores_last_materialized_at": [],
        "output_column_names": ["some_lookup_feature_V231227"],
        "output_dtypes": ["BOOL"],
        "precomputed_lookup_feature_table_info": None,
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
        "feature_store_id": feature_table_dict["feature_store_id"],
        "feature_cluster_path": feature_table_dict["feature_cluster_path"],
        "deployment_ids": [scd_lookup_deployment_id],
        "is_deleted": False,
    }
    assert await has_scheduled_task(periodic_task_service, feature_table)
    await check_feast_registry(
        app_container,
        deployment_id=scd_lookup_deployment_id,
        expected_feast_registry_mapping=expected_feast_registry_mapping,
        enabled_deployment=True,
    )


@pytest.mark.asyncio
async def test_aggregate_asat_feature(
    app_container,
    document_service,
    periodic_task_service,
    deployed_aggregate_asat_feature,
    customer_to_gender_relationship_info,
    aggregate_asat_deployment_id,
    expected_feast_registry_mapping,
    update_fixtures,
):
    """
    Test feature table creation with aggregate asat feature
    """
    feature_tables = await get_all_feature_tables(document_service)
    expected_suffix = get_lookup_steps_unique_identifier([customer_to_gender_relationship_info])
    assert set(feature_tables.keys()) == {
        "cat1_gender_1d",
        f"cat1_gender_1d_via_cust_id_{expected_suffix}",
    }
    feature_table = feature_tables["cat1_gender_1d"]

    feature_table_dict = feature_table.model_dump(
        by_alias=True, exclude={"created_at", "updated_at"}
    )
    feature_table_id = feature_table_dict.pop("_id")
    _ = feature_table_dict.pop("feature_cluster")
    entity_universe = feature_table_dict.pop("entity_universe")
    assert_equal_with_expected_fixture(
        entity_universe["query_template"]["formatted_expression"],
        "tests/fixtures/offline_store_feature_table/aggregate_asat_feature_universe.sql",
        update_fixtures,
    )

    assert feature_table_dict == {
        "aggregation_ids": [],
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "feature_ids": [deployed_aggregate_asat_feature.id],
        "feature_job_setting": {
            "blind_spot": "0s",
            "period": "86400s",
            "offset": "0s",
            "execution_buffer": "0s",
        },
        "has_ttl": False,
        "entity_lookup_info": None,
        "last_materialized_at": None,
        "name": "cat1_gender_1d",
        "base_name": "gender_1d",
        "name_prefix": "cat1",
        "name_suffix": None,
        "online_stores_last_materialized_at": [],
        "output_column_names": ["asat_gender_count_V231227"],
        "output_dtypes": ["INT"],
        "precomputed_lookup_feature_table_info": None,
        "primary_entity_ids": deployed_aggregate_asat_feature.primary_entity_ids,
        "serving_names": ["gender"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
        "feature_store_id": feature_table_dict["feature_store_id"],
        "feature_cluster_path": feature_table_dict["feature_cluster_path"],
        "deployment_ids": [aggregate_asat_deployment_id],
        "is_deleted": False,
    }
    assert await has_scheduled_task(periodic_task_service, feature_table)
    await check_feast_registry(
        app_container,
        deployment_id=aggregate_asat_deployment_id,
        expected_feast_registry_mapping=expected_feast_registry_mapping,
        enabled_deployment=True,
        entity_rel_id1=customer_to_gender_relationship_info.id,
        expected_suffix=expected_suffix,
    )

    # check precomputed lookup feature table
    feature_table = feature_tables[f"cat1_gender_1d_via_cust_id_{expected_suffix}"]
    feature_table_dict = feature_table.model_dump(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    entity_universe = feature_table_dict.pop("entity_universe")
    assert feature_table_dict == {
        "aggregation_ids": [],
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "description": None,
        "entity_lookup_info": None,
        "feature_cluster": None,
        "feature_cluster_path": None,
        "feature_ids": [],
        "feature_job_setting": None,
        "feature_store_id": deployed_aggregate_asat_feature.tabular_source.feature_store_id,
        "has_ttl": False,
        "last_materialized_at": None,
        "name": f"cat1_gender_1d_via_cust_id_{expected_suffix}",
        "base_name": None,
        "name_prefix": None,
        "name_suffix": None,
        "online_stores_last_materialized_at": [],
        "output_column_names": [],
        "output_dtypes": [],
        "precomputed_lookup_feature_table_info": {
            "lookup_steps": [customer_to_gender_relationship_info.model_dump(by_alias=True)],
            "lookup_mapping": [
                {
                    "lookup_feature_table_serving_name": "cust_id",
                    "source_feature_table_serving_name": "gender",
                }
            ],
            "source_feature_table_id": feature_table_id,
        },
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
        "deployment_ids": [aggregate_asat_deployment_id],
        "is_deleted": False,
    }
    assert_equal_with_expected_fixture(
        entity_universe["query_template"]["formatted_expression"],
        "tests/fixtures/offline_store_feature_table/cust_id_to_gender_universe.sql",
        update_fixture=update_fixtures,
    )


@pytest.mark.asyncio
async def test_new_deployment_when_all_features_already_deployed(
    app_container,
    deployed_feature_list_when_all_features_already_deployed,
    transaction_to_customer_relationship_info,
    feat_list_with_all_feats_deployed_deployment_id,
    expected_feast_registry_mapping,
):
    """
    Test enabling a new deployment when all the underlying features are already deployed
    """
    _ = deployed_feature_list_when_all_features_already_deployed
    _ = transaction_to_customer_relationship_info
    await check_feast_registry(
        app_container,
        deployment_id=feat_list_with_all_feats_deployed_deployment_id,
        expected_feast_registry_mapping=expected_feast_registry_mapping,
        enabled_deployment=True,
    )


@pytest.mark.asyncio
async def test_multiple_parts_in_same_feature_table(
    test_dir, persistent, user, snowflake_feature_store
):
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

    # create catalog document
    catalog_id = feature_model.catalog_id
    app_container = LazyAppContainer(
        app_container_config=app_container_config,
        instance_map={
            "user": user,
            "persistent": persistent,
            "catalog_id": catalog_id,
        },
    )
    await app_container.catalog_service.create_document(
        data=CatalogCreate(
            _id=catalog_id,
            name="test_catalog",
            default_feature_store_ids=[snowflake_feature_store.id],
        )
    )

    # initialize offline store info
    service = app_container.offline_store_info_initialization_service
    offline_store_info = await service.initialize_offline_store_info(
        feature=feature_model,
        table_name_prefix="cat1",
        entity_id_to_serving_name={
            entity_id: str(entity_id) for entity_id in feature_model.entity_ids
        },
    )
    feature_model.internal_offline_store_info = offline_store_info.model_dump(by_alias=True)

    offline_ingest_graph_container = await OfflineIngestGraphContainer.build([feature_model])
    offline_store_table_name_to_feature_ids = {
        table_name: [feat.id for feat in features]
        for (
            table_name,
            features,
        ) in offline_ingest_graph_container.offline_store_table_name_to_features.items()
    }
    assert offline_store_table_name_to_feature_ids == {
        "cat1_659ccffa8c6f3c0e0a7_1d": [ObjectId("659cd19b7e511ad3fcdec2fe")],
        "cat1_659ccffb8c6f3c0e0a7_1h": [ObjectId("659cd19b7e511ad3fcdec2fe")],
    }


@pytest.mark.asyncio
async def test_new_deployment_on_already_enabled_feature_list(
    app_container,
    document_service,
    deployed_float_feature_list_cust_id_use_case,
    transaction_entity,
    transaction_to_customer_relationship_info,
):
    """
    Test precomputed lookup feature table is created for a no-op deployment request (when all the
    underlying features are already online enabled)
    """
    feature_tables = await get_all_feature_tables(document_service)
    assert set(feature_tables.keys()) == {
        "cat1_cust_id_30m",
    }
    deployment_ids_before = feature_tables["cat1_cust_id_30m"].deployment_ids

    # Make a new deployment with transaction use case. Now we need to be able to serve this feature
    # list using child entity transaction.
    deployment_id = ObjectId()
    _ = await deploy_feature_list(
        app_container,
        deployed_float_feature_list_cust_id_use_case,
        context_primary_entity_ids=[transaction_entity.id],
        deployment_name_override="another_deployment_same_feature_list",
        deployment_id=deployment_id,
    )

    # Check offline feature tables
    feature_tables = await get_all_feature_tables(document_service)
    expected_suffix = get_lookup_steps_unique_identifier([
        transaction_to_customer_relationship_info
    ])
    assert set(feature_tables.keys()) == {
        "cat1_cust_id_30m",
        f"cat1_cust_id_30m_via_transaction_id_{expected_suffix}",
    }
    deployment_ids_after = feature_tables["cat1_cust_id_30m"].deployment_ids
    assert set(deployment_ids_after) == set(deployment_ids_before + [deployment_id])
    assert feature_tables[
        f"cat1_cust_id_30m_via_transaction_id_{expected_suffix}"
    ].deployment_ids == [deployment_id]


@pytest.mark.asyncio
async def test_deployment_enabled_with_complex_feature(
    app_container,
    deployed_complex_feature,
    complex_feat_deployment_id,
    expected_feast_registry_mapping,
):
    """
    Test enabling a deployment with a complex feature
    """
    _ = deployed_complex_feature
    await check_feast_registry(
        app_container,
        deployment_id=complex_feat_deployment_id,
        expected_feast_registry_mapping=expected_feast_registry_mapping,
        enabled_deployment=True,
    )


@pytest.mark.asyncio
async def test_feature_with_internal_parent_child_relationships(
    app_container,
    document_service,
    deployed_feature_with_internal_parent_child_relationships,
    feat_with_internal_parent_child_relationships_deployment_id,
    expected_feast_registry_mapping,
    update_fixtures,
):
    """
    Test feature with internal parent child relationships. That information should be reflected in
    the feature cluster of the offline store feature table.
    """
    catalog_id = app_container.catalog_id
    feature_tables = await get_all_feature_tables(document_service)
    assert set(feature_tables.keys()) == {
        "cat1_cust_id_1d",
    }
    feature_table = feature_tables["cat1_cust_id_1d"]

    feature_table_dict = feature_table.model_dump(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    feature_cluster = feature_table_dict.pop("feature_cluster")

    # Remove dynamic fields before comparison
    for node_info in feature_cluster["feature_node_relationships_infos"]:
        for info in node_info["relationships_info"]:
            info.pop("_id")
    for info in feature_cluster["combined_relationships_info"]:
        info.pop("_id")

    # Check feast registry
    await check_feast_registry(
        app_container,
        deployment_id=feat_with_internal_parent_child_relationships_deployment_id,
        expected_feast_registry_mapping=expected_feast_registry_mapping,
        enabled_deployment=True,
    )

    _ = feature_table_dict.pop("entity_universe")  # covered in service/test_feature_materialize.py
    assert feature_table_dict == {
        "aggregation_ids": [],
        "block_modification_by": [],
        "catalog_id": catalog_id,
        "description": None,
        "entity_lookup_info": None,
        "feature_cluster_path": feature_table_dict["feature_cluster_path"],
        "feature_ids": [deployed_feature_with_internal_parent_child_relationships.id],
        "feature_job_setting": {
            "blind_spot": "0s",
            "period": "86400s",
            "offset": "0s",
            "execution_buffer": "0s",
        },
        "feature_store_id": ObjectId("646f6c190ed28a5271fb02a1"),
        "has_ttl": False,
        "last_materialized_at": None,
        "name": "cat1_cust_id_1d",
        "base_name": "cust_id_1d",
        "name_prefix": "cat1",
        "name_suffix": None,
        "online_stores_last_materialized_at": [],
        "output_column_names": ["complex_parent_child_feature_V231227"],
        "output_dtypes": ["VARCHAR"],
        "precomputed_lookup_feature_table_info": None,
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
        "deployment_ids": [feat_with_internal_parent_child_relationships_deployment_id],
        "is_deleted": False,
    }
    assert_equal_json_fixture(
        feature_cluster,
        "tests/fixtures/offline_store_feature_table/feature_cluster_with_internal_relationships.json",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_deployment_failure_cleanup(
    app_container,
    float_feature,
    transaction_entity,
    float_feat_deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
    document_service,
    periodic_task_service,
):
    """
    Test failure during deployment should perform necessary cleanups
    """
    _ = mock_update_data_warehouse

    # Simulate a failure during deployment in handle_online_enabled_features() after feature tables
    # are created and feature materialize tasks are scheduled
    mock_offline_store_feature_manager_dependencies["apply_comments"].side_effect = ValueError(
        "Failing on purpose"
    )
    with pytest.raises(DocumentCreationError) as exc_info:
        await deploy_feature(
            app_container,
            float_feature,
            context_primary_entity_ids=[transaction_entity.id],
            return_type="feature_list",
            deployment_id=float_feat_deployment_id,
        )
    assert str(exc_info.value) == "Failed to create deployment"

    # Check that feature tables and tasks are cleaned up
    feature_tables = await get_all_feature_tables(document_service)
    scheduled_tasks = await get_all_scheduled_tasks(periodic_task_service)
    assert not list(feature_tables.keys())
    assert not list(scheduled_tasks.keys())


@pytest.mark.asyncio
async def test_entity_universe_debug_info_dump(
    app_container,
    float_feature,
    transaction_entity,
    float_feat_deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
    document_service,
    periodic_task_service,
    storage,
):
    """
    Test entity universe dump during deployment error for troubleshooting
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies

    with patch(
        "featurebyte.service.offline_store_feature_table_construction.get_combined_universe",
        return_value=None,
    ):
        with pytest.raises(DocumentCreationError) as exc_info:
            await deploy_feature(
                app_container,
                float_feature,
                context_primary_entity_ids=[transaction_entity.id],
                return_type="feature_list",
                deployment_id=float_feat_deployment_id,
            )
    assert str(exc_info.value) == "Failed to create deployment"

    # Check ingest graphs dump for troubleshooting
    feature_table_name = "cat1_cust_id_30m"
    path = f"catalog/{app_container.catalog_id}/offline_store_feature_table/{feature_table_name}/entity_universe_debug_info.pickle"
    debug_info_bytes = await storage.get_bytes(path)
    debug_info = pickle.loads(debug_info_bytes)
    assert isinstance(debug_info, dict)
    ingest_graphs_dump = debug_info["offline_ingest_graphs"]
    assert len(ingest_graphs_dump) == 1
    assert isinstance(ingest_graphs_dump[0][0], OfflineStoreIngestQueryGraph)

    # Check that feature tables and tasks are cleaned up
    feature_tables = await get_all_feature_tables(document_service)
    scheduled_tasks = await get_all_scheduled_tasks(periodic_task_service)
    assert not list(feature_tables.keys())
    assert not list(scheduled_tasks.keys())


@pytest.mark.asyncio
async def test_undeploy_bad_state_error_handling(
    app_container,
    deployed_float_feature,
    deployed_float_feature_post_processed,
    document_service,
    periodic_task_service,
    storage,
    caplog,
):
    """
    Test OfflineStoreFeatureTableBadStateError is handled when disabling a deployment
    """
    _ = deployed_float_feature_post_processed

    # Simulate OfflineStoreFeatureTableBadStateError during undeployment
    with patch(
        "featurebyte.service.offline_store_feature_table_construction.get_combined_universe",
        return_value=None,
    ):
        await undeploy_feature_async(deployed_float_feature, app_container)

    # Check ingest graphs dump for troubleshooting
    feature_table_name = "cat1_cust_id_30m"
    path = f"catalog/{app_container.catalog_id}/offline_store_feature_table/{feature_table_name}/entity_universe_debug_info.pickle"
    debug_info_bytes = await storage.get_bytes(path)
    debug_info = pickle.loads(debug_info_bytes)
    assert isinstance(debug_info, dict)
    ingest_graphs_dump = debug_info["offline_ingest_graphs"]
    assert len(ingest_graphs_dump) == 1
    assert isinstance(ingest_graphs_dump[0][0], OfflineStoreIngestQueryGraph)

    # Check error logged
    logs = [
        record
        for record in caplog.records
        if record.msg == "Failed to update offline store feature table when disabling a deployment"
    ]
    assert len(logs) == 1

    # Check that feature tables and tasks are cleaned up
    feature_tables = await get_all_feature_tables(document_service)
    scheduled_tasks = await get_all_scheduled_tasks(periodic_task_service)
    assert not list(feature_tables.keys())
    assert not list(scheduled_tasks.keys())


@pytest.mark.asyncio
async def test_item_view_window_aggregate(
    app_container,
    document_service,
    deployed_item_view_window_aggregate_feature,
    item_id_to_item_type_relationship_info,
    float_feat_deployment_id,
    item_entity,
    update_fixtures,
):
    """
    Test feature table creation for a feature with item view window aggregate
    """
    catalog_id = app_container.catalog_id
    feature_tables = await get_all_feature_tables(document_service)
    expected_suffix = get_lookup_steps_unique_identifier([item_id_to_item_type_relationship_info])
    assert set(feature_tables.keys()) == {
        "cat1_item_type_30m",
        f"cat1_item_type_30m_via_item_id_{expected_suffix}",
    }
    feature_table = feature_tables["cat1_item_type_30m"]

    feature_table_dict = feature_table.model_dump(
        by_alias=True, exclude={"created_at", "updated_at"}
    )
    feature_table_id = feature_table_dict.pop("_id")
    feature_table_dict.pop("feature_cluster")
    assert feature_table_dict == {
        "aggregation_ids": ["sum_2e4057b32df81d547bde013cd755a1189af7e615"],
        "base_name": "item_type_30m",
        "block_modification_by": [],
        "catalog_id": catalog_id,
        "deployment_ids": [float_feat_deployment_id],
        "description": None,
        "entity_lookup_info": None,
        "entity_universe": {
            "query_template": {
                "formatted_expression": textwrap.dedent(
                    """
                    SELECT DISTINCT
                      "item_type" AS "item_type"
                    FROM (
                      SELECT
                        L."item_type" AS "item_type"
                      FROM (
                        SELECT
                          "event_id_col" AS "event_id_col",
                          "item_id_col" AS "item_id_col",
                          "item_type" AS "item_type",
                          "item_amount" AS "item_amount",
                          "created_at" AS "created_at",
                          "event_timestamp" AS "event_timestamp"
                        FROM "sf_database"."sf_schema"."items_table"
                      ) AS L
                      INNER JOIN (
                        SELECT
                          "col_int",
                          ANY_VALUE("col_float") AS "col_float",
                          ANY_VALUE("col_char") AS "col_char",
                          ANY_VALUE("col_text") AS "col_text",
                          ANY_VALUE("col_binary") AS "col_binary",
                          ANY_VALUE("col_boolean") AS "col_boolean",
                          ANY_VALUE("event_timestamp") AS "event_timestamp",
                          ANY_VALUE("cust_id") AS "cust_id"
                        FROM (
                          SELECT
                            "col_int" AS "col_int",
                            "col_float" AS "col_float",
                            "col_char" AS "col_char",
                            "col_text" AS "col_text",
                            "col_binary" AS "col_binary",
                            "col_boolean" AS "col_boolean",
                            "event_timestamp" AS "event_timestamp",
                            "cust_id" AS "cust_id"
                          FROM "sf_database"."sf_schema"."sf_table"
                        )
                        GROUP BY
                          "col_int"
                      ) AS R
                        ON L."event_id_col" = R."col_int"
                      WHERE
                        "event_timestamp_event" >= CAST(FLOOR((
                          DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 300
                        ) / 1800) * 1800 + 300 - 600 - 86400 AS TIMESTAMP)
                        AND "event_timestamp_event" < CAST(FLOOR((
                          DATE_PART(EPOCH_SECOND, "__fb_current_feature_timestamp") - 300
                        ) / 1800) * 1800 + 300 - 600 AS TIMESTAMP)
                    )
                    WHERE
                      "item_type" IS NOT NULL
                    """
                ).strip(),
                "source_type": "snowflake",
            }
        },
        "feature_cluster_path": feature_table_dict["feature_cluster_path"],
        "feature_ids": [deployed_item_view_window_aggregate_feature.id],
        "feature_job_setting": {
            "blind_spot": "600s",
            "period": "1800s",
            "offset": "300s",
            "execution_buffer": "0s",
        },
        "feature_store_id": feature_table_dict["feature_store_id"],
        "has_ttl": True,
        "last_materialized_at": None,
        "name": "cat1_item_type_30m",
        "name_prefix": "cat1",
        "name_suffix": None,
        "online_stores_last_materialized_at": [],
        "output_column_names": ["sum_1d_V231227"],
        "output_dtypes": ["FLOAT"],
        "precomputed_lookup_feature_table_info": None,
        "primary_entity_ids": [ObjectId("664a3e7d7ac430c2ae37aedf")],
        "serving_names": ["item_type"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
        "is_deleted": False,
    }

    # check precomputed lookup feature table
    feature_table = feature_tables[f"cat1_item_type_30m_via_item_id_{expected_suffix}"]
    feature_table_dict = feature_table.model_dump(
        by_alias=True, exclude={"created_at", "updated_at", "id"}
    )
    entity_universe = feature_table_dict.pop("entity_universe")
    assert feature_table_dict == {
        "aggregation_ids": [],
        "block_modification_by": [],
        "catalog_id": catalog_id,
        "description": None,
        "entity_lookup_info": None,
        "feature_cluster": None,
        "feature_cluster_path": None,
        "feature_ids": [],
        "feature_job_setting": None,
        "feature_store_id": deployed_item_view_window_aggregate_feature.tabular_source.feature_store_id,
        "has_ttl": True,
        "last_materialized_at": None,
        "name": f"cat1_item_type_30m_via_item_id_{expected_suffix}",
        "base_name": None,
        "name_prefix": None,
        "name_suffix": None,
        "online_stores_last_materialized_at": [],
        "output_column_names": [],
        "output_dtypes": [],
        "precomputed_lookup_feature_table_info": {
            "lookup_steps": [item_id_to_item_type_relationship_info.model_dump(by_alias=True)],
            "lookup_mapping": [
                {
                    "lookup_feature_table_serving_name": "item_id",
                    "source_feature_table_serving_name": "item_type",
                }
            ],
            "source_feature_table_id": feature_table_id,
        },
        "primary_entity_ids": [item_entity.id],
        "serving_names": item_entity.serving_names,
        "user_id": ObjectId("63f9506dd478b94127123456"),
        "deployment_ids": [float_feat_deployment_id],
        "is_deleted": False,
    }
    assert_equal_with_expected_fixture(
        entity_universe["query_template"]["formatted_expression"],
        "tests/fixtures/offline_store_feature_table/item_id_to_item_type_universe.sql",
        update_fixture=update_fixtures,
    )


@pytest.mark.asyncio
async def test_latest_aggregation_features(
    app_container,
    document_service,
    periodic_task_service,
    deployed_latest_aggregation_features,
    latest_aggregation_with_window_deployment_id,
    latest_aggregation_without_window_deployment_id,
    expected_feast_registry_mapping,
    update_fixtures,
):
    """
    Test feature table creation with latest aggregation features
    """
    feature_tables = await get_all_feature_tables(document_service)
    assert set(feature_tables.keys()) == {"cat1_cust_id_30m", "cat1_cust_id_30m_1"}

    # Check latest aggregation with window
    feature_table = feature_tables["cat1_cust_id_30m"]
    feature_table_dict = feature_table.model_dump(
        by_alias=True, exclude={"created_at", "updated_at"}
    )
    feature_table_dict.pop("_id")
    _ = feature_table_dict.pop("feature_cluster")
    entity_universe = feature_table_dict.pop("entity_universe")
    assert_equal_with_expected_fixture(
        entity_universe["query_template"]["formatted_expression"],
        "tests/fixtures/offline_store_feature_table/latest_aggregation_with_window_universe.sql",
        update_fixtures,
    )
    assert feature_table_dict == {
        "aggregation_ids": ["latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2"],
        "base_name": "cust_id_30m",
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "deployment_ids": [latest_aggregation_with_window_deployment_id],
        "description": None,
        "entity_lookup_info": None,
        "feature_cluster_path": feature_table_dict["feature_cluster_path"],
        "feature_ids": [deployed_latest_aggregation_features[0].id],
        "feature_job_setting": {
            "blind_spot": "600s",
            "execution_buffer": "0s",
            "offset": "300s",
            "period": "1800s",
        },
        "feature_store_id": ObjectId("646f6c190ed28a5271fb02a1"),
        "has_ttl": True,
        "last_materialized_at": None,
        "name": "cat1_cust_id_30m",
        "name_prefix": "cat1",
        "name_suffix": None,
        "online_stores_last_materialized_at": [],
        "output_column_names": ["latest_event_timestamp_90d_V231227"],
        "output_dtypes": ["TIMESTAMP_TZ"],
        "precomputed_lookup_feature_table_info": None,
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
        "is_deleted": False,
    }
    assert await has_scheduled_task(periodic_task_service, feature_table)
    await check_feast_registry(
        app_container,
        deployment_id=latest_aggregation_with_window_deployment_id,
        expected_feast_registry_mapping=expected_feast_registry_mapping,
        enabled_deployment=True,
    )

    # Check latest aggregation without window
    feature_table = feature_tables["cat1_cust_id_30m_1"]
    feature_table_dict = feature_table.model_dump(
        by_alias=True, exclude={"created_at", "updated_at"}
    )
    feature_table_dict.pop("_id")
    _ = feature_table_dict.pop("feature_cluster")
    entity_universe = feature_table_dict.pop("entity_universe")
    assert_equal_with_expected_fixture(
        entity_universe["query_template"]["formatted_expression"],
        "tests/fixtures/offline_store_feature_table/latest_aggregation_without_window_universe.sql",
        update_fixtures,
    )
    assert feature_table_dict == {
        "aggregation_ids": ["latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2"],
        "base_name": "cust_id_30m",
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "deployment_ids": [latest_aggregation_without_window_deployment_id],
        "description": None,
        "entity_lookup_info": None,
        "feature_cluster_path": feature_table_dict["feature_cluster_path"],
        "feature_ids": [deployed_latest_aggregation_features[1].id],
        "feature_job_setting": {
            "blind_spot": "600s",
            "execution_buffer": "0s",
            "offset": "300s",
            "period": "1800s",
        },
        "feature_store_id": ObjectId("646f6c190ed28a5271fb02a1"),
        "has_ttl": False,
        "last_materialized_at": None,
        "name": "cat1_cust_id_30m_1",
        "name_prefix": "cat1",
        "name_suffix": "1",
        "online_stores_last_materialized_at": [],
        "output_column_names": ["latest_event_timestamp_V231227"],
        "output_dtypes": ["TIMESTAMP_TZ"],
        "precomputed_lookup_feature_table_info": None,
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
        "is_deleted": False,
    }
    assert await has_scheduled_task(periodic_task_service, feature_table)
    await check_feast_registry(
        app_container,
        deployment_id=latest_aggregation_without_window_deployment_id,
        expected_feast_registry_mapping=expected_feast_registry_mapping,
        enabled_deployment=True,
    )


@pytest.mark.asyncio
async def test_count_distinct_window_aggregate_feature(
    app_container,
    document_service,
    periodic_task_service,
    deployed_count_distinct_window_aggregate_feature,
    count_distinct_window_aggregate_deployment_id,
    expected_feast_registry_mapping,
    update_fixtures,
):
    """
    Test feature table creation with latest aggregation features
    """
    feature_tables = await get_all_feature_tables(document_service)
    assert set(feature_tables.keys()) == {"cat1_cust_id_30m"}

    # Check feature table
    feature_table = feature_tables["cat1_cust_id_30m"]
    feature_table_dict = feature_table.model_dump(
        by_alias=True, exclude={"created_at", "updated_at"}
    )
    feature_table_dict.pop("_id")
    _ = feature_table_dict.pop("feature_cluster")
    entity_universe = feature_table_dict.pop("entity_universe")
    assert_equal_with_expected_fixture(
        entity_universe["query_template"]["formatted_expression"],
        "tests/fixtures/offline_store_feature_table/count_distinct_window_aggregate_universe.sql",
        update_fixtures,
    )
    assert feature_table_dict == {
        "aggregation_ids": [],
        "base_name": "cust_id_30m",
        "block_modification_by": [],
        "catalog_id": ObjectId("646f6c1c0ed28a5271fb02db"),
        "deployment_ids": [count_distinct_window_aggregate_deployment_id],
        "description": None,
        "entity_lookup_info": None,
        "feature_cluster_path": feature_table_dict["feature_cluster_path"],
        "feature_ids": [deployed_count_distinct_window_aggregate_feature.id],
        "feature_job_setting": {
            "blind_spot": "600s",
            "period": "1800s",
            "offset": "300s",
            "execution_buffer": "0s",
        },
        "feature_store_id": ObjectId("646f6c190ed28a5271fb02a1"),
        "has_ttl": True,
        "last_materialized_at": None,
        "name": "cat1_cust_id_30m",
        "name_prefix": "cat1",
        "name_suffix": None,
        "online_stores_last_materialized_at": [],
        "output_column_names": ["col_int_distinct_count_48h_V231227"],
        "output_dtypes": ["INT"],
        "precomputed_lookup_feature_table_info": None,
        "primary_entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
        "serving_names": ["cust_id"],
        "user_id": ObjectId("63f9506dd478b94127123456"),
        "is_deleted": False,
    }
    assert await has_scheduled_task(periodic_task_service, feature_table)
    await check_feast_registry(
        app_container,
        deployment_id=count_distinct_window_aggregate_deployment_id,
        expected_feast_registry_mapping=expected_feast_registry_mapping,
        enabled_deployment=True,
    )
