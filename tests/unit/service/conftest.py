"""
Fixture for Service related unit tests
"""

from __future__ import annotations

import json
import os.path
from typing import Optional
from unittest.mock import Mock, patch

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte import Catalog
from featurebyte.common import DEFAULT_CATALOG_ID
from featurebyte.enum import SemanticType, SourceType
from featurebyte.models.online_store import OnlineStoreModel, RedisOnlineStoreDetails
from featurebyte.models.relationship import RelationshipType
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.entity_relationship_info import (
    EntityRelationshipInfo,
    FeatureEntityLookupInfo,
)
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.catalog import CatalogCreate, CatalogOnlineStoreUpdate
from featurebyte.schema.context import ContextCreate
from featurebyte.schema.dimension_table import DimensionTableCreate
from featurebyte.schema.entity import EntityCreate
from featurebyte.schema.event_table import EventTableCreate, EventTableServiceUpdate
from featurebyte.schema.feature import FeatureServiceCreate
from featurebyte.schema.feature_list import FeatureListServiceCreate
from featurebyte.schema.feature_namespace import FeatureNamespaceServiceUpdate
from featurebyte.schema.feature_store import FeatureStoreCreate
from featurebyte.schema.item_table import ItemTableCreate
from featurebyte.schema.relationship_info import RelationshipInfoCreate
from featurebyte.schema.scd_table import SCDTableCreate
from featurebyte.schema.target import TargetCreate
from featurebyte.service.catalog import CatalogService
from tests.util.helper import deploy_feature_ids, get_relationship_info, manage_document


@pytest.fixture(name="feature_store_service")
def feature_store_service_fixture(app_container):
    """FeatureStore service"""
    return app_container.feature_store_service


@pytest.fixture(name="feature_store_warehouse_service")
def feature_store_warehouse_service_fixture(
    app_container, feature_store, mock_snowflake_session, mock_get_feature_store_session
):
    """FeatureStore Warehouse service"""
    with patch("featurebyte.service.preview.PreviewService._get_feature_store_session") as mocked:
        mocked.return_value = feature_store, mock_snowflake_session
        mock_get_feature_store_session.return_value = mock_snowflake_session
        yield app_container.feature_store_warehouse_service


@pytest.fixture(name="entity_service")
def entity_service_fixture(app_container):
    """Entity service"""
    return app_container.entity_service


@pytest.fixture(name="session_validator_service")
def get_session_validator_service_fixture(app_container):
    """
    Fixture to get a session validator service
    """
    return app_container.session_validator_service


@pytest.fixture(name="semantic_service")
def semantic_service_fixture(app_container):
    """Semantic service"""
    return app_container.semantic_service


@pytest.fixture(name="info_service")
def info_service_fixture(app_container):
    """InfoService fixture"""
    return app_container.info_service


@pytest.fixture(name="view_construction_service")
def view_construction_service_fixture(app_container):
    """View construction service"""
    return app_container.view_construction_service


@pytest.fixture(name="context_service")
def context_service_fixture(app_container):
    """Context service"""
    return app_container.context_service


@pytest.fixture(name="table_service")
def table_service_fixture(app_container):
    """Table service"""
    return app_container.table_service


@pytest.fixture(name="event_table_service")
def event_table_service_fixture(app_container):
    """EventTable service"""
    return app_container.event_table_service


@pytest.fixture(name="item_table_service")
def item_table_service_fixture(app_container):
    """ItemTable service"""
    return app_container.item_table_service


@pytest.fixture(name="dimension_table_service")
def dimension_table_service_fixture(app_container):
    """DimensionTable service"""
    return app_container.dimension_table_service


@pytest.fixture(name="scd_table_service")
def scd_table_service_fixture(app_container):
    """SCDTable service"""
    return app_container.scd_table_service


@pytest.fixture(name="table_facade_service")
def table_facade_service_fixture(app_container):
    """TableFacade service"""
    return app_container.table_facade_service


@pytest.fixture(name="feature_namespace_service")
def feature_namespace_service_fixture(app_container):
    """FeatureNamespaceService fixture"""
    return app_container.feature_namespace_service


@pytest.fixture(name="feature_service")
def feature_service_fixture(app_container):
    """FeatureService fixture"""
    return app_container.feature_service


@pytest.fixture(name="feature_list_namespace_service")
def feature_list_namespace_service_fixture(app_container):
    """FeatureListNamespaceService fixture"""
    return app_container.feature_list_namespace_service


@pytest.fixture(name="feature_list_service")
def feature_list_service_fixture(app_container):
    """FeatureListService fixture"""
    return app_container.feature_list_service


@pytest.fixture(name="feature_table_cache_metadata_service")
def feature_table_cache_metadata_service_fixture(app_container):
    """FeatureTableCacheMetadataService fixture"""
    return app_container.feature_table_cache_metadata_service


@pytest.fixture(name="feature_table_cache_service")
def feature_table_cache_service_fixture(app_container):
    """FeatureTableCacheService fixture"""
    return app_container.feature_table_cache_service


@pytest.fixture(name="table_columns_info_service")
def table_columns_info_service_fixture(app_container):
    """TableColumnsInfoService fixture"""
    return app_container.table_columns_info_service


@pytest.fixture(name="table_status_service")
def table_status_service_fixture(app_container):
    """TableStatusService fixture"""
    return app_container.table_status_service


@pytest.fixture(name="feature_readiness_service")
def feature_readiness_service_fixture(app_container):
    """FeatureReadinessService fixture"""
    return app_container.feature_readiness_service


@pytest.fixture(name="default_version_mode_service")
def default_version_mode_service_fixture(app_container):
    """DefaultVersionModeService fixture"""
    return app_container.default_version_mode_service


@pytest.fixture(name="target_service")
def target_service_fixture(app_container):
    """TargetService fixture"""
    return app_container.target_service


@pytest.fixture(name="online_enable_service")
def online_enable_service_fixture(app_container):
    """OnlineEnableService fixture"""
    return app_container.online_enable_service


@pytest.fixture(name="mock_get_feature_store_session")
def mock_get_feature_store_session_fixture():
    """Mock get_feature_store_session fixture"""
    with patch(
        "featurebyte.service.online_enable.SessionManagerService.get_feature_store_session"
    ) as mock_get_feature_store_session:
        yield mock_get_feature_store_session


@pytest.fixture(name="preview_service")
def preview_service_fixture(
    app_container, feature_store, mock_snowflake_session, mock_get_feature_store_session
):
    """PreviewService fixture"""
    with patch("featurebyte.service.preview.PreviewService._get_feature_store_session") as mocked:
        mocked.return_value = feature_store, mock_snowflake_session
        mock_get_feature_store_session.return_value = mock_snowflake_session
        yield app_container.preview_service


@pytest.fixture(name="feature_preview_service")
def feature_preview_service_fixture(app_container):
    """FeaturePreviewService fixture"""
    return app_container.feature_preview_service


@pytest.fixture(name="historical_features_service")
def historical_features_service_fixture(app_container):
    """HistoricalFeaturesService fixture"""
    return app_container.historical_features_service


@pytest.fixture(name="tile_cache_service")
def tile_cache_service_fixture(app_container):
    """TileCacheService fixture"""
    return app_container.tile_cache_service


@pytest.fixture(name="warehouse_table_service")
def warehouse_table_service_fixture(app_container):
    """WarehouseTableService fixture"""
    return app_container.warehouse_table_service


@pytest.fixture(name="cron_helper")
def cron_helper_fixture(app_container):
    """CronHelper fixture"""
    return app_container.cron_helper


@pytest.fixture(name="column_statistics_service")
def column_statistics_service_fixture(app_container):
    """ColumnStatisticsService fixture"""
    return app_container.column_statistics_service


@pytest.fixture(name="tile_job_log_service")
def tile_job_log_service(app_container):
    """
    TileJobLogService fixture
    """
    return app_container.tile_job_log_service


@pytest.fixture(name="tile_task_executor")
def tile_task_executor_fixture(app_container):
    """
    TileTaskExecutor fixture
    """
    return app_container.tile_task_executor


@pytest.fixture(name="tile_registry_service")
def tile_registry_service_fixture(app_container):
    """
    TileRegistryService fixture
    """
    return app_container.tile_registry_service


@pytest.fixture(name="relationship_info_service")
def relationship_info_service_fixture(app_container):
    """
    RelationshipInfoService fixture
    """
    return app_container.relationship_info_service


@pytest.fixture(name="online_enable_service_data_warehouse_mocks")
def online_enable_service_data_warehouse_mocks_fixture():
    """
    Patches required to bypass actual data warehouse updates and allow inspecting expected calls to
    FeatureManager
    """
    mocks = {}
    with patch(
        "featurebyte.service.online_enable.SessionManagerService.get_feature_store_session"
    ) as mock_get_feature_store_session:
        with patch(
            "featurebyte.service.online_enable.FeatureManagerService.online_enable",
        ) as online_enable:
            mock_get_feature_store_session.return_value = Mock(source_type=SourceType.SNOWFLAKE)
            mocks["get_feature_store_session"] = mock_get_feature_store_session
            mocks["feature_manager_online_enable"] = online_enable
            yield mocks


@pytest.fixture(name="online_serving_service")
def online_serving_service_fixture(app_container):
    """OnlineEnableService fixture"""
    return app_container.online_serving_service


@pytest.fixture(name="online_store_table_version_service")
def online_store_table_version_service_fixture(app_container):
    """OnlineStoreTableVersionService fixture"""
    return app_container.online_store_table_version_service


@pytest.fixture(name="online_store_cleanup_scheduler_service")
def online_store_cleanup_scheduler_service_fixture(app_container):
    """OnlineStoreCleanupSchedulerService fixture"""
    return app_container.online_store_cleanup_scheduler_service


@pytest.fixture(name="periodic_task_service")
def periodic_task_service_fixture(app_container):
    """PeriodicTaskService fixture"""
    return app_container.periodic_task_service


@pytest.fixture(name="version_service")
def version_service_fixture(app_container):
    """VersionService fixture"""
    return app_container.version_service


@pytest.fixture(name="deploy_service")
def deploy_service_fixture(app_container):
    """DeployService fixture"""
    return app_container.deploy_service


@pytest.fixture(name="entity_validation_service")
def entity_validation_service_fixture(app_container):
    """EntityValidationService fixture"""
    return app_container.entity_validation_service


@pytest.fixture(name="parent_entity_lookup_service")
def parent_entity_lookup_service(app_container):
    """ParentEntityLookupService fixture"""
    return app_container.parent_entity_lookup_service


@pytest.fixture(name="feature_list_status_service")
def feature_list_status_service_fixture(app_container):
    """Feature list status service fixture"""
    return app_container.feature_list_status_service


@pytest.fixture(name="observation_table_service")
def observation_table_service(app_container):
    """ObservationTableService fixture"""
    return app_container.observation_table_service


@pytest.fixture(name="static_source_table_service")
def static_source_table_service(app_container):
    """StaticSourceTableService fixture"""
    return app_container.static_source_table_service


@pytest_asyncio.fixture(name="feature_store")
async def feature_store_fixture(test_dir, feature_store_service):
    """FeatureStore model"""
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_store.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature_store = await feature_store_service.create_document(
            data=FeatureStoreCreate(**payload)
        )
        return feature_store


@pytest_asyncio.fixture(name="catalog")
async def catalog_fixture(test_dir, user, persistent, storage):
    """Catalog model"""
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/catalog.json")
    # Cannot inject catalog service here because the app_container fixture depends on catalog, which creates a cycle.
    catalog_service = CatalogService(
        user,
        persistent,
        catalog_id=DEFAULT_CATALOG_ID,
        block_modification_handler=BlockModificationHandler(),
        storage=storage,
        redis=Mock(),
    )
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        catalog = await catalog_service.create_document(data=CatalogCreate(**payload))
        Catalog.activate(catalog.name)
        return catalog


@pytest_asyncio.fixture(name="entity")
async def entity_fixture(test_dir, entity_service):
    """Entity model"""
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/entity.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        entity = await entity_service.create_document(data=EntityCreate(**payload))
        return entity


@pytest_asyncio.fixture(name="entity_transaction")
async def entity_transaction_fixture(test_dir, entity_service):
    """Entity model"""
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/entity_transaction.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        entity = await entity_service.create_document(data=EntityCreate(**payload))
        return entity


@pytest_asyncio.fixture(name="context")
async def context_fixture(test_dir, context_service, feature_store, entity, entity_transaction):
    """Context model"""
    _ = feature_store, entity, entity_transaction
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/context.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        context = await context_service.create_document(data=ContextCreate(**payload))
        return context


@pytest.fixture(name="event_table_factory")
def event_table_factory_fixture(test_dir, feature_store, event_table_service, semantic_service):
    """
    EventTable factory returns a function that can be used to create EventTable models.
    """
    _ = feature_store

    async def factory(
        remove_feature_job_setting: Optional[bool] = False,
    ):
        fixture_path = os.path.join(test_dir, "fixtures/request_payloads/event_table.json")
        with open(fixture_path, encoding="utf") as fhandle:
            payload = json.loads(fhandle.read())
            payload["tabular_source"]["table_details"]["table_name"] = "sf_event_table"
            if remove_feature_job_setting:
                payload["default_feature_job_setting"] = None
            event_table = await event_table_service.create_document(
                data=EventTableCreate(**payload)
            )
            event_timestamp_sem = await semantic_service.get_or_create_document(
                name=SemanticType.EVENT_TIMESTAMP.value
            )
            columns_info = []
            for col in event_table.columns_info:
                col_dict = col.model_dump()
                if col.name == "event_timestamp":
                    col_dict["semantic_id"] = event_timestamp_sem.id
                columns_info.append(col_dict)

            event_table = await event_table_service.update_document(
                document_id=event_table.id,
                data=EventTableServiceUpdate(columns_info=columns_info),
                return_document=True,
            )
            return event_table

    return factory


@pytest_asyncio.fixture(name="event_table")
async def event_table_fixture(event_table_factory):
    """EventTable model"""
    return await event_table_factory()


@pytest_asyncio.fixture(name="item_table")
async def item_table_fixture(test_dir, feature_store, item_table_service, event_table):
    """ItemTable model"""
    _, _ = feature_store, event_table
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/item_table.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        payload["tabular_source"]["table_details"]["table_name"] = "sf_item_table"
        item_table = await item_table_service.create_document(data=ItemTableCreate(**payload))
        return item_table


@pytest_asyncio.fixture(name="dimension_table")
async def dimension_table_fixture(test_dir, feature_store, dimension_table_service):
    """DimensionTable model"""
    _ = feature_store
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/dimension_table.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        payload["tabular_source"]["table_details"]["table_name"] = "sf_dimension_table"
        dimension_table = await dimension_table_service.create_document(
            data=DimensionTableCreate(**payload)
        )
        return dimension_table


@pytest_asyncio.fixture(name="scd_table")
async def scd_table_fixture(test_dir, feature_store, scd_table_service):
    """SCDTable model"""
    _ = feature_store
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/scd_table.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        payload["tabular_source"]["table_details"]["table_name"] = "sf_scd_table"
        scd_table = await scd_table_service.create_document(data=SCDTableCreate(**payload))
        return scd_table


@pytest.fixture(name="feature_factory")
def feature_factory_fixture(test_dir, feature_service, entity):
    """
    Feature factory

    Note that this will only create the feature, and might throw an error if used alone. You'll need to make sure that
    you've created the event table and entity models first.
    """
    _ = entity

    async def factory():
        fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_sum_30m.json")
        with open(fixture_path, encoding="utf") as fhandle:
            payload = json.loads(fhandle.read())
            feature = await feature_service.create_document(data=FeatureServiceCreate(**payload))
            return feature

    return factory


@pytest_asyncio.fixture(name="feature")
async def feature_fixture(event_table, entity, feature_factory):
    """Feature model"""
    _ = event_table, entity
    return await feature_factory()


@pytest_asyncio.fixture(name="feature_iet")
async def feature_iet_fixture(test_dir, event_table, entity, feature_service):
    """Feature model (IET feature)"""
    _ = event_table, entity
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_iet.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature = await feature_service.create_document(data=FeatureServiceCreate(**payload))
        return feature


@pytest_asyncio.fixture(name="feature_non_time_based")
async def feature_non_time_based_fixture(
    test_dir, event_table, item_table, entity_transaction, feature_service
):
    """Feature model (non-time-based feature)"""
    _ = event_table, item_table, entity_transaction
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_item_event.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature = await feature_service.create_document(data=FeatureServiceCreate(**payload))
        return feature


@pytest_asyncio.fixture(name="feature_item_event")
async def feature_item_event_fixture(
    test_dir, event_table, item_table, entity_transaction, feature_service
):
    """Feature model (item-event feature)"""
    _ = event_table, item_table, entity_transaction
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_item_event.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature = await feature_service.create_document(data=FeatureServiceCreate(**payload))
        return feature


@pytest_asyncio.fixture(name="production_ready_feature")
async def production_ready_feature_fixture(feature_readiness_service, feature):
    """Production ready readiness feature fixture"""
    prod_feat = await feature_readiness_service.update_feature(
        feature_id=feature.id, readiness="PRODUCTION_READY", ignore_guardrails=True
    )
    assert prod_feat.readiness == "PRODUCTION_READY"
    return prod_feat


@pytest_asyncio.fixture(name="feature_namespace")
async def feature_namespace_fixture(feature_namespace_service, feature):
    """FeatureNamespace fixture"""
    return await feature_namespace_service.get_document(document_id=feature.feature_namespace_id)


@pytest_asyncio.fixture(name="feature_list")
async def feature_list_fixture(test_dir, feature, feature_list_service, storage):
    """Feature list model"""
    _ = feature
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_list_single.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        async with manage_document(
            feature_list_service,
            FeatureListServiceCreate(**payload),
            storage,
        ) as feature_list:
            yield feature_list


@pytest_asyncio.fixture(name="feature_list_repeated")
async def feature_list_repeated_fixture(test_dir, feature, feature_list_service, storage):
    """Feature list model that has the same underlying features as feature_list"""
    _ = feature
    fixture_path = os.path.join(
        test_dir, "fixtures/request_payloads/feature_list_single_repeated.json"
    )
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        async with manage_document(
            feature_list_service, FeatureListServiceCreate(**payload), storage
        ) as feature_list:
            yield feature_list


@pytest_asyncio.fixture(name="feature_list_namespace")
async def feature_list_namespace_fixture(feature_list_namespace_service, feature_list):
    """FeatureListNamespace fixture"""
    return await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )


@pytest_asyncio.fixture(name="production_ready_feature_list")
async def production_ready_feature_list_fixture(
    production_ready_feature, feature_list_service, storage
):
    """Fixture for a production ready feature list"""
    data = FeatureListServiceCreate(
        name="Production Ready Feature List",
        feature_ids=[production_ready_feature.id],
    )
    async with manage_document(feature_list_service, data, storage) as feature_list:
        yield feature_list


@pytest_asyncio.fixture(name="deployed_feature_list")
async def deployed_feature_list_fixture(
    online_enable_service_data_warehouse_mocks,
    deploy_service,
    feature_list_service,
    production_ready_feature_list,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """Fixture for a deployed feature list"""
    _ = (
        online_enable_service_data_warehouse_mocks,
        mock_update_data_warehouse,
        mock_offline_store_feature_manager_dependencies,
    )
    await deploy_service.create_deployment(
        feature_list_id=production_ready_feature_list.id,
        deployment_id=ObjectId(),
        deployment_name="test-deployment",
        to_enable_deployment=True,
    )
    updated_feature_list = await feature_list_service.get_document(
        document_id=production_ready_feature_list.id
    )
    return updated_feature_list


async def insert_feature_into_persistent(
    test_dir,
    user,
    persistent,
    readiness,
    name=None,
    catalog_id=None,
    version=None,
    namespace_id=None,
):
    """Insert a feature into persistent"""
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_sum_30m.json")
    with open(fixture_path) as fhandle:
        payload = json.loads(fhandle.read())
        payload = FeatureServiceCreate(**payload, user_id=user.id).model_dump(by_alias=True)
        payload["_id"] = ObjectId()
        payload["readiness"] = readiness
        payload["catalog_id"] = catalog_id or DEFAULT_CATALOG_ID
        if name:
            payload["name"] = name
        if namespace_id:
            payload["feature_namespace_id"] = namespace_id
        if version:
            payload["version"] = version
        feature_id = await persistent.insert_one(
            collection_name="feature",
            document=payload,
            user_id=user.id,
        )
        return feature_id


@pytest_asyncio.fixture(name="target")
async def target_fixture(test_dir, target_service, event_table, item_table, entity):
    """Target model"""
    _ = event_table, item_table, entity
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/target.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        return await target_service.create_document(data=TargetCreate(**payload))


@pytest_asyncio.fixture(name="setup_for_feature_readiness")
async def setup_for_feature_readiness_fixture(
    test_dir,
    feature_list_service,
    feature_list_namespace_service,
    feature_namespace_service,
    feature,
    feature_list,
    user,
    persistent,
    storage,
):
    """Setup for feature readiness test fixture"""
    namespace = await feature_namespace_service.get_document(
        document_id=feature.feature_namespace_id
    )
    assert namespace.default_version_mode == "AUTO"
    assert namespace.default_feature_id == feature.id
    assert namespace.feature_ids == [feature.id]

    # add a deprecated feature version first
    new_feature_id = await insert_feature_into_persistent(
        test_dir=test_dir,
        user=user,
        persistent=persistent,
        readiness="DEPRECATED",
        namespace_id=feature.feature_namespace_id,
        catalog_id=feature.catalog_id,
        version="V0",
    )
    feat_namespace = await feature_namespace_service.update_document(
        document_id=feature.feature_namespace_id,
        data=FeatureNamespaceServiceUpdate(feature_ids=namespace.feature_ids + [new_feature_id]),
    )
    assert len(feat_namespace.feature_ids) == 2
    assert new_feature_id in feat_namespace.feature_ids
    assert feat_namespace.default_feature_id == feature.id
    assert feat_namespace.default_version_mode == "AUTO"
    assert feat_namespace.readiness == "DRAFT"

    # create another feature list version
    feature_list_data = FeatureListServiceCreate(
        feature_ids=[new_feature_id],
        feature_list_namespace_id=feature_list.feature_list_namespace_id,
        name="sf_feature_list",
        version={"name": "V220914"},
    )
    async with manage_document(feature_list_service, feature_list_data, storage) as new_flist:
        flist_namespace = await feature_list_namespace_service.get_document(
            document_id=feature_list.feature_list_namespace_id
        )
        assert len(flist_namespace.feature_list_ids) == 2
        assert new_flist.id in flist_namespace.feature_list_ids
        assert new_flist.feature_list_namespace_id == feature_list.feature_list_namespace_id
        assert flist_namespace.default_feature_list_id == feature_list.id
        yield new_feature_id, new_flist.id


async def create_event_table_with_entities(
    data_name, test_dir, event_table_service, columns, table_facade_service
):
    """Helper function to create an EventTable with provided columns and entities"""

    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/event_table.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())

    def _update_columns_info(columns_info):
        new_columns_info = []
        keep_cols = ["event_timestamp", "created_at", "col_int"]
        for col_info in columns_info:
            if col_info["name"] in keep_cols:
                col_info["entity_id"] = None  # strip entity id from the original payload
                new_columns_info.append(col_info)
        for col_name, col_entity_id in columns:
            col_info = {"name": col_name, "entity_id": col_entity_id, "dtype": "INT"}
            new_columns_info.append(col_info)

        output = [ColumnInfo(**col_info) for col_info in new_columns_info]
        return output

    payload.pop("_id")
    payload["tabular_source"]["table_details"]["table_name"] = data_name
    payload["name"] = data_name

    event_table = await event_table_service.create_document(data=EventTableCreate(**payload))
    columns_info = _update_columns_info(payload["columns_info"])
    await table_facade_service.update_table_columns_info(
        table_id=event_table.id,
        columns_info=columns_info,
        service=event_table_service,
    )
    event_table = await event_table_service.get_document(document_id=event_table.id)
    return event_table


@pytest_asyncio.fixture(name="entity_a")
async def entity_a_fixture(entity_service):
    """
    An entity A
    """
    entity_a = await entity_service.create_document(EntityCreate(name="entity_a", serving_name="A"))
    return entity_a


@pytest_asyncio.fixture(name="entity_b")
async def entity_b_fixture(entity_service):
    """
    An entity B
    """
    entity_b = await entity_service.create_document(EntityCreate(name="entity_b", serving_name="B"))
    return entity_b


@pytest_asyncio.fixture(name="entity_c")
async def entity_c_fixture(entity_service):
    """
    An entity C
    """
    entity_c = await entity_service.create_document(EntityCreate(name="entity_c", serving_name="C"))
    return entity_c


@pytest_asyncio.fixture(name="entity_d")
async def entity_d_fixture(entity_service):
    """
    An entity D
    """
    entity_d = await entity_service.create_document(EntityCreate(name="entity_d", serving_name="D"))
    return entity_d


@pytest_asyncio.fixture(name="entity_e")
async def entity_e_fixture(entity_service):
    """
    An entity E
    """
    entity_e = await entity_service.create_document(EntityCreate(name="entity_e", serving_name="E"))
    return entity_e


async def create_table_and_add_parent(
    test_dir,
    event_table_service,
    table_facade_service,
    relationship_info_service,
    child_entity,
    parent_entity,
    child_column,
    parent_column,
):
    """
    Helper function to create table and add a relationship between two entities
    """
    table = await create_event_table_with_entities(
        f"{parent_entity.name}_is_parent_of_{child_entity.name}_data",
        test_dir,
        event_table_service,
        [(child_column, child_entity.id), (parent_column, parent_entity.id)],
        table_facade_service,
    )
    relationship_info = await relationship_info_service.create_document(
        data=RelationshipInfoCreate(
            name=f"{child_entity.id}_{parent_entity.id}",
            relationship_type=RelationshipType.CHILD_PARENT,
            entity_id=child_entity.id,
            related_entity_id=parent_entity.id,
            relation_table_id=table.id,
            enabled=True,
        )
    )
    return table, relationship_info


@pytest_asyncio.fixture(name="b_is_parent_of_a")
async def b_is_parent_of_a_fixture(
    entity_a,
    entity_b,
    relationship_info_service,
    event_table_service,
    table_facade_service,
    test_dir,
    feature_store,
):
    """
    Fixture to make B a parent of A
    """
    _ = feature_store
    return await create_table_and_add_parent(
        test_dir,
        event_table_service,
        table_facade_service,
        relationship_info_service,
        child_entity=entity_a,
        parent_entity=entity_b,
        child_column="a",
        parent_column="b",
    )


@pytest_asyncio.fixture(name="c_is_parent_of_b")
async def c_is_parent_of_b_fixture(
    entity_b,
    entity_c,
    relationship_info_service,
    event_table_service,
    table_facade_service,
    test_dir,
    feature_store,
):
    """
    Fixture to make C a parent of B
    """
    _ = feature_store
    return await create_table_and_add_parent(
        test_dir,
        event_table_service,
        table_facade_service,
        relationship_info_service,
        child_entity=entity_b,
        parent_entity=entity_c,
        child_column="b",
        parent_column="c",
    )


@pytest_asyncio.fixture(name="d_is_parent_of_b")
async def d_is_parent_of_b_fixture(
    entity_b,
    entity_d,
    relationship_info_service,
    event_table_service,
    table_facade_service,
    test_dir,
    feature_store,
):
    """
    Fixture to make D a parent of B
    """
    _ = feature_store
    return await create_table_and_add_parent(
        test_dir,
        event_table_service,
        table_facade_service,
        relationship_info_service,
        child_entity=entity_b,
        parent_entity=entity_d,
        child_column="b",
        parent_column="d",
    )


@pytest_asyncio.fixture(name="d_is_parent_of_c")
async def d_is_parent_of_c_fixture(
    entity_c,
    entity_d,
    relationship_info_service,
    event_table_service,
    table_facade_service,
    test_dir,
    feature_store,
):
    """
    Fixture to make D a parent of C
    """
    _ = feature_store
    return await create_table_and_add_parent(
        test_dir,
        event_table_service,
        table_facade_service,
        relationship_info_service,
        child_entity=entity_c,
        parent_entity=entity_d,
        child_column="c",
        parent_column="d",
    )


@pytest_asyncio.fixture(name="a_is_parent_of_c_and_d")
async def a_is_parent_of_c_and_d_fixture(
    entity_a,
    entity_c,
    entity_d,
    relationship_info_service,
    event_table_service,
    table_facade_service,
    test_dir,
    feature_store,
):
    """
    Fixture to make A a parent of C and D
    """
    _ = feature_store
    await create_table_and_add_parent(
        test_dir,
        event_table_service,
        table_facade_service,
        relationship_info_service,
        child_entity=entity_c,
        parent_entity=entity_a,
        child_column="c",
        parent_column="a",
    )
    await create_table_and_add_parent(
        test_dir,
        event_table_service,
        table_facade_service,
        relationship_info_service,
        child_entity=entity_d,
        parent_entity=entity_a,
        child_column="d",
        parent_column="a",
    )


@pytest.fixture(name="feature_materialize_service")
def feature_materialize_service_fixture(app_container):
    """
    Fixture for FeatureMaterializeService
    """
    return app_container.feature_materialize_service


@pytest_asyncio.fixture(name="online_store")
async def online_store_fixture(app_container):
    """
    Fixture to return an online store model
    """
    online_store_model = await app_container.online_store_service.create_document(
        OnlineStoreModel(
            name="redis_online_store",
            details=RedisOnlineStoreDetails(
                redis_type="redis",
                connection_string="localhost:36379",
            ),
        )
    )
    return online_store_model


@pytest_asyncio.fixture(name="catalog_with_online_store")
async def catalog_with_online_store_fixture(app_container, catalog, online_store):
    """
    Fixture for a catalog with an online store
    """
    catalog_update = CatalogOnlineStoreUpdate(online_store_id=online_store.id)
    catalog = await app_container.catalog_service.update_document(
        document_id=catalog.id, data=catalog_update
    )
    return catalog


@pytest.fixture(name="fl_requiring_parent_serving_deployment_id")
def fl_requiring_parent_serving_deployment_id_fixture():
    """
    Fixture for a deployment id for a feature list that requires parent serving
    """
    return ObjectId()


@pytest.fixture(name="is_online_store_registered_for_catalog")
def is_online_store_registered_for_catalog_fixture():
    """
    Fixture to determine if catalog is configured with an online store
    """
    return True


@pytest_asyncio.fixture(name="deployed_feature_list_requiring_parent_serving")
async def deployed_feature_list_requiring_parent_serving_fixture(
    app_container,
    float_feature,
    aggregate_asat_feature,
    cust_id_entity,
    gender_entity,
    fl_requiring_parent_serving_deployment_id,
    mock_offline_store_feature_manager_dependencies,
    mock_update_data_warehouse,
    is_online_store_registered_for_catalog,
    online_store,
):
    """
    Fixture a deployed feature list that require serving parent features

    float_feature: customer entity feature (ttl)
    aggregate_asat_feature: gender entity feature (non-ttl)

    Gender is a parent of customer.

    Primary entity of the combined feature is customer.

    Combined feature requires serving two feature tables (one customer, one gender) using customer
    as the serving entity.
    """
    _ = mock_offline_store_feature_manager_dependencies
    _ = mock_update_data_warehouse

    new_feature = float_feature + aggregate_asat_feature
    new_feature.name = "feature_requiring_parent_serving"
    new_feature.save()

    new_feature_2 = new_feature + 123
    new_feature_2.name = new_feature.name + "_plus_123"
    new_feature_2.save()

    if is_online_store_registered_for_catalog:
        catalog_update = CatalogOnlineStoreUpdate(online_store_id=online_store.id)
        await app_container.catalog_service.update_document(
            document_id=app_container.catalog_id, data=catalog_update
        )

    feature_list = await deploy_feature_ids(
        app_container,
        feature_list_name="fl_requiring_parent_serving",
        feature_ids=[
            new_feature.id,
            new_feature_2.id,
        ],
        deployment_id=fl_requiring_parent_serving_deployment_id,
    )

    expected_relationship_info = await get_relationship_info(
        app_container,
        child_entity_id=cust_id_entity.id,
        parent_entity_id=gender_entity.id,
    )
    assert feature_list.features_entity_lookup_info == [
        FeatureEntityLookupInfo(
            feature_id=new_feature.id,
            feature_list_to_feature_primary_entity_join_steps=[],
            feature_internal_entity_join_steps=[
                EntityRelationshipInfo(**expected_relationship_info.model_dump(by_alias=True))
            ],
        ),
        FeatureEntityLookupInfo(
            feature_id=new_feature_2.id,
            feature_list_to_feature_primary_entity_join_steps=[],
            feature_internal_entity_join_steps=[
                EntityRelationshipInfo(**expected_relationship_info.model_dump(by_alias=True))
            ],
        ),
    ]

    return feature_list


@pytest_asyncio.fixture(name="deployed_feature_list_requiring_parent_serving_ttl")
async def deployed_feature_list_requiring_parent_serving_ttl_fixture(
    app_container,
    float_feature,
    non_time_based_feature,
    cust_id_entity,
    transaction_entity,
    fl_requiring_parent_serving_deployment_id,
    mock_offline_store_feature_manager_dependencies,
    mock_update_data_warehouse,
):
    """
    Fixture a deployed feature list that require serving parent features with ttl

    float_feature: customer entity feature (ttl)
    non_time_based_feature: transaction entity feature (non-ttl)

    Customer is a parent of transaction.

    Primary entity of the combined feature is transaction.

    Combined feature requires serving two feature tables (one transaction, one customer) using
    transaction as the serving entity.
    """
    _ = mock_offline_store_feature_manager_dependencies
    _ = mock_update_data_warehouse

    new_feature = float_feature + non_time_based_feature
    new_feature.name = "feature_requiring_parent_serving_ttl"
    new_feature.save()

    feature_list = await deploy_feature_ids(
        app_container,
        feature_list_name="fl_requiring_parent_serving_ttl",
        feature_ids=[
            new_feature.id,
        ],
        deployment_id=fl_requiring_parent_serving_deployment_id,
    )

    expected_relationship_info = await get_relationship_info(
        app_container,
        child_entity_id=transaction_entity.id,
        parent_entity_id=cust_id_entity.id,
    )
    assert feature_list.features_entity_lookup_info == [
        FeatureEntityLookupInfo(
            feature_id=new_feature.id,
            feature_list_to_feature_primary_entity_join_steps=[],
            feature_internal_entity_join_steps=[
                EntityRelationshipInfo(**expected_relationship_info.model_dump(by_alias=True))
            ],
        ),
    ]

    return feature_list


@pytest_asyncio.fixture(name="deployed_feature_list_requiring_parent_serving_composite_entity")
async def deployed_feature_list_requiring_parent_serving_composite_entity_fixture(
    app_container,
    descendant_of_gender_feature,
    aggregate_asat_composite_entity_feature,
    group_entity,
    gender_entity,
    another_entity,
    fl_requiring_parent_serving_deployment_id,
    mock_offline_store_feature_manager_dependencies,
    mock_update_data_warehouse,
    is_online_store_registered_for_catalog,
    online_store,
):
    """
    Fixture a deployed feature list that require serving parent features with composite entities

    descendant_of_gender_feature: group entity feature (non-ttl)
    aggregate_asat_composite_entity_feature: gender X another_entity feature (non-ttl)

    Gender is a parent of Group.

    Primary entity of the combined feature is group X another_entity.

    Combined feature requires serving two feature tables (1. group entity, 2. gender entity X
    another_entity via group X another_entity) using group entity X another_entity as the serving
    entity.
    """
    _ = mock_offline_store_feature_manager_dependencies
    _ = mock_update_data_warehouse

    new_feature = descendant_of_gender_feature + aggregate_asat_composite_entity_feature
    new_feature.name = "feature_requiring_parent_serving"
    new_feature.save()

    if is_online_store_registered_for_catalog:
        catalog_update = CatalogOnlineStoreUpdate(online_store_id=online_store.id)
        await app_container.catalog_service.update_document(
            document_id=app_container.catalog_id, data=catalog_update
        )

    feature_list = await deploy_feature_ids(
        app_container,
        feature_list_name="fl_requiring_parent_serving",
        feature_ids=[new_feature.id],
        deployment_id=fl_requiring_parent_serving_deployment_id,
    )
    assert feature_list.primary_entity_ids == [another_entity.id, group_entity.id]
    expected_relationship_info = await get_relationship_info(
        app_container,
        child_entity_id=group_entity.id,
        parent_entity_id=gender_entity.id,
    )
    assert feature_list.features_entity_lookup_info == [
        FeatureEntityLookupInfo(
            feature_id=new_feature.id,
            feature_list_to_feature_primary_entity_join_steps=[],
            feature_internal_entity_join_steps=[
                EntityRelationshipInfo(**expected_relationship_info.model_dump(by_alias=True))
            ],
        ),
    ]
    return feature_list
