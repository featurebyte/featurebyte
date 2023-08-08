"""
Fixture for Service related unit tests
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from typing import Optional

import json
import os.path
from unittest.mock import Mock, patch

import pytest
import pytest_asyncio
from bson.objectid import ObjectId

from featurebyte import Catalog
from featurebyte.enum import SemanticType, SourceType
from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.models.entity import ParentEntity
from featurebyte.models.entity_validation import EntityInfo
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.registry import app_container_config
from featurebyte.schema.catalog import CatalogCreate
from featurebyte.schema.context import ContextCreate
from featurebyte.schema.dimension_table import DimensionTableCreate
from featurebyte.schema.entity import EntityCreate, EntityServiceUpdate
from featurebyte.schema.event_table import EventTableCreate, EventTableServiceUpdate
from featurebyte.schema.feature import FeatureServiceCreate
from featurebyte.schema.feature_list import FeatureListServiceCreate
from featurebyte.schema.feature_namespace import FeatureNamespaceServiceUpdate
from featurebyte.schema.feature_store import FeatureStoreCreate
from featurebyte.schema.item_table import ItemTableCreate
from featurebyte.schema.scd_table import SCDTableCreate
from featurebyte.schema.target import TargetCreate
from featurebyte.service.catalog import CatalogService
from featurebyte.storage import LocalTempStorage
from featurebyte.worker import get_celery, get_redis


@pytest.fixture(name="get_credential")
def get_credential_fixture(credentials):
    """
    get_credential fixture
    """

    async def get_credential(user_id, feature_store_name):
        _ = user_id
        return credentials.get(feature_store_name)

    return get_credential


@pytest.fixture(name="app_container")
def app_container_fixture(persistent, user, catalog):
    """
    Return an app container used in tests. This will allow us to easily retrieve instances of the right type.
    """
    return LazyAppContainer(
        user=user,
        persistent=persistent,
        temp_storage=LocalTempStorage(),
        celery=get_celery(),
        redis=get_redis(),
        storage=LocalTempStorage(),
        catalog_id=catalog.id,
        app_container_config=app_container_config,
    )


@pytest.fixture(name="feature_store_service")
def feature_store_service_fixture(app_container):
    """FeatureStore service"""
    return app_container.feature_store_service


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


@pytest.fixture(name="preview_service")
def preview_service_fixture(app_container):
    """PreviewService fixture"""
    return app_container.preview_service


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
async def catalog_fixture(test_dir, user, persistent):
    """Catalog model"""
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/catalog.json")
    catalog_service = CatalogService(user, persistent, catalog_id=DEFAULT_CATALOG_ID)
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
async def context_fixture(test_dir, context_service, feature_store, entity):
    """Context model"""
    _ = feature_store, entity
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
            event_timestamp = await semantic_service.get_or_create_document(
                name=SemanticType.EVENT_TIMESTAMP
            )
            columns_info = []
            for col in event_table.columns_info:
                col_dict = col.dict()
                if col.name == "event_timestamp":
                    col_dict["semantic_id"] = event_timestamp.id
                columns_info.append(col_dict)

            event_table = await event_table_service.update_document(
                document_id=event_table.id,
                data=EventTableServiceUpdate(columns_info=columns_info),
                return_document=True,
            )
            return event_table

    return factory


@pytest_asyncio.fixture(name="event_table")
async def event_table_fixture(event_table_factory, entity, entity_transaction):
    """EventTable model"""
    _ = entity, entity_transaction
    return await event_table_factory()


@pytest_asyncio.fixture(name="item_table")
async def item_table_fixture(test_dir, feature_store, item_table_service):
    """ItemTable model"""
    _ = feature_store
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
def feature_factory_fixture(test_dir, feature_service):
    """
    Feature factory

    Note that this will only create the feature, and might throw an error if used alone. You'll need to make sure that
    you've created the event table and entity models first.
    """

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
async def feature_list_fixture(test_dir, feature, feature_list_service):
    """Feature list model"""
    _ = feature
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_list_single.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature_list = await feature_list_service.create_document(
            data=FeatureListServiceCreate(**payload)
        )
        return feature_list


@pytest_asyncio.fixture(name="feature_list_namespace")
async def feature_list_namespace_fixture(feature_list_namespace_service, feature_list):
    """FeatureListNamespace fixture"""
    return await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )


@pytest_asyncio.fixture(name="production_ready_feature_list")
async def production_ready_feature_list_fixture(production_ready_feature, feature_list_service):
    """Fixture for a production ready feature list"""
    data = FeatureListServiceCreate(
        name="Production Ready Feature List",
        feature_ids=[production_ready_feature.id],
    )
    result = await feature_list_service.create_document(data)
    return result


@pytest_asyncio.fixture(name="deployed_feature_list")
async def deployed_feature_list_fixture(
    online_enable_service_data_warehouse_mocks,
    deploy_service,
    feature_list_service,
    production_ready_feature_list,
):
    """Fixture for a deployed feature list"""
    _ = online_enable_service_data_warehouse_mocks
    await deploy_service.create_deployment(
        feature_list_id=production_ready_feature_list.id,
        deployment_id=ObjectId(),
        deployment_name="test-deployment",
        to_enable_deployment=True,
        get_credential=Mock(),
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
        payload = FeatureServiceCreate(**payload, user_id=user.id).dict(by_alias=True)
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
async def target_fixture(test_dir, target_service, event_table, item_table):
    """Target model"""
    _ = event_table, item_table
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
    new_flist = await feature_list_service.create_document(
        data=FeatureListServiceCreate(
            feature_ids=[new_feature_id],
            feature_list_namespace_id=feature_list.feature_list_namespace_id,
            name="sf_feature_list",
            version={"name": "V220914"},
        )
    )
    flist_namespace = await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )
    assert len(flist_namespace.feature_list_ids) == 2
    assert new_flist.id in flist_namespace.feature_list_ids
    assert new_flist.feature_list_namespace_id == feature_list.feature_list_namespace_id
    assert flist_namespace.default_version_mode == "AUTO"
    assert flist_namespace.default_feature_list_id == feature_list.id
    assert flist_namespace.readiness_distribution.__root__ == [{"readiness": "DRAFT", "count": 1}]
    yield new_feature_id, new_flist.id


async def create_event_table_with_entities(data_name, test_dir, event_table_service, columns):
    """Helper function to create an EventTable with provided columns and entities"""

    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/event_table.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())

    def _update_columns_info(columns_info):
        new_columns_info = []
        keep_cols = ["event_timestamp", "created_at", "col_int"]
        for col_info in columns_info:
            if col_info["name"] in keep_cols:
                new_columns_info.append(col_info)
        for col_name, col_entity_id in columns:
            col_info = {"name": col_name, "entity_id": col_entity_id, "dtype": "INT"}
            new_columns_info.append(col_info)
        return new_columns_info

    payload.pop("_id")
    payload["tabular_source"]["table_details"]["table_name"] = data_name
    payload["columns_info"] = _update_columns_info(payload["columns_info"])
    payload["name"] = data_name

    event_table = await event_table_service.create_document(data=EventTableCreate(**payload))
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
    entity_service,
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
    )
    parents = (await entity_service.get_document(child_entity.id)).parents
    parents += [ParentEntity(id=parent_entity.id, table_type=table.type, table_id=table.id)]
    update_parents = EntityServiceUpdate(parents=parents)
    await entity_service.update_document(child_entity.id, update_parents)
    return table


@pytest_asyncio.fixture(name="b_is_parent_of_a")
async def b_is_parent_of_a_fixture(
    entity_a,
    entity_b,
    entity_service,
    event_table_service,
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
        entity_service,
        child_entity=entity_a,
        parent_entity=entity_b,
        child_column="a",
        parent_column="b",
    )


@pytest_asyncio.fixture(name="c_is_parent_of_b")
async def c_is_parent_of_b_fixture(
    entity_b,
    entity_c,
    entity_service,
    event_table_service,
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
        entity_service,
        child_entity=entity_b,
        parent_entity=entity_c,
        child_column="b",
        parent_column="c",
    )


@pytest_asyncio.fixture(name="d_is_parent_of_b")
async def d_is_parent_of_b_fixture(
    entity_b,
    entity_d,
    entity_service,
    event_table_service,
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
        entity_service,
        child_entity=entity_b,
        parent_entity=entity_d,
        child_column="b",
        parent_column="d",
    )


@pytest_asyncio.fixture(name="d_is_parent_of_c")
async def d_is_parent_of_c_fixture(
    entity_c,
    entity_d,
    entity_service,
    event_table_service,
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
        entity_service,
        child_entity=entity_c,
        parent_entity=entity_d,
        child_column="c",
        parent_column="d",
    )


@pytest_asyncio.fixture(name="e_is_parent_of_c_and_d")
async def e_is_parent_of_c_and_d_fixture(
    entity_c,
    entity_d,
    entity_e,
    entity_service,
    event_table_service,
    test_dir,
    feature_store,
):
    """
    Fixture to make E a parent of C and D
    """
    _ = feature_store
    await create_table_and_add_parent(
        test_dir,
        event_table_service,
        entity_service,
        child_entity=entity_c,
        parent_entity=entity_e,
        child_column="c",
        parent_column="e",
    )
    await create_table_and_add_parent(
        test_dir,
        event_table_service,
        entity_service,
        child_entity=entity_d,
        parent_entity=entity_e,
        child_column="d",
        parent_column="e",
    )


@pytest.fixture
def entity_info_with_ambiguous_relationships(
    entity_a,
    entity_e,
    b_is_parent_of_a,
    c_is_parent_of_b,
    d_is_parent_of_b,
    e_is_parent_of_c_and_d,
) -> EntityInfo:
    """
    EntityInfo the arises from ambiguous relationships

    a (provided) --> b --> c ---> e (required)
                      `--> d --´
    """
    _ = b_is_parent_of_a
    _ = c_is_parent_of_b
    _ = d_is_parent_of_b
    _ = e_is_parent_of_c_and_d
    entity_info = EntityInfo(
        required_entities=[entity_e],
        provided_entities=[entity_a],
        serving_names_mapping={"A": "new_A"},
    )
    return entity_info


@pytest.fixture
def relationships(b_is_parent_of_a, c_is_parent_of_b):
    """
    Fixture to register all relationships
    """
    _ = b_is_parent_of_a
    _ = c_is_parent_of_b
    yield


@pytest.fixture(name="mock_update_data_warehouse")
def mock_update_data_warehouse():
    """Mock update data warehouse method"""
    with patch(
        "featurebyte.service.deploy.OnlineEnableService.update_data_warehouse"
    ) as mock_update_data_warehouse:
        yield mock_update_data_warehouse
