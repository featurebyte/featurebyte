"""
Fixture for Service related unit tests
"""
# pylint: disable=duplicate-code
from __future__ import annotations

import json
import os.path
from unittest.mock import Mock, patch

import pytest
import pytest_asyncio
from bson.objectid import ObjectId

from featurebyte.enum import SemanticType, SourceType
from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.models.entity import ParentEntity
from featurebyte.models.entity_validation import EntityInfo
from featurebyte.schema.context import ContextCreate
from featurebyte.schema.dimension_data import DimensionDataCreate
from featurebyte.schema.entity import EntityCreate, EntityServiceUpdate
from featurebyte.schema.event_data import EventDataCreate, EventDataServiceUpdate
from featurebyte.schema.feature import FeatureCreate
from featurebyte.schema.feature_list import FeatureListCreate
from featurebyte.schema.feature_namespace import FeatureNamespaceServiceUpdate
from featurebyte.schema.feature_store import FeatureStoreCreate
from featurebyte.schema.item_data import ItemDataCreate
from featurebyte.schema.scd_data import SCDDataCreate
from featurebyte.service.context import ContextService
from featurebyte.service.data_update import DataUpdateService
from featurebyte.service.dimension_data import DimensionDataService
from featurebyte.service.entity import EntityService
from featurebyte.service.event_data import EventDataService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.item_data import ItemDataService
from featurebyte.service.scd_data import SCDDataService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.version import VersionService


@pytest.fixture(scope="session")
def user():
    """
    Mock user
    """
    user = Mock()
    user.id = ObjectId()
    return user


@pytest.fixture(name="get_credential")
def get_credential_fixture(config):
    """
    get_credential fixture
    """

    async def get_credential(user_id, feature_store_name):
        _ = user_id
        return config.credentials.get(feature_store_name)

    return get_credential


@pytest.fixture(name="feature_store_service")
def feature_store_service_fixture(user, persistent):
    """FeatureStore service"""
    return FeatureStoreService(user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID)


@pytest.fixture(name="entity_service")
def entity_service_fixture(user, persistent):
    """Entity service"""
    return EntityService(user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID)


@pytest.fixture(name="semantic_service")
def semantic_service_fixture(user, persistent):
    """Semantic service"""
    return SemanticService(user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID)


@pytest.fixture(name="context_service")
def context_service_fixture(user, persistent):
    """Context service"""
    return ContextService(user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID)


@pytest.fixture(name="event_data_service")
def event_data_service_fixture(user, persistent):
    """EventData service"""
    return EventDataService(user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID)


@pytest.fixture(name="item_data_service")
def item_data_service_fixture(user, persistent):
    """ItemData service"""
    return ItemDataService(user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID)


@pytest.fixture(name="dimension_data_service")
def dimension_data_service_fixture(user, persistent):
    """DimensionData service"""
    return DimensionDataService(user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID)


@pytest.fixture(name="scd_data_service")
def scd_data_service_fixture(user, persistent):
    """SCDData service"""
    return SCDDataService(user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID)


@pytest.fixture(name="feature_namespace_service")
def feature_namespace_service_fixture(user, persistent):
    """FeatureNamespaceService fixture"""
    return FeatureNamespaceService(user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID)


@pytest.fixture(name="feature_service")
def feature_service_fixture(user, persistent):
    """FeatureService fixture"""
    return FeatureService(user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID)


@pytest.fixture(name="feature_list_namespace_service")
def feature_list_namespace_service_fixture(user, persistent):
    """FeatureListNamespaceService fixture"""
    return FeatureListNamespaceService(
        user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID
    )


@pytest.fixture(name="feature_list_service")
def feature_list_service_fixture(user, persistent):
    """FeatureListService fixture"""
    return FeatureListService(user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID)


@pytest.fixture(name="data_update_service")
def data_update_service_fixture(user, persistent):
    """DataUpdateService fixture"""
    return DataUpdateService(user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID)


@pytest.fixture(name="feature_readiness_service")
def feature_readiness_service_fixture(app_container):
    """FeatureReadinessService fixture"""
    return app_container.feature_readiness_service


@pytest.fixture(name="default_version_mode_service")
def default_version_mode_service_fixture(app_container):
    """DefaultVersionModeService fixture"""
    return app_container.default_version_mode_service


@pytest.fixture(name="online_enable_service")
def online_enable_service_fixture(app_container):
    """OnlineEnableService fixture"""
    return app_container.online_enable_service


@pytest.fixture(name="preview_service")
def preview_service_fixture(app_container):
    """PreviewService fixture"""
    return app_container.preview_service


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
            "featurebyte.service.online_enable.FeatureManager", autospec=True
        ) as feature_manager_cls:
            mock_get_feature_store_session.return_value = Mock(source_type=SourceType.SNOWFLAKE)
            mocks["get_feature_store_session"] = mock_get_feature_store_session
            mocks["feature_manager"] = feature_manager_cls.return_value
            yield mocks


@pytest.fixture(name="online_serving_service")
def online_serving_service_fixture(app_container):
    """OnlineEnableService fixture"""
    return app_container.online_serving_service


@pytest.fixture(name="version_service")
def version_service_fixture(user, persistent):
    """VersionService fixture"""
    return VersionService(user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID)


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


@pytest_asyncio.fixture(name="event_data")
async def event_data_fixture(test_dir, feature_store, event_data_service, semantic_service):
    """EventData model"""
    _ = feature_store
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/event_data.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        payload["tabular_source"]["table_details"]["table_name"] = "sf_event_table"
        event_data = await event_data_service.create_document(data=EventDataCreate(**payload))
        event_timestamp = await semantic_service.get_or_create_document(
            name=SemanticType.EVENT_TIMESTAMP
        )
        columns_info = []
        for col in event_data.columns_info:
            col_dict = col.dict()
            if col.name == "event_timestamp":
                col_dict["semantic_id"] = event_timestamp.id
            columns_info.append(col_dict)

        event_data = await event_data_service.update_document(
            document_id=event_data.id,
            data=EventDataServiceUpdate(columns_info=columns_info),
            return_document=True,
        )
        return event_data


@pytest_asyncio.fixture(name="item_data")
async def item_data_fixture(test_dir, feature_store, item_data_service):
    """ItemData model"""
    _ = feature_store
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/item_data.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        payload["tabular_source"]["table_details"]["table_name"] = "sf_item_table"
        item_data = await item_data_service.create_document(data=ItemDataCreate(**payload))
        return item_data


@pytest_asyncio.fixture(name="dimension_data")
async def dimension_data_fixture(test_dir, feature_store, dimension_data_service):
    """DimensionData model"""
    _ = feature_store
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/dimension_data.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        payload["tabular_source"]["table_details"]["table_name"] = "sf_dimension_table"
        item_data = await dimension_data_service.create_document(
            data=DimensionDataCreate(**payload)
        )
        return item_data


@pytest_asyncio.fixture(name="scd_data")
async def scd_data_fixture(test_dir, feature_store, scd_data_service):
    """SCDData model"""
    _ = feature_store
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/scd_data.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        payload["tabular_source"]["table_details"]["table_name"] = "sf_scd_table"
        scd_data = await scd_data_service.create_document(data=SCDDataCreate(**payload))
        return scd_data


@pytest_asyncio.fixture(name="feature")
async def feature_fixture(test_dir, event_data, entity, feature_service):
    """Feature model"""
    _ = event_data, entity
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_sum_30m.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature = await feature_service.create_document(data=FeatureCreate(**payload))
        return feature


@pytest_asyncio.fixture(name="feature_iet")
async def feature_iet_fixture(test_dir, event_data, entity, feature_service):
    """Feature model (IET feature)"""
    _ = event_data, entity
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_iet.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature = await feature_service.create_document(data=FeatureCreate(**payload))
        return feature


@pytest_asyncio.fixture(name="feature_non_time_based")
async def feature_non_time_based_fixture(
    test_dir, event_data, item_data, entity_transaction, feature_service
):
    """Feature model (non-time-based feature)"""
    _ = event_data, item_data, entity_transaction
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_non_time_based.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature = await feature_service.create_document(data=FeatureCreate(**payload))
        return feature


@pytest_asyncio.fixture(name="production_ready_feature")
async def production_ready_feature_fixture(feature_readiness_service, feature):
    """Production ready readiness feature fixture"""
    prod_feat = await feature_readiness_service.update_feature(
        feature_id=feature.id, readiness="PRODUCTION_READY"
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
        feature_list = await feature_list_service.create_document(data=FeatureListCreate(**payload))
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
    data = FeatureListCreate(
        name="Production Ready Feature List",
        feature_ids=[production_ready_feature.id],
    )
    result = await feature_list_service.create_document(data)
    return result


@pytest_asyncio.fixture(name="deployed_feature_list")
async def deployed_feature_list_fixture(
    online_enable_service_data_warehouse_mocks, deploy_service, production_ready_feature_list
):
    """Fixture for a deployed feature list"""
    _ = online_enable_service_data_warehouse_mocks
    updated_feature_list = await deploy_service.update_feature_list(
        feature_list_id=production_ready_feature_list.id,
        deployed=True,
        get_credential=Mock(),
        return_document=True,
    )
    return updated_feature_list


async def insert_feature_into_persistent(test_dir, user, persistent, readiness, name=None):
    """Insert a feature into persistent"""
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_sum_30m.json")
    with open(fixture_path) as fhandle:
        payload = json.loads(fhandle.read())
        payload = FeatureCreate(**payload, user_id=user.id).dict(by_alias=True)
        payload["_id"] = ObjectId()
        payload["readiness"] = readiness
        payload["catalog_id"] = DEFAULT_CATALOG_ID
        if name:
            payload["name"] = name
        feature_id = await persistent.insert_one(
            collection_name="feature",
            document=payload,
            user_id=user.id,
        )
        return feature_id


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
        data=FeatureListCreate(
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


async def create_event_data_with_entities(data_name, test_dir, event_data_service, columns):
    """Helper function to create an EventData with provided columns and entities"""

    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/event_data.json")
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

    event_data = await event_data_service.create_document(data=EventDataCreate(**payload))
    return event_data


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


async def create_data_and_add_parent(
    test_dir,
    event_data_service,
    entity_service,
    child_entity,
    parent_entity,
    child_column,
    parent_column,
):
    """
    Helper function to create data and add a relationship between two entities
    """
    data = await create_event_data_with_entities(
        f"{parent_entity.name}_is_parent_of_{child_entity.name}_data",
        test_dir,
        event_data_service,
        [(child_column, child_entity.id), (parent_column, parent_entity.id)],
    )
    parents = (await entity_service.get_document(child_entity.id)).parents
    parents += [ParentEntity(id=parent_entity.id, data_type=data.type, data_id=data.id)]
    update_parents = EntityServiceUpdate(parents=parents)
    await entity_service.update_document(child_entity.id, update_parents)
    return data


@pytest_asyncio.fixture(name="b_is_parent_of_a")
async def b_is_parent_of_a_fixture(
    entity_a,
    entity_b,
    entity_service,
    event_data_service,
    test_dir,
    feature_store,
):
    """
    Fixture to make B a parent of A
    """
    _ = feature_store
    return await create_data_and_add_parent(
        test_dir,
        event_data_service,
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
    event_data_service,
    test_dir,
    feature_store,
):
    """
    Fixture to make C a parent of B
    """
    _ = feature_store
    return await create_data_and_add_parent(
        test_dir,
        event_data_service,
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
    event_data_service,
    test_dir,
    feature_store,
):
    """
    Fixture to make D a parent of B
    """
    _ = feature_store
    return await create_data_and_add_parent(
        test_dir,
        event_data_service,
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
    event_data_service,
    test_dir,
    feature_store,
):
    """
    Fixture to make D a parent of C
    """
    _ = feature_store
    return await create_data_and_add_parent(
        test_dir,
        event_data_service,
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
    event_data_service,
    test_dir,
    feature_store,
):
    """
    Fixture to make E a parent of C and D
    """
    _ = feature_store
    await create_data_and_add_parent(
        test_dir,
        event_data_service,
        entity_service,
        child_entity=entity_c,
        parent_entity=entity_e,
        child_column="c",
        parent_column="e",
    )
    await create_data_and_add_parent(
        test_dir,
        event_data_service,
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
