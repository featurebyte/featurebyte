"""
Test feast registry service
"""
import pytest
import pytest_asyncio

from featurebyte.feast.schema.registry import FeastRegistryCreate, FeastRegistryUpdate


@pytest.fixture(autouse=True)
def always_configure_online_store(catalog, mysql_online_store):
    """Configure online store for all tests in this directory"""
    catalog.update_online_store(mysql_online_store.name)
    yield
    catalog.update_online_store(None)


@pytest.fixture(name="feast_registry_service")
def feast_registry_service_fixture(app_container):
    """Feast registry service fixture"""
    return app_container.feast_registry_service


@pytest.fixture(name="feast_feature_store_service")
def feast_feature_store_service_fixture(app_container):
    """Feast feature store service fixture"""
    return app_container.feast_feature_store_service


@pytest_asyncio.fixture(name="feast_registry")
async def feast_registry_fixture(feast_registry_service, feature_list):
    """Feast registry fixture"""
    feature_list_model = feature_list.cached_model
    registry = await feast_registry_service.create_document(
        data=FeastRegistryCreate(
            project_name="test_project",
            feature_lists=[feature_list_model],
        )
    )
    return registry


@pytest.mark.asyncio
async def test_create_and_retrieve_feast_registry(
    feast_registry_service, feast_registry, feature_list, snowflake_feature_store
):
    """Test create and retrieve feast registry"""
    feature_list_model = feature_list.cached_model
    registry = feast_registry
    registry_proto = registry.registry_proto()
    assert registry.name == "test_project"
    assert registry.catalog_id == feature_list_model.catalog_id
    assert registry.feature_store_id == snowflake_feature_store.cached_model.id
    assert len(registry_proto.feature_services) == 1
    assert registry_proto.feature_services[0].spec.name == "test_feature_list"

    retrieved_registry = await feast_registry_service.get_document(document_id=registry.id)
    assert retrieved_registry == registry


@pytest.mark.asyncio
async def test_update_feast_registry(feast_registry_service, feast_registry, feature_list):
    """Test update feast registry"""
    updated_doc = await feast_registry_service.update_document(
        document_id=feast_registry.id, data=FeastRegistryUpdate(feature_lists=[])
    )
    registry_proto = updated_doc.registry_proto()
    assert registry_proto.feature_services == []
    assert registry_proto.feature_views == []
    assert registry_proto.data_sources == []
    assert registry_proto.entities == []

    feature_list_model = feature_list.cached_model
    updated_doc = await feast_registry_service.update_document(
        document_id=feast_registry.id, data=FeastRegistryUpdate(feature_lists=[feature_list_model])
    )
    registry_proto = updated_doc.registry_proto()
    assert len(registry_proto.feature_services) == 1
    assert registry_proto.feature_services[0].spec.name == "test_feature_list"
    assert len(registry_proto.feature_views) == 2
    assert len(registry_proto.data_sources) == 3
    assert len(registry_proto.entities) == 2


@pytest.mark.asyncio
async def test_get_feast_feature_store(feast_feature_store_service, feast_registry, catalog_id):
    """Test get feast feature store"""
    feast_fs = await feast_feature_store_service.get_feast_feature_store(
        feast_registry_id=feast_registry.id
    )

    # check that assets in the registry can be retrieved
    data_src_names = [data_source.name for data_source in feast_fs.list_data_sources()]
    assert set(data_src_names) == {
        "POINT_IN_TIME",
        f"fb_entity_cust_id_fjs_1800_300_600_ttl_{catalog_id}",
        f"fb_entity_transaction_id_fjs_86400_0_0_{catalog_id}",
    }
    entity_names = [entity.name for entity in feast_fs.list_entities()]
    assert set(entity_names) == {"cust_id", "transaction_id"}
    fv_names = [feature_view.name for feature_view in feast_fs.list_feature_views()]
    assert set(fv_names) == {
        f"fb_entity_cust_id_fjs_1800_300_600_ttl_{catalog_id}",
        f"fb_entity_transaction_id_fjs_86400_0_0_{catalog_id}",
    }
    fs_names = [feature_service.name for feature_service in feast_fs.list_feature_services()]
    assert fs_names == ["test_feature_list"]
