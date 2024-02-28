"""
Test feast registry service
"""
import pytest
import pytest_asyncio
from google.protobuf.json_format import MessageToDict

from featurebyte.common.model_util import get_version
from featurebyte.feast.schema.registry import FeastRegistryUpdate


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
async def feast_registry_fixture(feast_registry_service, feature_list, mock_deployment_flow):
    """Feast registry fixture"""
    _ = mock_deployment_flow
    deployment = feature_list.deploy(make_production_ready=True, ignore_guardrails=True)
    deployment.enable()

    registry = await feast_registry_service.get_feast_registry_for_catalog()
    assert registry is not None
    return registry


@pytest.mark.asyncio
async def test_create_and_retrieve_feast_registry(
    feast_registry_service, feast_registry, feature_list, snowflake_feature_store
):
    """Test create and retrieve feast registry"""
    feature_list_model = feature_list.cached_model
    registry = feast_registry
    registry_proto = registry.registry_proto()
    assert registry.catalog_id == feature_list_model.catalog_id
    assert registry.feature_store_id == snowflake_feature_store.cached_model.id
    assert len(registry_proto.feature_services) == 1
    assert registry_proto.feature_services[0].spec.name == f"test_feature_list_{get_version()}"

    retrieved_registry = await feast_registry_service.get_document(document_id=registry.id)
    assert retrieved_registry == registry


@pytest.mark.asyncio
async def test_update_feast_registry(
    feast_registry_service,
    feast_registry,
    feature_list,
    expected_entity_names,
    expected_data_source_names,
    expected_feature_view_name_to_ttl,
    expected_on_demand_feature_view_names,
):
    """Test update feast registry"""
    updated_doc = await feast_registry_service.update_document(
        document_id=feast_registry.id, data=FeastRegistryUpdate(feature_lists=[])
    )
    registry_proto = updated_doc.registry_proto()
    assert registry_proto.feature_services == []
    assert registry_proto.feature_views == []
    assert registry_proto.data_sources == []

    registry_dict = MessageToDict(registry_proto)
    assert registry_dict["entities"] == [
        {
            "meta": registry_dict["entities"][0]["meta"],
            "spec": {"joinKey": "__dummy_id", "name": "__dummy", "project": feast_registry.name},
        }
    ]

    feature_list_model = feature_list.cached_model
    updated_doc = await feast_registry_service.update_document(
        document_id=feast_registry.id, data=FeastRegistryUpdate(feature_lists=[feature_list_model])
    )
    registry_proto = updated_doc.registry_proto()
    assert {entity.spec.name for entity in registry_proto.entities} == expected_entity_names
    assert {ds.name for ds in registry_proto.data_sources} == expected_data_source_names
    assert {fv.spec.name for fv in registry_proto.feature_views} == set(
        expected_feature_view_name_to_ttl
    )
    assert {
        odfv.spec.name for odfv in registry_proto.on_demand_feature_views
    } == expected_on_demand_feature_view_names
    assert {fs.spec.name for fs in registry_proto.feature_services} == {
        f"test_feature_list_{get_version()}"
    }


@pytest.mark.asyncio
async def test_get_feast_feature_store(
    feast_feature_store_service,
    feast_registry,
    expected_entity_names,
    expected_data_source_names,
    expected_feature_view_name_to_ttl,
    expected_on_demand_feature_view_names,
):
    """Test get feast feature store"""
    feast_fs = await feast_feature_store_service.get_feast_feature_store(
        feast_registry_id=feast_registry.id
    )

    # check that assets in the registry can be retrieved
    entity_names = {entity.name for entity in feast_fs.list_entities()}
    assert entity_names == {name for name in expected_entity_names if name != "__dummy"}

    data_src_names = [data_source.name for data_source in feast_fs.list_data_sources()]
    assert set(data_src_names) == expected_data_source_names

    fv_name_to_ttl = {fv.name: fv.ttl for fv in feast_fs.list_feature_views()}
    assert fv_name_to_ttl == expected_feature_view_name_to_ttl

    odfv_names = {odfv.name for odfv in feast_fs.list_on_demand_feature_views()}
    assert odfv_names == expected_on_demand_feature_view_names

    fs_names = {feature_service.name for feature_service in feast_fs.list_feature_services()}
    assert fs_names == {f"test_feature_list_{get_version()}"}
