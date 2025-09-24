"""Test feast registry service"""

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.feast.schema.registry import FeastRegistryUpdate
from featurebyte.schema.worker.task.deployment_create_update import CreateDeploymentPayload


@pytest.fixture(name="registry_service")
def registry_service_fixture(app_container):
    """Fixture for registry service"""
    return app_container.feast_registry_service


@pytest.fixture(name="deploy_service")
def deploy_service_fixture(app_container):
    """Fixture for deploy service"""
    return app_container.deploy_service


@pytest.fixture(name="deployment_service")
def deployment_service_fixture(app_container):
    """Fixture for deployment service"""
    return app_container.deployment_service


@pytest.fixture(name="storage")
def storage_fixture(app_container):
    """Fixture for storage"""
    return app_container.storage


@pytest_asyncio.fixture(name="deployment_id")
async def deployment_id_fixture(app_container, feature_list):
    """Fixture for test deployment"""
    feature_readiness_service = app_container.feature_readiness_service
    for feature_id in feature_list.feature_ids:
        await feature_readiness_service.update_feature(
            feature_id=feature_id, readiness="PRODUCTION_READY"
        )

    deploy_service = app_container.deploy_service
    deployment_id = ObjectId()
    await deploy_service.create_deployment(
        deployment_id=deployment_id,
        payload=CreateDeploymentPayload(
            name="test_deployment",
            feature_list_id=feature_list.id,
            enabled=False,
        ),
    )
    return deployment_id


@pytest_asyncio.fixture(name="registry_with_path")
async def registry_with_path_fixture(
    deploy_service,
    registry_service,
    deployment_service,
    deployment_id,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """Fixture for registry without path"""
    _ = mock_update_data_warehouse, mock_offline_store_feature_manager_dependencies

    await deploy_service.update_deployment(deployment_id=deployment_id, to_enable_deployment=True)

    deployment = await deployment_service.get_document(document_id=deployment_id)
    registry = await registry_service.get_feast_registry(deployment=deployment)
    registry_doc = await registry_service.get_document_as_dict(document_id=registry.id)
    assert registry_doc["registry_path"]
    assert "registry" not in registry_doc
    return registry


@pytest_asyncio.fixture(name="registry_without_path")
async def registry_without_path_fixture(
    registry_with_path,
    registry_service,
    persistent,
    user_id,
):
    """Fixture for registry without path"""
    registry_id = ObjectId()
    registry_doc = registry_with_path.model_dump(by_alias=True)
    registry_doc["_id"] = registry_id
    registry_doc["registry_path"] = None
    registry_doc["registry"] = registry_with_path.registry
    await persistent.insert_one(
        collection_name="feast_registry", document=registry_doc, user_id=user_id
    )

    registry = await registry_service.get_document(document_id=registry_id)
    assert registry.registry_path is None
    return registry


@pytest.mark.asyncio
async def test_get_document__registry_without_path(registry_without_path, registry_service):
    """Test get document"""
    registry = await registry_service.get_document(document_id=registry_without_path.id)
    assert registry == registry_without_path
    assert registry.registry_path is None
    assert registry.registry


@pytest.mark.asyncio
async def test_get_document__registry_with_path(registry_with_path, registry_service, storage):
    """Test get document"""
    registry = await registry_service.get_document(document_id=registry_with_path.id)
    assert registry == registry_with_path
    assert registry.registry_path
    assert registry.registry

    # check that registry is stored in storage
    registry_bytes = await storage.get_bytes(registry.registry_path)
    assert registry.registry == registry_bytes


@pytest.mark.asyncio
async def test_update_document__registry_without_path(
    registry_without_path, registry_service, storage, feature_list
):
    """Test update document on older record (registry without path)"""
    registry = await registry_service.update_document(
        document_id=registry_without_path.id,
        data=FeastRegistryUpdate(feature_lists=[feature_list]),
        populate_remote_attributes=True,
    )
    assert registry.registry_path
    assert registry.registry

    # get original registry document & check that it has been updated
    registry_doc = await registry_service.get_document_as_dict(document_id=registry.id)
    assert "registry" not in registry_doc
    assert registry_doc["registry_path"]

    # check the registry is stored in storage
    registry_bytes = await storage.get_bytes(registry_doc["registry_path"])
    assert registry.registry == registry_bytes


@pytest.mark.asyncio
async def test_update_document__registry_with_path(
    registry_with_path, registry_service, deployment_service, storage, feature_list
):
    """Test update document on older record (registry with path)"""
    registry = await registry_service.update_document(
        document_id=registry_with_path.id,
        data=FeastRegistryUpdate(feature_lists=[feature_list]),
        populate_remote_attributes=True,
    )
    assert registry.registry
    assert registry.name == registry_with_path.name
    assert registry.registry_path == registry_with_path.registry_path
    assert registry.offline_table_name_prefix == registry_with_path.offline_table_name_prefix

    # check get_feast_registry returns the updated registry
    assert registry.deployment_id is not None
    deployment = await deployment_service.get_document(document_id=registry.deployment_id)
    retrieved_registry = await registry_service.get_feast_registry(deployment=deployment)
    assert retrieved_registry == registry

    # get original registry document
    registry_doc = await registry_service.get_document_as_dict(document_id=registry.id)
    assert "registry" not in registry_doc

    # check underlying storage
    registry_bytes = await storage.get_bytes(registry.registry_path)
    assert registry.registry == registry_bytes
