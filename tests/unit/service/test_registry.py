"""
Test registry service.
"""

import random
from unittest.mock import Mock, patch

import pytest
from bson import ObjectId

from featurebyte import Catalog, FeatureStore
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.registry import app_container_config


@pytest.fixture(name="another_feature_store")
def another_feature_store_fixture(
    snowflake_feature_store_params,
    snowflake_execute_query,
):
    """
    Snowflake database source fixture
    """
    _ = snowflake_execute_query
    snowflake_feature_store_params["_id"] = ObjectId()
    snowflake_feature_store_params["name"] = "another_feature_store"
    snowflake_feature_store_params["type"] = "snowflake"
    snowflake_feature_store_params["details"].schema_name = "another_sf_schema"
    feature_store = FeatureStore(**snowflake_feature_store_params)
    feature_store.save()
    return feature_store


@pytest.fixture(name="registry_service_map")
def registry_service_fixture(user, persistent, snowflake_feature_store, another_feature_store):
    """Registry service fixture."""
    service_map = {}
    fs_catalog_name_pairs = [
        (snowflake_feature_store.name, "catalog1"),
        (snowflake_feature_store.name, "catalog2"),
        (another_feature_store.name, "catalog3"),
        (another_feature_store.name, "catalog4"),
    ]
    for feature_store_name, catalog_name in fs_catalog_name_pairs:
        catalog = Catalog.create(name=catalog_name, feature_store_name=feature_store_name)
        app_container = LazyAppContainer(
            app_container_config=app_container_config,
            instance_map={
                "user": user,
                "persistent": persistent,
                "catalog_id": catalog.id,
            },
        )
        service_map[(feature_store_name, catalog_name)] = app_container.feast_registry_service
    return service_map


@pytest.mark.asyncio
async def test_registry_service__project_name_creation(registry_service_map):
    """Test registry service create document"""
    catalog_name_to_table_prefix = {
        "catalog1": "cat1",  # snowflake feature store
        "catalog2": "cat2",  # snowflake feature store
        "catalog3": "cat1",  # another snowflake feature store
        "catalog4": "cat2",  # another snowflake feature store
    }
    registry_service = None
    existing_catalog_ids = set()
    existing_project_names = set()
    deployment = Mock()
    deployment.registry_info = None
    for (_, catalog_name), registry_service in registry_service_map.items():
        deployment.id = ObjectId()
        catalog = Catalog.get(name=catalog_name)
        feast_registry = await registry_service.get_or_create_feast_registry(deployment=deployment)
        expected_table_prefix = catalog_name_to_table_prefix[catalog_name]
        existing_catalog_ids.add(catalog.id)
        existing_project_names.add(feast_registry.name)
        assert feast_registry.name == str(deployment.id)[-7:]
        assert feast_registry.offline_table_name_prefix == expected_table_prefix

    # test create project name (check that new project name is generated)
    assert registry_service is not None
    random.seed(0)
    for catalog_id in existing_catalog_ids:
        new_project_name = await registry_service._create_project_name(catalog_id)
        assert new_project_name not in existing_project_names

    # check max try
    existing_deployment_id = deployment.id
    with patch("featurebyte.feast.service.registry.random") as mock_random:
        mock_random.randrange.return_value = int(str(existing_deployment_id)[-7:], 16)
        with pytest.raises(RuntimeError, match="Unable to generate unique project name"):
            await registry_service._create_project_name(existing_deployment_id)
