"""
Test lazy app container module
"""

from typing import Any, List

import pytest
from bson import ObjectId

from featurebyte.common import DEFAULT_CATALOG_ID
from featurebyte.models.base import User
from featurebyte.routes.app_container_config import AppContainerConfig, ClassDefinition
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.utils.persistent import MongoDBImpl


class NoDeps:
    """
    Test object with no deps
    """

    def __init__(self):
        self.no_deps = []


class TestServiceWithOneDep:
    """
    Test service with one dep
    """

    # add this to fix PytestCollectionWarning
    __test__ = False

    def __init__(self, no_deps: NoDeps):
        self.no_deps = no_deps


class TestService:
    """
    Test service
    """

    # add this to fix PytestCollectionWarning
    __test__ = False

    def __init__(self, user: Any, persistent: Any, catalog_id: ObjectId):
        self.user = user
        self.persistent = persistent
        self.catalog_id = catalog_id


class TestServiceWithOtherDeps:
    """
    Test service with other deps
    """

    # add this to fix PytestCollectionWarning
    __test__ = False

    def __init__(self, user: Any, persistent: Any, catalog_id: ObjectId, other_dep: Any):
        self.user = user
        self.persistent = persistent
        self.catalog_id = catalog_id
        self.other_dep = other_dep


class TestController:
    """
    Test controller
    """

    # add this to fix PytestCollectionWarning
    __test__ = False

    def __init__(self, test_service: TestService):
        self.test_service = test_service


@pytest.fixture(name="app_container_constructor_params")
def app_container_constructor_params_fixture():
    """
    Get app container constructor params
    """
    user = User()
    return {
        "user": user,
        "catalog_id": DEFAULT_CATALOG_ID,
    }


@pytest.fixture(name="test_app_config")
def test_app_config_fixture():
    """
    Test app config fixture
    """
    app_container_config = AppContainerConfig()
    app_container_config.register_class(NoDeps)
    app_container_config.register_class(TestService)
    app_container_config.register_class(TestServiceWithOtherDeps, {"other_dep": "test_service"})
    app_container_config.register_class(TestController)
    app_container_config.register_class(MongoDBImpl, name_override="persistent")
    return app_container_config


def test_lazy_initialization(test_app_config, app_container_constructor_params):
    """
    Test lazy initialization
    """
    lazy_app_container = LazyAppContainer(
        **app_container_constructor_params,
        app_container_config=test_app_config,
    )
    # Verify that test service is not in the instance map until we call it
    instance_map = lazy_app_container.instance_map
    assert "test_service" not in instance_map

    service = lazy_app_container.test_service
    assert service is not None
    # Service should be initialized
    instance_map = lazy_app_container.instance_map
    assert "test_service" in instance_map

    # Verify that other classes that are in the config, but not called, are not initialized
    assert "test_controller" not in instance_map
    test_controller = lazy_app_container.test_controller
    assert test_controller is not None
    instance_map = lazy_app_container.instance_map
    assert "test_controller" in instance_map


def test_getting_instance_with_factory_method(app_container_constructor_params):
    """
    Test retrieving an instance that has a dependency that is constructed via a factory method.
    """
    app_container_config = AppContainerConfig()

    def no_deps_factory() -> NoDeps:
        return NoDeps()

    app_container_config.register_class(TestServiceWithOneDep)
    app_container_config.register_factory_method(no_deps_factory)
    app_container = LazyAppContainer(
        app_container_config=app_container_config,
        **app_container_constructor_params,
    )
    instance = app_container.get(TestServiceWithOneDep)
    assert instance is not None


def test_construction__get_attr(app_container_constructor_params):
    """
    Test __get_attr__ works
    """
    app_container_config = AppContainerConfig()
    app_container = LazyAppContainer(
        **app_container_constructor_params,
        app_container_config=app_container_config,
    )
    # This has been initialized
    assert app_container.catalog_id is not None

    # random_item has not been initialized
    with pytest.raises(KeyError):
        assert app_container.random_item


def test_construction__build_with_missing_deps(app_container_constructor_params):
    """
    Test that an error is raised in an invalid dependency is passed in.
    """
    app_container_config = AppContainerConfig()
    app_container_config.register_class(TestServiceWithOtherDeps, {"other_dep": "test_service"})

    # KeyError raised as no `test_service` dep found
    app_container = LazyAppContainer(
        **app_container_constructor_params,
        app_container_config=app_container_config,
    )
    with pytest.raises(KeyError):
        _ = app_container.extra_deps


def test_get(app_container_constructor_params):
    """
    Test different ways of getting an object from the app container.
    """
    app_container_config = AppContainerConfig()
    app_container_config.register_class(TestService)

    app_container = LazyAppContainer(
        **app_container_constructor_params,
        app_container_config=app_container_config,
    )
    service = app_container.get("test_service")
    assert isinstance(service, TestService)

    service = app_container.test_service
    assert isinstance(service, TestService)

    service = app_container.get(TestService)
    assert isinstance(service, TestService)


def test_construction__service_with_invalid_constructor(app_container_constructor_params):
    """
    Test that error is thrown if a service has been registered but doesn't have the right parameters required
    in its constructor.

    This should ideally be checked during the construction of the `app_container_config` so that the feedback loop
    is tighter for users.
    """
    app_container_config = AppContainerConfig()
    app_container_config.register_class(TestController, name_override="random")

    app_container = LazyAppContainer(
        **app_container_constructor_params,
        app_container_config=app_container_config,
    )
    with pytest.raises(KeyError) as exc:
        _ = app_container.random
    assert "test_service" in str(exc)


def get_class_def(key: str, deps: List[str]) -> ClassDefinition:
    """
    Helper method to get class def
    """
    return ClassDefinition(
        name=key,
        getter=TestService,
        dependencies=deps,
    )


def test_disable_block_modification_check(app_container):
    """Test disable_block_modification_check"""
    # check that _check_block_modification_func is set properly for deeply nested services
    with app_container.block_modification_handler.disable_block_modification_check():
        # level-1 service
        assert (
            app_container.event_table_service.block_modification_handler.block_modification is False
        )
        # level-2 service
        service = app_container.table_facade_service.event_table_service
        assert service.block_modification_handler.block_modification is False
        # level-3 service
        service = app_container.table_facade_service.table_columns_info_service.semantic_service
        assert service.block_modification_handler.block_modification is False
        # level-4 service
        service = app_container.table_facade_service.table_columns_info_service.entity_relationship_service
        assert service.entity_service.block_modification_handler.block_modification is False

    # outside the context manager, the check should be enabled
    # level-1 service
    assert app_container.event_table_service.block_modification_handler.block_modification is True
    # level-2 service
    service = app_container.table_facade_service.event_table_service
    assert service.block_modification_handler.block_modification is True
    # level-3 service
    service = app_container.table_facade_service.table_columns_info_service.semantic_service
    assert service.block_modification_handler.block_modification is True
    # level-4 service
    service = (
        app_container.table_facade_service.table_columns_info_service.entity_relationship_service
    )
    assert service.entity_service.block_modification_handler.block_modification is True
