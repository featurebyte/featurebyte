"""
Test lazy app container module
"""
from typing import Any

import pytest
from bson import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID, User
from featurebyte.routes.app_container_config import AppContainerConfig
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.service.task_manager import TaskManager
from featurebyte.utils.storage import get_storage, get_temp_storage
from featurebyte.worker import get_celery


class NoDeps:
    """
    Test object with no deps
    """

    def __init__(self):
        self.no_deps = []


class TestService:
    """
    Test service
    """

    def __init__(self, user: Any, persistent: Any, catalog_id: ObjectId):
        self.user = user
        self.persistent = persistent
        self.catalog_id = catalog_id


class TestServiceWithOtherDeps:
    """
    Test service with other deps
    """

    def __init__(self, user: Any, persistent: Any, catalog_id: ObjectId, other_dep: Any):
        self.user = user
        self.persistent = persistent
        self.catalog_id = catalog_id
        self.other_dep = other_dep


class TestController:
    """
    Test controller
    """

    def __init__(self, test_service: TestService):
        self.test_service = test_service


@pytest.fixture(name="app_container_constructor_params")
def app_container_constructor_params_fixture(persistent):
    """
    Get app container constructor params
    """
    user = User()
    return {
        "user": user,
        "persistent": persistent,
        "temp_storage": get_temp_storage(),
        "storage": get_storage(),
        "task_manager": TaskManager(
            user=user, persistent=persistent, celery=get_celery(), catalog_id=DEFAULT_CATALOG_ID
        ),
        "catalog_id": DEFAULT_CATALOG_ID,
    }


@pytest.fixture(name="test_app_config")
def test_app_config_fixture():
    """
    Test app config fixture
    """
    app_container_config = AppContainerConfig()
    app_container_config.add_no_dep_objects("no_dep", NoDeps)
    app_container_config.add_basic_service("test_service", TestService)
    app_container_config.add_service_with_extra_deps(
        "extra_deps", TestServiceWithOtherDeps, ["test_service"]
    )
    app_container_config.add_controller("test_controller", TestController, ["test_service"])
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


def test_construction__empty_app_config_has_two_instances(app_container_constructor_params):
    """
    Test construction
    """
    app_container_config = AppContainerConfig()
    app_container = LazyAppContainer(
        **app_container_constructor_params,
        app_container_config=app_container_config,
    )
    assert len(app_container.instance_map) == 4


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
    assert app_container.task_controller is not None

    # random_item has not been initialized
    with pytest.raises(KeyError):
        assert app_container.random_item


def test_construction__build_with_missing_deps(app_container_constructor_params):
    """
    Test that an error is raised in an invalid dependency is passed in.
    """
    app_container_config = AppContainerConfig()
    app_container_config.add_service_with_extra_deps(
        "extra_deps", TestServiceWithOtherDeps, ["test_service"]
    )

    # KeyError raised as no `test_service` dep found
    app_container = LazyAppContainer(
        **app_container_constructor_params,
        app_container_config=app_container_config,
    )
    with pytest.raises(KeyError):
        _ = app_container.extra_deps


def test_construction__service_with_invalid_constructor(app_container_constructor_params):
    """
    Test that error is thrown if a service has been registered but doesn't have the right parameters required
    in its constructor.

    This should ideally be checked during the construction of the `app_container_config` so that the feedback loop
    is tighter for users.
    """
    app_container_config = AppContainerConfig()
    app_container_config.add_basic_service("random", TestController)

    app_container = LazyAppContainer(
        **app_container_constructor_params,
        app_container_config=app_container_config,
    )
    with pytest.raises(TypeError) as exc:
        _ = app_container.random

    assert "unexpected keyword argument" in str(exc)
