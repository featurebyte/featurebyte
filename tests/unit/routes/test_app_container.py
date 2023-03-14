"""
Test app container
"""
from typing import Any

import pytest
from bson import ObjectId

from featurebyte.app import User
from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.routes.app_container import AppContainer
from featurebyte.routes.app_container_config import AppContainerConfig
from featurebyte.service.task_manager import TaskManager
from featurebyte.utils.storage import get_storage, get_temp_storage


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
            user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID
        ),
        "catalog_id": DEFAULT_CATALOG_ID,
    }


def test_construction__empty_app_config_has_two_instances(app_container_constructor_params):
    """
    Test construction
    """
    app_container_config = AppContainerConfig()
    app_container = AppContainer(
        **app_container_constructor_params,
        app_config=app_container_config,
    )
    assert len(app_container.instance_map) == 2


def test_construction__get_attr(app_container_constructor_params):
    """
    Test __get_attr__ works
    """
    app_container_config = AppContainerConfig()
    app_container = AppContainer(
        **app_container_constructor_params,
        app_config=app_container_config,
    )
    # This has been initialized
    assert app_container.task_controller is not None

    # random_item has not been initialized
    with pytest.raises(KeyError):
        assert app_container.random_item


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


def test_construction__build_with_full_config(app_container_constructor_params):
    """
    Test that we are able to build the full app container.
    """
    app_container_config = AppContainerConfig()
    app_container_config.add_no_dep_objects("no_dep", NoDeps)
    app_container_config.add_basic_service("test_service", TestService)
    app_container_config.add_service_with_extra_deps(
        "extra_deps", TestServiceWithOtherDeps, ["test_service"]
    )
    app_container_config.add_controller("test_controller", TestController, ["test_service"])

    app_container = AppContainer(
        **app_container_constructor_params,
        app_config=app_container_config,
    )
    assert len(app_container.instance_map) == 6


def test_construction__build_with_missing_deps(app_container_constructor_params):
    """
    Test that an error is raised in an invalid dependency is passed in.
    """
    app_container_config = AppContainerConfig()
    app_container_config.add_service_with_extra_deps(
        "extra_deps", TestServiceWithOtherDeps, ["test_service"]
    )

    # KeyError raised as no `test_service` dep found
    with pytest.raises(KeyError):
        AppContainer(
            **app_container_constructor_params,
            app_config=app_container_config,
        )


def test_construction__service_with_invalid_constructor(app_container_constructor_params):
    """
    Test that error is thrown if a service has been registered but doesn't have the right parameters required
    in its constructor.

    This should ideally be checked during the construction of the `app_container_config` so that the feedback loop
    is tighter for users.
    """
    app_container_config = AppContainerConfig()
    app_container_config.add_basic_service("random", TestController)

    with pytest.raises(TypeError) as exc:
        AppContainer(
            **app_container_constructor_params,
            app_config=app_container_config,
        )
    assert "unexpected keyword argument" in str(exc)
