"""
Test app container config
"""
import pytest

from featurebyte.routes.app_container_config import AppContainerConfig


class TestClassA:
    """
    Test class A
    """

    def __init__(self):
        self.a = "a"


class TestClassB:
    """
    Test class B
    """

    def __init__(self, test_class_a: TestClassA):
        self.test_class_a = test_class_a


def test_all_dependencies():
    """
    Test all dependencies
    """
    config = AppContainerConfig()
    config.add_no_dep_objects("test_class_a", TestClassA)
    config.add_class_with_deps("test_class_b", TestClassB, ["test_class_a"])
    config.add_basic_service("basic_service", TestClassA)
    config.add_service_with_extra_deps("service_with_deps", TestClassB, ["test_class_a"])

    all_deps = config._all_dependencies()
    assert len(all_deps) == 4


def test_validate():
    """
    Test validate
    """
    config = AppContainerConfig()
    config.add_no_dep_objects("test_class_a", TestClassA)
    config.add_no_dep_objects("test_class_a", TestClassB)

    with pytest.raises(ValueError) as exc:
        config.validate()
    assert "error creating dependency map" in str(exc)
