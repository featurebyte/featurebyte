"""
Test app container config
"""
from __future__ import annotations

import pytest

from featurebyte.routes.app_container_config import AppContainerConfig, _get_class_name


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


class TestClassC:
    """
    Test class C
    """

    def __init__(self, test_class_d: TestClassD):
        self.test_class_d = test_class_d


class TestClassD:
    """
    Test class D
    """

    def __init__(self, test_class_e: TestClassE):
        self.test_class_e = test_class_e


class TestClassE:
    """
    Test class E
    """

    def __init__(self, test_class_c: TestClassC):
        self.test_class_c = test_class_c


def test_all_dependencies():
    """
    Test all dependencies
    """
    config = AppContainerConfig()
    config.register_class(TestClassA)
    config.register_class(TestClassB, ["test_class_a"])
    config.register_service(
        TestClassA,
        name_override="basic_service",
    )
    config.register_service(TestClassB, ["test_class_a"], name_override="service_with_deps")

    all_deps = config._all_dependencies()
    assert len(all_deps) == 4


def test_validate__duplicate_name():
    """
    Test validate - duplicate name throws error
    """
    config = AppContainerConfig()
    config.register_class(TestClassA)
    config.register_class(TestClassB, name_override="test_class_a")

    with pytest.raises(ValueError) as exc:
        config.validate()
    assert "error creating dependency map" in str(exc)


def test_circular_dependencies():
    """
    Test circular dependencies are validated against.
    """
    config = AppContainerConfig()
    config.register_class(TestClassC, ["test_class_d"])
    config.register_class(TestClassD, ["test_class_e"])
    config.register_class(TestClassE, ["test_class_c"])
    with pytest.raises(ValueError) as exc:
        config.validate()
    assert "circular dependency in the dependency graph" in str(exc)
    assert "test_class_c -> test_class_d -> test_class_e -> test_class_c" in str(exc)


def test_get_class_name():
    """
    Test _get_class_name
    """
    name = _get_class_name(TestClassC)
    assert name == "test_class_c"

    name = _get_class_name(TestClassC, name_override="hello")
    assert name == "hello"
