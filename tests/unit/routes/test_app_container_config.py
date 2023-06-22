"""
Test app container config
"""
from __future__ import annotations

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
    config.register_class("test_class_a", TestClassA)
    config.register_class("test_class_b", TestClassB, ["test_class_a"])
    config.register_service("basic_service", TestClassA)
    config.register_service("service_with_deps", TestClassB, ["test_class_a"])

    all_deps = config._all_dependencies()
    assert len(all_deps) == 4


def test_validate():
    """
    Test validate
    """
    config = AppContainerConfig()
    config.register_class("test_class_a", TestClassA)
    config.register_class("test_class_a", TestClassB)

    with pytest.raises(ValueError) as exc:
        config.validate()
    assert "error creating dependency map" in str(exc)


def test_circular_dependencies():
    """
    Test circular dependencies are validated against.
    """
    config = AppContainerConfig()
    config.register_class("test_class_c", TestClassC, ["test_class_d"])
    config.register_class("test_class_d", TestClassD, ["test_class_e"])
    config.register_class("test_class_e", TestClassD, ["test_class_c"])
    with pytest.raises(ValueError) as exc:
        config.validate()
    assert "circular dependency in the dependency graph" in str(exc)
    assert "test_class_c -> test_class_d -> test_class_e -> test_class_c" in str(exc)
