"""
Test app container
"""
from featurebyte.routes.app_container import app_container_config, register_service_constructor


class TestClass:
    """
    Local test class
    """

    pass


def test_register_service_constructor():
    original_services = app_container_config["services"].copy()
    assert len(original_services) > 0
    register_service_constructor(TestClass)
    services = app_container_config["services"].copy()
    assert len(services) == (len(original_services) + 1)
