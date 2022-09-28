"""
Common fixture for both unit and integration tests
"""
import os
from unittest.mock import patch

import pytest


def pytest_configure(config):
    """Set up additional pytest markers"""
    # register an additional marker
    config.addinivalue_line("markers", "no_mock_process_store: mark test to not mock process store")


def pytest_addoption(parser):
    """Set up additional pytest options"""
    parser.addoption("--update-fixtures", action="store_true", default=False)


@pytest.fixture(scope="session")
def update_fixtures(pytestconfig):
    """Fixture corresponding to pytest --update-fixtures option"""
    return pytestconfig.getoption("update_fixtures")


@pytest.fixture(autouse=True)
def mock_settings_env_vars():
    """Mask default config path to avoid unintentionally using a real configuration file"""
    with patch.dict(os.environ, {}):
        yield


@pytest.fixture(name="test_dir")
def test_directory_fixture():
    """Test directory"""
    path = os.path.dirname(os.path.abspath(__file__))
    return path
