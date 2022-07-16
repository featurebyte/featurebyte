"""
Common test fixtures used across files in api directory
"""
import os
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True, scope="session")
def mock_settings_env_vars(config):
    """Override default config path for all API tests"""
    with patch.dict(
        os.environ,
        {"FEATUREBYTE_CONFIG_PATH": config._config_file_path},  # pylint: disable=protected-access
    ):
        yield


@pytest.fixture(autouse=True, scope="session")
def mock_persistent(mock_get_persistent):
    """Use mock persistent for all API tests"""
    _ = mock_get_persistent
    yield
