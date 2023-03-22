"""
Config related fixtures.
"""
import os
import tempfile
from unittest import mock

import pytest
import yaml

from featurebyte import Configurations


@pytest.fixture(name="config_file")
def config_file_fixture():
    """
    Config file for unit testing
    """
    config_dict = {
        "credential": [
            {
                "feature_store": "sf_featurestore",
                "credential_type": "USERNAME_PASSWORD",
                "username": "sf_user",
                "password": "sf_password",
            },
            {
                "feature_store": "sq_featurestore",
            },
        ],
        "profile": [
            {
                "name": "local",
                "api_url": "http://localhost:8080",
                "api_token": "token",
            },
        ],
        "logging": {
            "level": "DEBUG",
            "telemetry": False,
            "telemetry_url": "http://127.0.0.1",
        },
    }
    with tempfile.TemporaryDirectory() as tempdir:
        config_file_path = os.path.join(tempdir, "config.yaml")
        with open(config_file_path, "w") as file_handle:
            file_handle.write(yaml.dump(config_dict))
            file_handle.flush()
            yield config_file_path


@pytest.fixture(name="config")
def config_fixture(config_file):
    """
    Config object for unit testing
    """
    yield Configurations(config_file_path=config_file)


@pytest.fixture(name="mock_config_path_env")
def mock_config_path_env_fixture(config_file):
    """
    Mock FEATUREBYTE_HOME in featurebyte/config.py
    """

    def mock_env_side_effect(*args, **kwargs):
        if args[0] == "FEATUREBYTE_HOME":
            return os.path.dirname(config_file)
        env = dict(os.environ)
        return env.get(*args, **kwargs)

    with mock.patch("featurebyte.config.os.environ.get") as mock_env_get:
        mock_env_get.side_effect = mock_env_side_effect
        yield
