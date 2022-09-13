"""
Test config parser
"""
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from featurebyte.config import (
    APIClient,
    Configurations,
    FeatureByteSettings,
    GitSettings,
    LocalStorageSettings,
    LoggingSettings,
)
from featurebyte.exception import InvalidSettingsError
from featurebyte.logger import logger
from featurebyte.models.credential import CredentialType


def test_configurations():
    """
    Test creating configuration from config file
    """
    config = Configurations("tests/fixtures/config_multiple_persistent.yaml")

    # one credential with db source as key
    assert len(config.credentials) == 1
    feature_store_name = "Snowflake FeatureStøre"
    assert config.credentials[feature_store_name].dict() == {
        "name": feature_store_name,
        "credential_type": CredentialType.USERNAME_PASSWORD,
        "credential": {"username": "user", "password": "password"},
    }

    # logging settings
    assert config.logging == LoggingSettings(
        level="INFO",
        serialize=True,
    )

    # git settings
    assert config.git == GitSettings(
        remote_url="git@github.com:featurebyte/playground.git",
        key_path="~/.ssh/id_rsa",
        branch="test",
    )

    # storage settings
    assert config.storage == LocalStorageSettings(local_path="~/.featurebyte_custom/data")

    # other settings
    assert config.settings == {}

    # featurebyte settings
    assert config.featurebyte == FeatureByteSettings(
        api_url="https://app.featurebyte.com/api/v1",
        api_token="API_TOKEN_VALUE",
    )


def test_get_client_no_persistence_settings():
    """
    Test getting client with no persistent settings
    """
    with pytest.raises(InvalidSettingsError) as exc_info:
        Configurations("tests/fixtures/invalid_config.yaml").get_client()
    assert str(exc_info.value) == "Git or FeatureByte API settings must be specified"


def test_get_client_git_persistent_settings():
    """
    Test getting client with git persistent only
    """
    # expect a local fastapi test client
    client = Configurations("tests/fixtures/config_git_persistent.yaml").get_client()
    assert isinstance(client, TestClient)


def test_get_client_featurebyte_persistent_settings__success():
    """
    Test getting client with featurebyte persistent
    """
    # expect a local fastapi test client
    with patch("requests.Session.send") as mock_requests_get:
        mock_requests_get.return_value.status_code = 200
        client = Configurations("tests/fixtures/config_featurebyte_persistent.yaml").get_client()
    assert isinstance(client, APIClient)

    # check api token included in header
    mock_requests_get.assert_called_once()
    assert mock_requests_get.call_args[0][0].headers == {
        "user-agent": "Python SDK",
        "Accept-Encoding": "gzip, deflate",
        "accept": "application/json",
        "Connection": "keep-alive",
        "Authorization": "Bearer API_TOKEN_VALUE",
    }


def test_get_client_featurebyte_persistent_settings__invalid_token():
    """
    Test getting client with featurebyte persistent only with invalid token
    """
    # expect a local fastapi test client
    with pytest.raises(InvalidSettingsError) as exc_info:
        with patch("requests.Session.send") as mock_requests_get:
            mock_requests_get.return_value.status_code = 401
            Configurations("tests/fixtures/config_featurebyte_persistent.yaml").get_client()
    assert str(exc_info.value) == "Authentication failed"


def test_logging_level_change():
    """
    Test logging level is consistent after local logging import in Configurations class
    """
    # pylint: disable=protected-access
    # fix core log level to 10
    logger._core.min_level = 10

    config = Configurations()
    config.logging.level = 20

    # expect logging to adopt logging level specified in the config
    config.get_client()
    assert logger._core.min_level == 20


def test_default_local_storage():
    """
    Test default local storage location if not specified
    """
    config = Configurations("tests/fixtures/config_no_persistent.yaml")
    assert config.storage.local_path == Path("~/.featurebyte/data").expanduser()
