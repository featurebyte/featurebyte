"""
Test config parser
"""
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from featurebyte.config import (
    APIClient,
    Configurations,
    FeatureByteSettings,
    GitSettings,
    LoggingSettings,
)
from featurebyte.enum import SourceType
from featurebyte.exception import InvalidSettingsError
from featurebyte.models.credential import CredentialType


def test_configurations():
    """
    Test creating configuration from config file
    """
    config = Configurations("tests/fixtures/config_multiple_persistent.yaml")

    # one database source with section name as key
    assert len(config.feature_stores) == 1
    expected_feature_store_dict = {
        "type": SourceType.SNOWFLAKE,
        "details": {
            "account": "sf_account",
            "warehouse": "COMPUTE_WH",
            "database": "FEATUREBYTE",
            "sf_schema": "FEATUREBYTE",
        },
    }
    feature_store = config.feature_stores["Snowflake FeatureStøre"]
    assert feature_store.dict() == expected_feature_store_dict

    # one credential with db source as key
    assert len(config.credentials) == 1
    assert config.credentials[feature_store].dict() == {
        "name": "Snowflake FeatureStøre",
        "feature_store": expected_feature_store_dict,
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

    # other settings
    assert config.settings == {}

    # featurebyte settings
    assert config.featurebyte == FeatureByteSettings(
        api_url="https://app.featurebyte.com/api/v1",
        api_token="API_TOKEN_VALUE",
    )


def test_configurations_malformed_datasource():
    """
    Test creating configuration from malformed config file
    """
    with pytest.raises(InvalidSettingsError) as exc_info:
        Configurations("tests/fixtures/config_malformed.yaml")
    assert str(exc_info.value) == "Invalid settings for datasource: Snowflake FeatureStøre"


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
    with patch("requests.request") as mock_requests_get:
        mock_requests_get.return_value.status_code = 200
        client = Configurations("tests/fixtures/config_featurebyte_persistent.yaml").get_client()
    assert isinstance(client, APIClient)

    # check api token included in header
    mock_requests_get.assert_called_once_with(
        "GET",
        "https://app.featurebyte.com/api/v1/user/me",
        headers={"accept": "application/json", "Authorization": "Bearer API_TOKEN_VALUE"},
    )


def test_get_client_featurebyte_persistent_settings__invalid_token():
    """
    Test getting client with featurebyte persistent only with invalid token
    """
    # expect a local fastapi test client
    with pytest.raises(InvalidSettingsError) as exc_info:
        with patch("requests.request") as mock_requests_get:
            mock_requests_get.return_value.status_code = 401
            Configurations("tests/fixtures/config_featurebyte_persistent.yaml").get_client()
    assert str(exc_info.value) == "Authentication failed"
