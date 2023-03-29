"""
Test config parser
"""
import os
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import requests.exceptions

from featurebyte.config import (
    DEFAULT_HOME_PATH,
    APIClient,
    Configurations,
    LocalStorageSettings,
    LoggingSettings,
    Profile,
)
from featurebyte.exception import InvalidSettingsError
from featurebyte.logger import logger
from featurebyte.models.credential import (
    Credential,
    CredentialType,
    StorageCredentialType,
    UsernamePasswordCredential,
)


def test_configurations():
    """
    Test creating configuration from config file
    """
    config = Configurations("tests/fixtures/config/config.yaml")

    # one credential with db source as key
    assert len(config.credentials) == 1
    feature_store_name = "Snowflake FeatureStøre"
    assert config.credentials[feature_store_name].dict() == {
        "name": feature_store_name,
        "credential_type": CredentialType.USERNAME_PASSWORD,
        "credential": {
            "username": "user",
            "password": "password",
            "storage_credential_type": StorageCredentialType.NONE,
            "storage_credential": {},
        },
    }

    # logging settings
    assert config.logging == LoggingSettings(
        level="INFO",
        serialize=True,
    )

    # storage settings
    assert config.storage == LocalStorageSettings(local_path="~/.featurebyte_custom/data")

    # other settings
    assert config.settings == {}

    # featurebyte settings
    assert config.profiles == [
        Profile(
            name="featurebyte1",
            api_url="https://app1.featurebyte.com/api/v1",
            api_token="API_TOKEN_VALUE1",
        ),
        Profile(
            name="featurebyte2",
            api_url="https://app2.featurebyte.com/api/v1",
            api_token="API_TOKEN_VALUE2",
        ),
        Profile(name="invalid", api_url="http://invalid.endpoint:1234"),
    ]


def test_configurations_telemetry():
    """
    Test creating configuration from config file
    """
    empty_config = Configurations("tests/fixtures/config/empty.yaml")
    config = Configurations("tests/fixtures/config/config_telemetry.yaml")

    # No configuration testing
    assert not empty_config.logging.telemetry
    assert empty_config.logging.telemetry_url == "https://log.int.featurebyte.com"

    assert not config.logging.telemetry
    assert config.logging.telemetry_url == "https://telemetry.featurebyte.com"


def test_get_client_no_persistence_settings():
    """
    Test getting client with no persistent settings
    """
    with pytest.raises(InvalidSettingsError) as exc_info:
        Configurations("tests/fixtures/config/invalid_config.yaml").get_client()
    assert str(exc_info.value) == "No profile setting specified"


def test_get_client__success():
    """
    Test getting client
    """
    # expect a local fastapi test client
    client = Configurations("tests/fixtures/config/config.yaml").get_client()
    with patch.object(client, "request") as mock_request:
        mock_request.return_value.status_code = 200
        client.get("/user/me")
        assert isinstance(client, APIClient)
        mock_request.assert_called_once_with("GET", "/user/me", allow_redirects=True)

        # check api token included in header
        assert client.headers == {
            "user-agent": "Python SDK",
            "Accept-Encoding": "gzip, deflate",
            "accept": "application/json",
            "Connection": "keep-alive",
            "Authorization": "Bearer API_TOKEN_VALUE1",
        }


def test_logging_level_change():
    """
    Test logging level is consistent after local logging import in Configurations class
    """
    # pylint: disable=protected-access
    # fix core log level to 10
    logger._core.min_level = 10

    config = Configurations("tests/fixtures/config/config.yaml")
    config.logging.level = 20

    # expect logging to adopt logging level specified in the config
    config.get_client()
    assert logger._core.min_level == 20


def test_default_local_storage():
    """
    Test default local storage location if not specified
    """
    config = Configurations("tests/fixtures/config/config_no_profile.yaml")
    assert config.storage.local_path == Path(
        os.environ.get("FEATUREBYTE_HOME", str(DEFAULT_HOME_PATH))
    ).joinpath("data/files")


@patch("requests.Session.send")
def test_use_profile(mock_requests_get):
    """
    Test selecting profile for api service
    """
    mock_requests_get.return_value.status_code = 200
    with patch("featurebyte.config.get_home_path") as mock_get_home_path:
        mock_get_home_path.return_value = Path("tests/fixtures/config")
        config = Configurations()
        assert config.profile.name == "featurebyte1"
        assert config.get_client().base_url == "https://app1.featurebyte.com/api/v1"

        Configurations.use_profile("featurebyte2")
        config = Configurations()
        assert config.profile.name == "featurebyte2"
        assert config.get_client().base_url == "https://app2.featurebyte.com/api/v1"


def test_use_profile_non_existent():
    """
    Test use non-existent profile
    """
    with patch("featurebyte.config.get_home_path") as mock_get_home_path:
        mock_get_home_path.return_value = Path("tests/fixtures/config")
        with pytest.raises(InvalidSettingsError) as exc_info:
            Configurations.use_profile("non-existent-profile")
        assert str(exc_info.value) == "Profile not found: non-existent-profile"


def test_use_profile_invalid_endpoint():
    """
    Test use non-existent profile
    """
    with patch("featurebyte.config.get_home_path") as mock_get_home_path:
        mock_get_home_path.return_value = Path("tests/fixtures/config")
        with patch("featurebyte.config.BaseAPIClient.request") as mock_request:
            mock_request.side_effect = requests.exceptions.ConnectionError()
            with pytest.raises(InvalidSettingsError) as exc_info:
                Configurations.use_profile("invalid")
            assert (
                str(exc_info.value)
                == "Service endpoint is inaccessible: http://invalid.endpoint:1234"
            )


def test_write_creds__no_update_if_creds_exist():
    """
    Test write_creds function - no update expected if credentials exist in file
    """
    config = Configurations("tests/fixtures/config/config.yaml")
    assert len(config.credentials) == 1
    feature_store_name = "Snowflake FeatureStøre"
    cred = Credential(
        name="random",
        credential_type=CredentialType.USERNAME_PASSWORD,
        credential=UsernamePasswordCredential(
            username="random_username",
            password="random_password",
        ),
    )
    did_update = config.write_creds(cred, feature_store_name)
    assert not did_update


def test_write_creds__creds_for_other_feature_store_exists():
    """
    Test write_creds function - no update when creds for another feature store exists.
    """
    with tempfile.NamedTemporaryFile(mode="w") as file_handle:
        config_with_existing_feature_store = "tests/fixtures/config/config.yaml"
        config_file_name = file_handle.name
        shutil.copy2(config_with_existing_feature_store, config_file_name)

        config = Configurations(config_file_name)
        # Verify that credentials for another feature store exists
        assert len(config.credentials) == 1

        # Try to write these creds
        other_feature_store_name = "other feature store"
        cred = Credential(
            name=other_feature_store_name,
            credential_type=CredentialType.USERNAME_PASSWORD,
            credential=UsernamePasswordCredential(
                username="random_username",
                password="random_password",
            ),
        )
        did_update = config.write_creds(cred, other_feature_store_name)
        assert not did_update

        # Verify that the old credentials are still there
        new_config = Configurations(config_file_name)
        assert len(new_config.credentials) == 1


def test_write_creds__update_if_no_creds_exist():
    """
    Test write_creds function - no update expected if credentials exist in file
    """
    with tempfile.NamedTemporaryFile(mode="w") as file_handle:
        config_no_profile_file_name = "tests/fixtures/config/config_no_profile.yaml"
        config_file_name = file_handle.name
        shutil.copy2(config_no_profile_file_name, config_file_name)
        config = Configurations(config_file_name)
        # Assert no credentials exist
        assert len(config.credentials) == 0
        # Assert basic logging config exists
        initial_logging_settings = LoggingSettings(
            level="INFO",
            serialize=True,
        )
        assert config.logging == initial_logging_settings

        # Write creds to file
        feature_store_name = "random_feature_store_name"
        cred = Credential(
            name=feature_store_name,
            credential_type=CredentialType.USERNAME_PASSWORD,
            credential=UsernamePasswordCredential(
                username="random_username",
                password="random_password",
            ),
        )
        did_update = config.write_creds(cred, feature_store_name)
        assert did_update

        # Reload config
        new_config = Configurations(config_file_name)
        # Assert credentials exist
        assert len(new_config.credentials) == 1
        loaded_creds = new_config.credentials.get(feature_store_name)
        assert loaded_creds == cred
        # Assert existing configs are still there
        assert new_config.logging == initial_logging_settings


def test_empty_configuration_file():
    """
    Test creating configuration from empty config file does not fail
    """
    with tempfile.NamedTemporaryFile() as file_handle:
        Configurations(file_handle.name)
