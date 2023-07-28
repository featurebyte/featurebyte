"""
Test config parser
"""
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
import requests.exceptions
from websocket import (
    WebSocketAddressException,
    WebSocketBadStatusException,
    WebSocketConnectionClosedException,
)

from featurebyte.config import (
    DEFAULT_HOME_PATH,
    APIClient,
    Configurations,
    LocalStorageSettings,
    LoggingSettings,
    Profile,
    WebsocketClient,
)
from featurebyte.exception import InvalidSettingsError
from featurebyte.logging import get_logger

logger = get_logger(__name__)


def test_configurations():
    """
    Test creating configuration from config file
    """
    config = Configurations("tests/fixtures/config/config.yaml")

    # logging settings
    assert config.logging == LoggingSettings(
        level="INFO",
    )

    # storage settings
    assert config.storage == LocalStorageSettings(local_path="~/.featurebyte_custom/data")

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


@patch("httpx._client.Client.send")
def test_tls_verify_configurations(mock_requests_get):
    # Mock return status code 200
    mock_requests_get.return_value.status_code = 200

    config = Configurations("tests/fixtures/config/config_certificate.yaml")

    # Certificate verification
    config.use_profile("cert_check")
    assert config.get_client().verify

    # Certificate verification disabled
    config.use_profile("cert_no_check")
    assert config.get_client().verify is False

    # Certificate verification default (enabled)
    config.use_profile("cert_default")
    assert config.get_client().verify


def test_get_client_no_persistence_settings():
    """
    Test getting client with no persistent settings
    """
    config = Configurations("tests/fixtures/config/config_no_profile.yaml")
    with pytest.raises(InvalidSettingsError) as exc_info:
        config.get_client()
    assert (
        str(exc_info.value)
        == 'No valid profile specified. Update config file or specify valid profile name with "use_profile".'
    )


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
    logger.setLevel(10)

    config = Configurations("tests/fixtures/config/config.yaml")
    config.logging.level = 20

    # expect logging to adopt logging level specified in the config
    config.get_client()
    assert logger.level == 20


def test_default_local_storage():
    """
    Test default local storage location if not specified
    """
    config = Configurations("tests/fixtures/config/config_no_profile.yaml")
    assert config.storage.local_path == Path(
        os.environ.get("FEATUREBYTE_HOME", str(DEFAULT_HOME_PATH))
    ).joinpath("data/files")


@patch("httpx._client.Client.send")
def test_use_profile(mock_requests_get):
    """
    Test selecting profile for api service
    """
    mock_requests_get.return_value.status_code = 200

    config = Configurations("tests/fixtures/config/config.yaml")
    assert config.profile.name == "featurebyte1"
    assert config.get_client().base_url == "https://app1.featurebyte.com/api/v1"

    config.use_profile("featurebyte2")
    assert config.profile.name == "featurebyte2"
    assert config.get_client().base_url == "https://app2.featurebyte.com/api/v1"


def test_use_profile_non_existent():
    """
    Test use non-existent profile
    """
    with patch("featurebyte.config.get_home_path") as mock_get_home_path:
        mock_get_home_path.return_value = Path("tests/fixtures/config")
        with pytest.raises(InvalidSettingsError) as exc_info:
            Configurations().use_profile("non-existent-profile")
        assert str(exc_info.value) == "Profile not found: non-existent-profile"


def test_use_profile_invalid_endpoint():
    """
    Test use invalid endpoint profile
    """
    config = Configurations("tests/fixtures/config/config.yaml")
    with patch("featurebyte.config.BaseAPIClient.request") as mock_request:
        mock_request.side_effect = requests.exceptions.ConnectionError()
        with pytest.raises(InvalidSettingsError) as exc_info:
            config.use_profile("invalid")
        assert (
            str(exc_info.value) == "Service endpoint is inaccessible: http://invalid.endpoint:1234"
        )


def test_empty_configuration_file():
    """
    Test creating configuration from empty config file does not fail
    """
    with tempfile.NamedTemporaryFile() as file_handle:
        Configurations(file_handle.name)


@patch("featurebyte.config.Configurations.check_sdk_versions")
def test_client_redirection(mock_check_sdk_versions):
    """
    Test client disallows redirection.
    Redirection can fail for some endpoints as POST requests gets redirected to GET.
    """
    mock_check_sdk_versions.return_value = {"remote sdk": 0.1, "local sdk": 0.1}

    Configurations("tests/fixtures/config/config.yaml").use_profile("featurebyte1")
    # expect 30x response to be returned as is instead of following redirection
    client = Configurations().get_client()
    assert isinstance(client, APIClient)
    with patch("featurebyte.config.BaseAPIClient.request") as mock_request:
        mock_request.return_value.status_code = 301
        response = client.get("/user/me")
        # response should be 301
        assert response.status_code == 301
        mock_request.assert_called_once_with(
            "GET",
            "https://app1.featurebyte.com/api/v1/user/me",
            allow_redirects=False,
            headers={},
        )

        # check api token included in header
        assert client.headers == {
            "user-agent": "Python SDK",
            "Accept-Encoding": "gzip, deflate",
            "accept": "application/json",
            "Connection": "keep-alive",
            "Authorization": "Bearer API_TOKEN_VALUE1",
        }


@pytest.mark.no_mock_websocket_client
def test_websocket_ssl():
    """
    Test websocket with ssl
    """
    config = Configurations("tests/fixtures/config/config.yaml")

    # getting a websocket client with ssl url should not fail
    config.profile.api_url = "https://some_endpoint"
    with pytest.raises(WebSocketAddressException):
        with config.get_websocket_client(str(uuid4())) as ws_client:
            assert isinstance(ws_client, WebsocketClient)


@pytest.mark.no_mock_websocket_client
def test_websocket_reconnect():
    """
    Test websocket reconnection logic
    """
    config = Configurations("tests/fixtures/config/config.yaml")

    with patch("featurebyte.config.websocket.create_connection") as mock_create_connection:
        mock_ws = Mock()
        mock_create_connection.side_effect = lambda *args, **kwargs: mock_ws

        with config.get_websocket_client(str(uuid4())) as ws_client:
            # get some data from websocket
            mock_ws.recv.side_effect = [b"test"]
            data = ws_client.receive_bytes()
            assert data == b"test"

            # get empty data from websocket
            mock_ws.recv.side_effect = [b""]
            data = ws_client.receive_bytes()
            assert data == b""

            # unexpected disconnect of valid connection
            mock_ws.recv.side_effect = [
                WebSocketConnectionClosedException(),
                WebSocketConnectionClosedException(),
                b"test",
            ]
            data = ws_client.receive_bytes()
            assert data == b"test"

            # unexpected disconnect of closed connection
            mock_ws.recv.side_effect = [
                WebSocketConnectionClosedException(),
                WebSocketBadStatusException("Not found", 404),
            ]
            with pytest.raises(WebSocketBadStatusException):
                ws_client.receive_bytes()
