"""
Read configurations from ini file
"""
from typing import Any, Dict, Iterator, List, Optional, Union, cast

import json
import os
import time
from contextlib import contextmanager
from http import HTTPStatus
from pathlib import Path

import requests
import websocket
import yaml
from pydantic import AnyHttpUrl, BaseModel, Field, validator
from requests import Response
from websocket import WebSocketConnectionClosedException

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.utils import get_version
from featurebyte.enum import StrEnum
from featurebyte.exception import InvalidSettingsError
from featurebyte.models.base import get_active_catalog_id

# default local location
DEFAULT_HOME_PATH: Path = Path.home().joinpath(".featurebyte")


def get_home_path() -> Path:
    """
    Get Featurebyte Home path

    Returns
    -------
    Path
        Featurebyte Home path
    """
    return Path(os.environ.get("FEATUREBYTE_HOME", str(DEFAULT_HOME_PATH)))


class LogLevel(StrEnum):
    """
    Log levels
    """

    CRITICAL = "CRITICAL"
    FATAL = "FATAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    WARN = "WARN"
    INFO = "INFO"
    DEBUG = "DEBUG"
    NOTSET = "NOTSET"


class LoggingSettings(BaseModel):
    """
    Settings for logging
    """

    level: LogLevel = LogLevel.INFO


class LocalStorageSettings(BaseModel):
    """
    Settings for local file storage
    """

    local_path: Path = Field(default_factory=lambda: get_home_path().joinpath("data/files"))

    @validator("local_path")
    @classmethod
    def expand_path(cls, value: Path) -> Path:
        """
        Expand path

        Parameters
        ----------
        value: Path
            Path to be expanded

        Returns
        -------
        Path
            Expanded path
        """
        return value.expanduser()


class Profile(BaseModel):
    """
    Settings for FeatureByte Application API access
    """

    name: str
    api_url: AnyHttpUrl
    api_token: Optional[str]


class ProfileList(BaseModel):
    """
    List of Profile entries
    """

    profiles: List[Profile]


class BaseAPIClient(requests.Session):
    """
    Base Http client to facilitate testing
    """


class APIClient(BaseAPIClient):
    """
    Http client for accessing the FeatureByte Application API
    """

    def __init__(self, api_url: str, api_token: Optional[str]) -> None:
        """
        Initialize api settings

        Parameters
        ----------
        api_url: str
            URL of FeatureByte API service
        api_token: Optional[str]
            Access token to used for authentication
        """
        super().__init__()
        self.base_url = api_url
        additional_headers = {
            "user-agent": "Python SDK",
            "accept": "application/json",
        }
        if api_token:
            additional_headers["Authorization"] = f"Bearer {api_token}"
        self.headers.update(additional_headers)

    def request(
        self,
        method: Union[str, bytes],
        url: Union[str, bytes],
        *args: Any,
        **kwargs: Any,
    ) -> Response:
        """
        Make http request
        Parameters
        ----------
        method: str
            Request method (GET, POST, PATCH, DELETE)
        url: Union[str, bytes]
            URL to make request to
        *args: Any
            Additional positional arguments
        **kwargs: Any
            Additional keyword arguments

        Returns
        -------
        Response
            HTTP Response

        Raises
        ------
        InvalidSettingsError
            Invalid service endpoint
        """
        try:
            headers = kwargs.get("headers", {})
            active_catalog_id = headers.get("active-catalog-id", get_active_catalog_id())
            if active_catalog_id:
                headers["active-catalog-id"] = str(active_catalog_id)
            kwargs["headers"] = headers
            kwargs["allow_redirects"] = False
            return super().request(
                method,
                self.base_url + str(url),
                *args,
                **kwargs,
            )
        except requests.exceptions.ConnectionError:
            raise InvalidSettingsError(
                f"Service endpoint is inaccessible: {self.base_url}"
            ) from None


class WebsocketClient:
    """
    Websocket client for accessing the FeatureByte Application API
    """

    def __init__(self, url: str, access_token: Optional[str]) -> None:
        """
        Initialize api settings

        Parameters
        ----------
        url: str
            URL of FeatureByte websocket service
        access_token: Optional[str]
            Access token to used for authentication
        """
        if access_token:
            self._url = f"{url}?token={access_token}"
        else:
            self._url = url
        self._reconnect()

    def _reconnect(self) -> None:
        """
        Reconnect websocket connection
        """
        self._ws = websocket.create_connection(self._url, enable_multithread=True)

    def close(self) -> None:
        """
        Close websocket connection
        """
        self._ws.close()

    def receive_bytes(self) -> bytes:
        """
        Receive message from websocket server as bytes

        Returns
        -------
        bytes
            Message

        Raises
        ------
        TimeoutError
            No message received from websocket server
        """
        start_time = time.time()
        # maintain connection for up to 3 minutes without receiving any new message
        while (time.time() - start_time) < 180:
            try:
                return cast(bytes, self._ws.recv())
            except WebSocketConnectionClosedException:
                # reconnect on unexpected connection close
                self._reconnect()
        raise TimeoutError("No message received from websocket server")

    def receive_json(self) -> Optional[Dict[str, Any]]:
        """
        Receive message from websocket server as json

        Returns
        -------
        Dict[str, Any]
            Message as json dictionary
        """
        message_bytes = self.receive_bytes()
        if message_bytes:
            return cast(Dict[str, Any], json.loads(message_bytes.decode("utf8")))
        return None


class Configurations:
    """
    FeatureByte SDK settings. Contains general settings.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        proxy_class="featurebyte.Configurations",
        skipped_members=["get_client"],
    )

    def __init__(self, config_file_path: Optional[str] = None) -> None:
        """
        Load and parse configurations

        Parameters
        ----------
        config_file_path: str | None
            Path to read configurations from
        """
        home_path = get_home_path()
        self._config_file_path = (
            Path(config_file_path) if config_file_path else home_path.joinpath("config.yaml")
        )

        # create config file if it does not exist
        if not self._config_file_path.exists():
            self._config_file_path.parent.mkdir(parents=True, exist_ok=True)
            self._config_file_path.write_text(
                "# featurebyte configuration\n\n"
                "profile:\n"
                "  - name: local\n"
                "    api_url: http://127.0.0.1:8088\n\n"
                "default_profile: local\n\n"
            )

        self.storage: LocalStorageSettings = LocalStorageSettings()
        self._profile: Optional[Profile] = None
        self.profiles: Optional[List[Profile]] = None
        self.settings: Dict[str, Any] = {}
        self.logging: LoggingSettings = LoggingSettings()
        self._parse_config(self._config_file_path)

    @property
    def profile(self) -> Profile:
        """
        Get active profile

        Returns
        -------
        Profile
            Active profile

        Raises
        ------
        InvalidSettingsError
            No valid profile specified
        """
        if not self._profile:
            raise InvalidSettingsError(
                'No valid profile specified. Update config file or specify valid profile name with "use_profile".'
            )
        return self._profile

    @property
    def config_file_path(self) -> Path:
        """
        Config file path

        Returns
        -------
        Path
            path to config file
        """
        return self._config_file_path

    def _parse_config(self, path: Path) -> None:
        """
        Parse configurations file

        Parameters
        ----------
        path: Path
            Path to read configurations from
        """
        if not os.path.exists(path):
            return

        with open(path, encoding="utf-8") as file_obj:
            self.settings = yaml.safe_load(file_obj)
            if not self.settings:
                return

        logging_settings = self.settings.pop("logging", None)
        if logging_settings:
            # parse logging settings
            self.logging = LoggingSettings(**logging_settings)

        storage_settings = self.settings.pop("storage", None)
        if storage_settings:
            # parse storage settings
            self.storage = LocalStorageSettings(**storage_settings)

        profile_settings = self.settings.pop("profile", None)
        if profile_settings:
            # parse profile settings
            self.profiles = ProfileList(profiles=profile_settings).profiles

        if self.profiles:
            profile_map = {profile.name: profile for profile in self.profiles}
            default_profile = self.settings.pop("default_profile", None)
            selected_profile_name = os.environ.get("FEATUREBYTE_PROFILE")
            if selected_profile_name:
                self._profile = profile_map.get(selected_profile_name)
            else:
                self._profile = profile_map.get(default_profile)

    @classmethod
    def check_sdk_versions(cls) -> Dict[str, str]:
        """
        Check SDK versions of client and server for the active profile

        Returns
        -------
        Dict[str, str]
            Remote and local SDK versions

        Raises
        ------
        InvalidSettingsError
            Invalid service endpoint

        Examples
        --------
        >>> Configurations.check_sdk_versions()  # doctest: +SKIP
        {'remote sdk': '0.1.0.dev368', 'local sdk': '0.1.0.dev368'}
        """
        client = Configurations().get_client()
        response = client.get("/status")
        if response.status_code != HTTPStatus.OK:
            raise InvalidSettingsError(f"Service endpoint is inaccessible: {client.base_url}")
        sdk_version = response.json()["sdk_version"]
        return {"remote sdk": sdk_version, "local sdk": get_version()}

    @classmethod
    def use_profile(cls, profile_name: str) -> None:
        """
        Use a service profile specified in the configuration file.

        Parameters
        ----------
        profile_name: str
            Name of profile to use

        Raises
        ------
        InvalidSettingsError
            Invalid service endpoint

        Examples
        --------
        Content of configuration file at `~/.featurebyte/config.yaml`
        ```
        profile:
          - name: local
            api_url: https://app.featurebyte.com/api/v1
            api_token: API_TOKEN_VALUE

        default_profile: local
        ```
        Use service profile `local`

        >>> fb.Configurations().use_profile("local")
        """
        profile_names = [profile.name for profile in Configurations().profiles or []]
        if profile_name not in profile_names:
            raise InvalidSettingsError(f"Profile not found: {profile_name}")

        # test connection
        current_profile_name = os.environ.get("FEATUREBYTE_PROFILE")
        try:
            os.environ["FEATUREBYTE_PROFILE"] = profile_name
            cls.check_sdk_versions()
        except InvalidSettingsError:
            # restore previous profile
            if current_profile_name:
                os.environ["FEATUREBYTE_PROFILE"] = current_profile_name
            else:
                os.environ.pop("FEATUREBYTE_PROFILE", None)
            raise

    def get_client(self) -> APIClient:
        """
        Get a client for the configured profile.

        Returns
        -------
        APIClient
            API client
        """
        # pylint: disable=import-outside-toplevel,cyclic-import
        from featurebyte.logging import reconfigure_loggers

        # configure logger
        reconfigure_loggers(self)
        return APIClient(api_url=self.profile.api_url, api_token=self.profile.api_token)

    @contextmanager
    def get_websocket_client(self, task_id: str) -> Iterator[WebsocketClient]:
        """
        Get websocket client for the configured profile.

        Parameters
        ----------
        task_id: str
            Task ID

        Yields
        -------
        WebsocketClient
            Websocket client
        """
        url = self.profile.api_url.replace("http://", "ws://").replace("https://", "wss://")
        url = f"{url}/ws/{task_id}"
        websocket_client = WebsocketClient(url=url, access_token=self.profile.api_token)
        try:
            yield websocket_client
        finally:
            websocket_client.close()
