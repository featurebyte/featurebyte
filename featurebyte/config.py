"""
Read configurations from ini file
"""

import json
import os
import ssl
import time
from contextlib import contextmanager
from http import HTTPStatus
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union, cast

import requests
import websocket
import yaml
from pydantic import AnyHttpUrl, BaseModel, Field, field_serializer, field_validator
from requests import Response
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from websocket import WebSocketConnectionClosedException

from featurebyte.common import activate_catalog, get_active_catalog_id
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.utils import get_version, is_server_mode
from featurebyte.enum import StrEnum
from featurebyte.exception import InvalidSettingsError

# http request settings
HTTP_REQUEST_TIMEOUT: int = 180
HTTP_REQUEST_MAX_RETRIES: int = 3
HTTP_REQUEST_BACKOFF_FACTOR: float = 0.3

# feature requests row limits
FEATURE_PREVIEW_ROW_LIMIT: int = 50
ONLINE_FEATURE_REQUEST_ROW_LIMIT: int = 50


def get_home_path() -> Path:
    """
    Get Featurebyte Home path

    Returns
    -------
    Path
        Featurebyte Home path
    """
    default_home_path: Path = Path.home()
    try:
        # check if we are in DataBricks environment and valid secrets are present create a profile automatically
        from databricks.sdk.runtime import dbutils

        db_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()  # type: ignore
        default_home_path = Path(f"/Workspace/Users/{db_user}")
    except (ModuleNotFoundError, ImportError, ValueError, AttributeError):
        pass
    return Path(os.environ.get("FEATUREBYTE_HOME", str(default_home_path.joinpath(".featurebyte"))))


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

    @field_validator("local_path")
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
    api_token: Optional[str] = Field(default=None)
    ssl_verify: bool = Field(default=True)

    @field_serializer("api_url")
    def _serialize_api_url(self, system: AnyHttpUrl) -> str:
        # In pydantic V2, trailing slash is included in the url (https://github.com/pydantic/pydantic/issues/7186).
        # This is a workaround to remove the trailing slash.
        return str(system).rstrip("/")


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

    def __init__(self, api_url: str, api_token: Optional[str], ssl_verify: bool = True) -> None:
        """
        Initialize api settings

        Parameters
        ----------
        api_url: str
            URL of FeatureByte API service
        api_token: Optional[str]
            Access token to used for authentication
        ssl_verify: bool
            Flag to specify whether to verify SSL certificates
        """
        super().__init__()
        self.base_url = api_url
        self.verify = ssl_verify  # To Ignore SSL Verification
        additional_headers = {
            "user-agent": "Python SDK",
            "accept": "application/json",
        }
        if api_token:
            additional_headers["Authorization"] = f"Bearer {api_token}"

        self.headers.update(additional_headers)

        # add retry adapter
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=HTTP_REQUEST_MAX_RETRIES,
                read=HTTP_REQUEST_MAX_RETRIES,
                connect=HTTP_REQUEST_MAX_RETRIES,
                backoff_factor=HTTP_REQUEST_BACKOFF_FACTOR,
                status_forcelist=(),
            )
        )
        self.mount("http://", adapter)
        self.mount("https://", adapter)

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

            kwargs["timeout"] = HTTP_REQUEST_TIMEOUT
            kwargs["headers"] = headers
            kwargs["allow_redirects"] = False
            full_url = str(self.base_url).rstrip("/") + str(url)
            return super().request(
                method,
                full_url,
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

    def __init__(self, url: str, access_token: Optional[str], ssl_verify: bool) -> None:
        """
        Initialize api settings

        Parameters
        ----------
        url: str
            URL of FeatureByte websocket service
        access_token: Optional[str]
            Access token to used for authentication
        ssl_verify: bool
            Flag to specify whether to verify SSL certificates
        """
        if access_token:
            self._url = f"{url}?token={access_token}"
        else:
            self._url = url
        self.verify = ssl_verify
        self._reconnect()

    def _reconnect(self) -> None:
        """
        Reconnect websocket connection
        """
        self._ws = websocket.create_connection(
            self._url, enable_multithread=True, sslopt=self._get_sslopt()
        )

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

    def _get_sslopt(self) -> Dict[str, Any]:
        """
        Return ssl options for websocket connection

        Returns
        -------
        Dict[str, Any]
            SSL options
        """
        if self.verify:
            return {}
        return {"cert_reqs": ssl.CERT_NONE}


class Configurations:
    """
    FeatureByte SDK settings. Contains general settings.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        proxy_class="featurebyte.Configurations",
        skipped_members=["get_client"],
    )

    _instance = None

    def __new__(cls, *args: Any, **kwargs: Any) -> "Configurations":
        if Configurations._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config_file_path: Optional[str] = None, force: bool = False) -> None:
        """
        Load and parse configurations

        Parameters
        ----------
        config_file_path: str | None
            Path to read configurations from
            this will force a reload of configuration if specified
        force: bool
            Force reload configurations
        """
        # Singleton initialization
        # force is set
        # config_file_path is set
        if not hasattr(self, "_config_file_path") or force or config_file_path is not None:
            # Default values set
            self._config_file_path: Path = (
                Path(config_file_path)
                if config_file_path
                else get_home_path().joinpath("config.yaml")
            )
            self.storage: LocalStorageSettings = LocalStorageSettings()
            self.default_profile_name: Optional[str] = None
            self._profile: Optional[Profile] = None
            self.profiles: List[Profile] = []
            self.logging: LoggingSettings = LoggingSettings()

            # create config file if it does not exist
            if not self._config_file_path.exists():
                self._config_file_path.parent.mkdir(parents=True, exist_ok=True)
                self._config_file_path.write_text(
                    "# featurebyte configuration\n\n"
                    "profile:\n"
                    "  - name: local\n"
                    "    ssl_verify: true\n"
                    "    api_url: http://127.0.0.1:8088\n\n"
                    "default_profile: local\n\n"
                )
            self._parse_config(self._config_file_path)
        else:
            pass  # do nothing

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
            settings = yaml.safe_load(file_obj) or {}

        logging_settings = settings.pop("logging", None)
        if logging_settings:
            # parse logging settings
            self.logging = LoggingSettings(**logging_settings)

        storage_settings = settings.pop("storage", None)
        if storage_settings:
            # parse storage settings
            self.storage = LocalStorageSettings(**storage_settings)

        profile_settings = settings.pop("profile", None)
        if profile_settings:
            # parse profile settings
            self.profiles = ProfileList(profiles=profile_settings).profiles

            # Set default _profile if specified
            profile_map = {profile.name: profile for profile in self.profiles}
            self.default_profile_name = settings.pop("default_profile", None)
            if self.default_profile_name:
                self._profile = profile_map.get(self.default_profile_name)

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

    def use_profile(self, profile_name: str) -> None:
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
            ssl_verify: true

        default_profile: local
        ```
        Use service profile `local`

        >>> fb.Configurations().use_profile("local")
        """
        activate_catalog(None)
        new_profile: Optional[Profile] = None
        for prof in self.profiles:
            if prof.name == profile_name:
                new_profile = prof
                break
        if new_profile is None:
            raise InvalidSettingsError(f"Profile not found: {profile_name}")

        # test connection
        old_profile = self._profile
        try:
            self._profile = new_profile
            self.check_sdk_versions()
        except InvalidSettingsError:
            # restore previous profile
            self._profile = old_profile
            raise

    def get_client(self) -> APIClient:
        """
        Get a client for the configured profile.

        Returns
        -------
        APIClient
            API client
        """
        if not is_server_mode():
            from featurebyte.logging import configure_featurebyte_logger

            # configure logger
            configure_featurebyte_logger(self)

        return APIClient(
            api_url=str(self.profile.api_url),
            api_token=self.profile.api_token,
            ssl_verify=self.profile.ssl_verify,
        )

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
        url = str(self.profile.api_url).replace("http://", "ws://").replace("https://", "wss://")
        url = url.rstrip("/")
        url = f"{url}/ws/{task_id}"
        websocket_client = WebsocketClient(
            url=url, access_token=self.profile.api_token, ssl_verify=self.profile.ssl_verify
        )
        try:
            yield websocket_client
        finally:
            websocket_client.close()
