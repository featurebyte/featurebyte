"""
Read configurations from ini file
"""
from typing import Any, Dict, Iterator, List, Optional, Union, cast

import json
import os
from contextlib import contextmanager
from http import HTTPStatus
from pathlib import Path

import requests
import websocket
import yaml
from bson import ObjectId
from pydantic import AnyHttpUrl, BaseModel, Field, validator
from pydantic.error_wrappers import ValidationError
from requests import Response

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.utils import get_version
from featurebyte.enum import StrEnum
from featurebyte.exception import InvalidSettingsError
from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.models.credential import Credential, CredentialType, UsernamePasswordCredential

# data source to credential mapping
Credentials = Dict[str, Optional[Credential]]

# default local location
DEFAULT_HOME_PATH: Path = Path.home().joinpath(".featurebyte")

ACTIVE_CATALOG_ID: ObjectId = DEFAULT_CATALOG_ID


def get_home_path() -> Path:
    """
    Get Featurebyte Home path

    Returns
    -------
    Path
        Featurebyte Home path
    """
    return Path(os.environ.get("FEATUREBYTE_HOME", str(DEFAULT_HOME_PATH)))


def get_active_catalog_id() -> ObjectId:
    """
    Get active catalog id

    Returns
    -------
    ObjectId
    """
    return ACTIVE_CATALOG_ID


def activate_catalog(catalog_id: ObjectId) -> None:
    """
    Set active catalog

    Parameters
    ----------
    catalog_id: ObjectId
        Catalog ID to set as active
    """
    global ACTIVE_CATALOG_ID  # pylint: disable=global-statement
    ACTIVE_CATALOG_ID = catalog_id


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

    level: LogLevel = LogLevel.DEBUG
    serialize: bool = False
    telemetry: bool = True
    telemetry_url: str = "https://log.int.featurebyte.com"


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
            params = kwargs.get("params", {})
            params["catalog_id"] = str(get_active_catalog_id())
            kwargs["params"] = params
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
            url = f"{url}?token={access_token}"
        self._ws = websocket.create_connection(url, enable_multithread=True)

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
        """
        return cast(bytes, self._ws.recv())

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
    FeatureByte SDK settings. Contains general settings, database sources and credentials.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["Configurations"],
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
                "logging:\n"
                "  level: INFO\n"
                "  telemetry: true\n"
                "  telemetry_url: https://log.int.featurebyte.com\n"
            )

        self.storage: LocalStorageSettings = LocalStorageSettings()
        self.profile: Optional[Profile] = None
        self.profiles: Optional[List[Profile]] = None
        self.settings: Dict[str, Any] = {}
        self.credentials: Credentials = {}
        self.logging: LoggingSettings = LoggingSettings()
        self._parse_config(self._config_file_path)

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

        Raises
        ------
        InvalidSettingsError
            Invalid Settings Error
        """
        if not os.path.exists(path):
            return

        with open(path, encoding="utf-8") as file_obj:
            self.settings = yaml.safe_load(file_obj)
            if not self.settings:
                return

        credentials = self.settings.pop("credential", [])
        for credential in credentials:
            name = credential.pop("feature_store", "unnamed")
            try:
                # parse and store credentials
                new_credential = Credential(
                    **{
                        "name": name,
                        "credential_type": credential.get("credential_type", CredentialType.NONE),
                        "credential": credential,
                    },
                )
                self.credentials[name] = new_credential
            except ValidationError as exc:
                raise InvalidSettingsError(
                    f"Invalid settings for feature store: {name}\n{str(exc)}"
                ) from exc

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
            # use first profile as fallback
            self.profile = self.profiles[0]
            selected_profile_name = os.environ.get("FEATUREBYTE_PROFILE")
            if selected_profile_name:
                for profile in self.profiles:
                    if profile.name == selected_profile_name:
                        self.profile = profile
                        break

    @classmethod
    def use_profile(cls, profile_name: str) -> Dict[str, str]:
        """
        Use a service profile specified in the configuration file.

        Parameters
        ----------
        profile_name: str
            Name of profile to use

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
        Content of configuration file at `~/.featurebyte/config.yaml`
        ```
        profile:
          - name: featurebyte
            api_url: https://app.featurebyte.com/api/v1
            api_token: API_TOKEN_VALUE
        ```
        Use service profile `featurebyte`

        >>> import featurebyte as fb
        >>> fb.Configuration.use_profile("featurebyte")  # doctest: +SKIP
        {'remote sdk': '0.1.0.dev368', 'local sdk': '0.1.0.dev368'}
        """
        profile_names = [profile.name for profile in Configurations().profiles or []]
        if profile_name not in profile_names:
            raise InvalidSettingsError(f"Profile not found: {profile_name}")
        os.environ["FEATUREBYTE_PROFILE"] = profile_name
        # test connection
        client = Configurations().get_client()
        response = client.get("/status")
        if response.status_code != HTTPStatus.OK:
            raise InvalidSettingsError(f"Service endpoint is inaccessible: {client.base_url}")
        sdk_version = response.json()["sdk_version"]
        return {"remote sdk": sdk_version, "local sdk": get_version()}

    def get_client(self) -> APIClient:
        """
        Get a client for the configured profile.

        Returns
        -------
        APIClient
            API client

        Raises
        ------
        InvalidSettingsError
            Invalid settings
        """
        # pylint: disable=import-outside-toplevel,cyclic-import
        from featurebyte.logger import configure_logger, logger

        # configure logger
        configure_logger(logger, self)

        if self.profile:
            client = APIClient(api_url=self.profile.api_url, api_token=self.profile.api_token)
        else:
            raise InvalidSettingsError("No profile setting specified")

        return client

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

        Raises
        ------
        InvalidSettingsError
            Invalid settings
        """
        if self.profile:
            url = self.profile.api_url.replace("http://", "ws://")
            url = f"{url}/ws/{task_id}"
            websocket_client = WebsocketClient(url=url, access_token=self.profile.api_token)
            try:
                yield websocket_client
            finally:
                websocket_client.close()
        else:
            raise InvalidSettingsError("No profile setting specified")

    def write_creds(self, credential: Credential, feature_store_name: str) -> bool:
        """
        Write creds will try to write the credentials to the configuration file. This will no-op if any other
        credential's already exist in the config file.

        Parameters
        ----------
        credential: Credential
            credentials to write to the file
        feature_store_name: str
            the feature store associated with the credentials being passed in

        Returns
        -------
        bool
            True if we updated the config file, False if otherwise
        """
        if len(self.credentials) > 0:
            return False

        # Append text to file
        with self._config_file_path.open(mode="a", encoding="utf-8") as config_file:
            username_pw_cred = credential.credential
            assert isinstance(username_pw_cred, UsernamePasswordCredential)
            config_file.write(
                "\n\n# credentials\n"
                "credential:\n"
                f"  - feature_store: {feature_store_name}\n"
                f"    credential_type: {credential.credential_type}\n"
                f"    username: {username_pw_cred.username}\n"
                f"    password: {username_pw_cred.password}\n"
            )
        return True
