"""
Read configurations from ini file
"""
from typing import Any, Dict, List, Optional, Union

import os
from pathlib import Path

# pylint: disable=too-few-public-methods
import requests
import yaml
from pydantic import AnyHttpUrl, BaseModel, Field, validator
from pydantic.error_wrappers import ValidationError
from requests import Response

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import StrEnum
from featurebyte.exception import InvalidSettingsError
from featurebyte.models.credential import Credential

# data source to credential mapping
Credentials = Dict[str, Optional[Credential]]

# default local location
DEFAULT_HOME_PATH = Path.home().joinpath(".featurebyte")


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

    level: LogLevel = LogLevel.DEBUG
    serialize: bool = False


class LocalStorageSettings(BaseModel):
    """
    Settings for local file storage
    """

    local_path: Path = Field(default_factory=lambda: get_home_path().joinpath("data"))

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


class APIClient(requests.Session):
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
            API token to used for authentication
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
        """
        return super().request(
            method,
            self.base_url + str(url),
            *args,
            **kwargs,
        )


class Configurations:
    """
    FeatureByte SDK settings. Contains general settings, database sources and credentials.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["Configurations"],
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
        if not self._config_file_path.exists() and home_path == DEFAULT_HOME_PATH:
            self._config_file_path.parent.mkdir(parents=True, exist_ok=True)
            self._config_file_path.write_text(
                "# featurebyte configuration\n\n"
                "profile:\n"
                "  - name: local\n"
                "    api_url: http://localhost:8088\n\n"
                "logging:\n"
                "  level: INFO\n"
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

        credentials = self.settings.pop("credential", [])
        for credential in credentials:
            name = credential.pop("feature_store", "unnamed")
            try:
                # parse and store credentials
                try:
                    new_credential = Credential(
                        name=name,
                        credential_type=credential["credential_type"],
                        credential=credential,
                    )
                except KeyError:
                    new_credential = None
                self.credentials[name] = new_credential
            except ValidationError as exc:
                raise InvalidSettingsError(f"Invalid settings for feature store: {name}") from exc

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
    def use_profile(cls, profile_name: str) -> None:
        """
        Use a profile

        Parameters
        ----------
        profile_name: str
            Name of profile to use
        """
        os.environ["FEATUREBYTE_PROFILE"] = profile_name

    def get_client(self) -> APIClient:
        """
        Retrieve API client

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
