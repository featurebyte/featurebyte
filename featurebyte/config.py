"""
Read configurations from ini file
"""
from typing import Any, Dict, List, Optional, Pattern, Union

import os
import re
from enum import Enum
from pathlib import Path

# pylint: disable=too-few-public-methods
import requests
import yaml
from fastapi.testclient import TestClient
from pydantic import BaseModel, ConstrainedStr, Field, HttpUrl, validator
from pydantic.error_wrappers import ValidationError
from requests import Response

from featurebyte.exception import InvalidSettingsError
from featurebyte.models.credential import Credential

# data source to credential mapping
Credentials = Dict[str, Optional[Credential]]

# default local location
DEFAULT_LOCAL_PATH = Path.home().joinpath(".featurebyte")
DEFAULT_CONFIG_PATH = DEFAULT_LOCAL_PATH.joinpath("config.yaml")


class LogLevel(str, Enum):
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


class GitRepoUrl(ConstrainedStr):
    """
    Git repo string
    """

    regex: Optional[Pattern[str]] = re.compile(r"^(.*\.git|file:///.*)$")


class GitSettings(BaseModel):
    """
    Settings for git access
    """

    remote_url: GitRepoUrl
    key_path: Optional[Path]
    branch: str


class LocalStorageSettings(BaseModel):
    """
    Settings for local file storage
    """

    local_path: Path = Field(default=Path(os.path.join(DEFAULT_LOCAL_PATH, "data")))

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
    api_url: HttpUrl
    api_token: str


class ProfileList(BaseModel):
    """
    List of Profile entries
    """

    profiles: List[Profile]


class APIClient(requests.Session):
    """
    Http client for accessing the FeatureByte Application API
    """

    def __init__(self, api_url: str, api_token: str) -> None:
        """
        Initialize api settings

        Parameters
        ----------
        api_url: str
            URL of FeatureByte API service
        api_token: str
            API token to used for authentication
        """
        super().__init__()
        self.base_url = api_url
        self.headers.update(
            {
                "user-agent": "Python SDK",
                "accept": "application/json",
                "Authorization": f"Bearer {api_token}",
            }
        )

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

    def __init__(self, config_file_path: Optional[str] = None) -> None:
        """
        Load and parse configurations

        Parameters
        ----------
        config_file_path: str | None
            Path to read configurations from
        """
        self._config_file_path = Path(
            str(
                config_file_path
                or os.environ.get("FEATUREBYTE_CONFIG_PATH", str(DEFAULT_CONFIG_PATH))
            )
        )

        # create config file if it does not exist
        if not self._config_file_path.exists() and self._config_file_path == DEFAULT_CONFIG_PATH:
            self._config_file_path.parent.mkdir(parents=True, exist_ok=True)
            self._config_file_path.write_text(
                "# featurebyte configurations\n\nlogging:\n  level: INFO\n"
            )

        self.git: Optional[GitSettings] = None
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

        feature_stores = self.settings.pop("featurestore", [])
        for feature_store_details in feature_stores:
            name = feature_store_details.pop("name", "unnamed")
            try:
                # parse and store credentials
                credentials = None
                if "credential_type" in feature_store_details:
                    # credentials are stored together with feature store name in the config file,
                    # Credential pydantic model will use only the relevant fields
                    credentials = Credential(
                        name=name,
                        credential_type=feature_store_details["credential_type"],
                        credential=feature_store_details,
                    )
                self.credentials[name] = credentials
            except ValidationError as exc:
                raise InvalidSettingsError(f"Invalid settings for datasource: {name}") from exc

        logging_settings = self.settings.pop("logging", None)
        if logging_settings:
            # parse logging settings
            self.logging = LoggingSettings(**logging_settings)

        git_settings = self.settings.pop("git", None)
        if git_settings:
            # parse git settings
            self.git = GitSettings(**git_settings)

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

    def get_client(self) -> Union[TestClient, APIClient]:
        """
        Retrieve API client

        Returns
        -------
        Union[TestClient, APIClient]
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

        client: Union[TestClient, APIClient]

        if self.profile:
            if self.git:
                logger.warning("Ignoring Git settings in configurations")
            client = APIClient(api_url=self.profile.api_url, api_token=self.profile.api_token)
        elif self.git:
            # avoid git binary as a requirement for remote api url
            from featurebyte.app import app

            client = TestClient(app)
        else:
            raise InvalidSettingsError("Git or FeatureByte profile settings must be specified")

        return client
