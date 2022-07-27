"""
Read configurations from ini file
"""
from typing import Any, Dict, Optional, Pattern, Union

import os
import re
from enum import Enum
from http import HTTPStatus
from pathlib import Path

# pylint: disable=too-few-public-methods
import requests
import yaml
from fastapi.testclient import TestClient
from pydantic import BaseSettings, ConstrainedStr, HttpUrl, StrictStr
from pydantic.error_wrappers import ValidationError
from requests import Response

from featurebyte.exception import InvalidSettingsError
from featurebyte.models.credential import Credential

# data source to credential mapping
Credentials = Dict[StrictStr, Optional[Credential]]


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


class LoggingSettings(BaseSettings):
    """
    Settings for logging
    """

    level: LogLevel = LogLevel.DEBUG
    serialize: bool = False


class GitRepoUrl(ConstrainedStr):
    """
    Git repo string
    """

    regex: Optional[Pattern[str]] = re.compile(r".*\.git")


class GitSettings(BaseSettings):
    """
    Settings for git access
    """

    remote_url: GitRepoUrl
    key_path: Optional[Path]
    branch: str


class FeatureByteSettings(BaseSettings):
    """
    Settings for FeatureByte Application API access
    """

    api_url: HttpUrl
    api_token: str


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

        Raises
        ------
        InvalidSettingsError
            Invalid settings
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
        response = self.get("/user/me")
        if response.status_code != HTTPStatus.OK:
            raise InvalidSettingsError("Authentication failed")

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
                or os.environ.get(
                    "FEATUREBYTE_CONFIG_PATH", os.path.join(os.environ["HOME"], ".featurebyte.yaml")
                )
            )
        )
        self.git: Optional[GitSettings] = None
        self.featurebyte: Optional[FeatureByteSettings] = None
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

        featurebyte_settings = self.settings.pop("featurebyte", None)
        if featurebyte_settings:
            # parse featurebyte settings
            self.featurebyte = FeatureByteSettings(**featurebyte_settings)

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
        from featurebyte.app import app
        from featurebyte.logger import logger

        client: Union[TestClient, APIClient]

        if self.featurebyte:
            if self.git:
                logger.warning("Ignoring Git settings in configurations")
            client = APIClient(
                api_url=self.featurebyte.api_url, api_token=self.featurebyte.api_token
            )
        elif self.git:
            client = TestClient(app)
        else:
            raise InvalidSettingsError("Git or FeatureByte API settings must be specified")

        return client
