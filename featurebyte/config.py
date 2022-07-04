"""
Read configurations from ini file
"""
# pylint: disable=too-few-public-methods
from typing import Any, Dict, Optional, Pattern

import os
import re
from enum import Enum
from pathlib import Path

import yaml
from pydantic import BaseSettings, ConstrainedStr
from pydantic.error_wrappers import ValidationError

from featurebyte.enum import SourceType
from featurebyte.models.credential import Credential
from featurebyte.models.feature_store import FeatureStoreModel

# data source to credential mapping
Credentials = Dict[FeatureStoreModel, Optional[Credential]]


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
        config_file_path = str(
            config_file_path
            or os.environ.get(
                "FEATUREBYTE_CONFIG_PATH", os.path.join(os.environ["HOME"], ".featurebyte.yaml")
            )
        )
        self.settings: Dict[str, Any] = {}
        self.feature_stores: Dict[str, FeatureStoreModel] = {}
        self.credentials: Credentials = {}
        self.logging: LoggingSettings = LoggingSettings()
        self._config_file_path = config_file_path
        self._parse_config(config_file_path)

    def _parse_config(self, path: str) -> None:
        """
        Parse configurations file

        Parameters
        ----------
        path: str
            Path to read configurations from

        Raises
        ------
        ValueError
            Parsing Error
        """
        if not os.path.exists(path):
            return

        with open(path, encoding="utf-8") as file_obj:
            self.settings = yaml.safe_load(file_obj)

        feature_stores = self.settings.pop("featurestore", [])
        for feature_store in feature_stores:
            name = feature_store.pop("name", "unnamed")
            try:
                if "source_type" in feature_store:
                    # parse and store feature store
                    source_type = SourceType(feature_store["source_type"])
                    db_source = FeatureStoreModel(
                        type=source_type,
                        details=feature_store,
                    )
                    self.feature_stores[name] = db_source

                    # parse and store credentials
                    credentials = None
                    if "credential_type" in feature_store:
                        # credentials are stored together with feature store details in the config file,
                        # Credential pydantic model will use only the relevant fields
                        credentials = Credential(
                            name=name,
                            source=db_source,
                            credential_type=feature_store["credential_type"],
                            credential=feature_store,
                        )
                    self.credentials[db_source] = credentials
            except ValidationError as exc:
                raise ValueError(f"Invalid settings for datasource: {name}") from exc

        logging_settings = self.settings.pop("logging", None)
        if logging_settings:
            # parse logging settings
            self.logging = LoggingSettings(**logging_settings)

        git_settings = self.settings.pop("git", None)
        if git_settings:
            # parse git settings
            self.git = GitSettings(**git_settings)
