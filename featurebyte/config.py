"""
Read configurations from ini file
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Any, Dict, Optional

import os
from enum import Enum

import yaml
from pydantic import BaseSettings
from pydantic.error_wrappers import ValidationError

from featurebyte.enum import SourceType
from featurebyte.models.credential import CREDENTIAL_CLASS, Credential, CredentialType
from featurebyte.models.event_data import DB_DETAILS_CLASS, DatabaseSourceModel

# data source to credential mapping
Credentials = Dict[DatabaseSourceModel, Optional[Credential]]


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
    Settings related with the logging
    """

    level: LogLevel = LogLevel.DEBUG
    serialize: bool = False


class SnowflakeSettings(BaseSettings):
    """
    Settings specific to snowflake
    """

    featurebyte_schema: str = "FEATUREBYTE"


class Configurations:
    """
    FeatureByte SDK settings. Contains general settings, database sources and credentials.
    """

    def __init__(self, config_file_path: str | None = None) -> None:
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
        self.settings: dict[str, Any] = {}
        self.db_sources: dict[str, DatabaseSourceModel] = {}
        self.credentials: Credentials = {}
        self.logging: LoggingSettings = LoggingSettings()
        self.snowflake = SnowflakeSettings()
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

        datasources = self.settings.pop("datasource", [])
        for datasource in datasources:
            try:
                name = datasource.pop("name", "unnamed")
                if "source_type" in datasource and datasource["source_type"] in DB_DETAILS_CLASS:
                    # parse and store database source
                    source_type = SourceType(datasource["source_type"])
                    db_source = DatabaseSourceModel(
                        type=source_type,
                        details=DB_DETAILS_CLASS[source_type](**datasource),
                    )
                    self.db_sources[name] = db_source

                    # parse and store credentials
                    credentials = None
                    if datasource.get("credential_type"):
                        credential_type = CredentialType(datasource["credential_type"])
                        credentials = Credential(
                            name=name,
                            source=db_source,
                            credential=CREDENTIAL_CLASS[credential_type](**datasource),
                            **datasource,
                        )
                    self.credentials[db_source] = credentials
            except ValidationError as exc:
                raise ValueError(f"Invalid settings for datasource: {name}") from exc

        logging_settings = self.settings.pop("logging", None)
        if logging_settings:
            # parse logging settings
            self.logging = LoggingSettings(**logging_settings)

        snowflake_settings = self.settings.pop("snowflake", None)
        if snowflake_settings:
            self.snowflake = SnowflakeSettings(**snowflake_settings)
