"""
Read configurations from ini file
"""
# pylint: disable=too-few-public-methods
from typing import Any, Dict, Optional

import os
from configparser import ConfigParser

from pydantic import BaseSettings
from pydantic.error_wrappers import ValidationError

from featurebyte.enum import SourceType
from featurebyte.models.credential import CREDENTIAL_CLASS, Credential, CredentialType
from featurebyte.models.event_data import DB_DETAILS_CLASS, DatabaseSource


class LoggingSettings(BaseSettings):
    """
    Settings related with the logging
    """

    level: str = "DEBUG"
    serialize: bool = False


class Configurations:
    """
    FeatureByte SDK settings. Contains general settings, database sources and credentials.
    """

    _config_file_path: str
    settings: Dict[str, Any] = {}
    db_sources: Dict[str, DatabaseSource] = {}
    credentials: Dict[DatabaseSource, Credential] = {}
    logging: LoggingSettings = LoggingSettings()

    def __init__(self, config_file_path: Optional[str] = None) -> None:
        """
        Load and parse configurations

        Parameters
        ----------
        config_file_path: Optional[str]
            Path to read configurations from
        """
        config_file_path = str(
            config_file_path
            or os.environ.get(
                "FEATUREBYTE_CONFIG_PATH", os.path.join(os.environ["HOME"], ".featurebyte.ini")
            )
        )
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
        config_parser = ConfigParser()
        config_parser.read(path)
        self.settings = {}
        for section in config_parser.sections():

            # parse db_sources and credentials for easier retrieval
            values = dict(config_parser.items(section))
            try:
                if "source_type" in values and values["source_type"] in DB_DETAILS_CLASS:
                    # parse and store database source
                    source_type = SourceType(values["source_type"])
                    db_source = DatabaseSource(
                        type=source_type,
                        details=DB_DETAILS_CLASS[source_type](**values),
                    )
                    self.db_sources[section] = db_source

                    # parse and store credentials
                    credential_type = CredentialType(values["credential_type"])
                    credentials = Credential(
                        name=section,
                        source=db_source,
                        credential=CREDENTIAL_CLASS[credential_type](**values),
                        **values,
                    )
                    self.credentials[db_source] = credentials

                elif section == "logging":
                    # parse logging settings
                    self.logging = LoggingSettings(**values)
                else:
                    # store generic settings
                    self.settings[section] = values

            except ValidationError as exc:
                raise ValueError(f"Invalid settings in section: {section}") from exc


config = Configurations()
