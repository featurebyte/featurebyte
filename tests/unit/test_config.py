"""
Test config parser
"""
import pytest

from featurebyte.config import Configurations, LoggingSettings
from featurebyte.enum import SourceType
from featurebyte.models.credential import CredentialType


def test_configurations():
    """
    Test creating configuration from config file
    """
    config = Configurations("tests/fixtures/sample_config.ini")

    # one database source with section name as key
    assert len(config.db_sources) == 1
    expected_db_source_dict = {
        "type": SourceType.SNOWFLAKE,
        "details": {
            "account": "sf_account",
            "warehouse": "COMPUTE_WH",
            "database": "TEST_DB",
            "sf_schema": "PUBLIC",
        },
    }
    db_source = config.db_sources["snowflake 数据库"]
    assert db_source.dict() == expected_db_source_dict

    # one credential with db source as key
    assert len(config.credentials) == 1
    assert config.credentials[db_source].dict() == {
        "name": "snowflake 数据库",
        "source": expected_db_source_dict,
        "credential_type": CredentialType.USERNAME_PASSWORD,
        "credential": {"username": "user", "password": "password"},
    }

    # logging settings
    assert config.logging == LoggingSettings(
        level="INFO",
        serialize=True,
    )

    # other settings
    assert config.settings == {
        "git": {
            "remote": "git@github.com:featurebyte/featurebyte.git",
            "ssh_key_path": "~/.ssh/id_rsa",
        },
        "featurebyte": {
            "api_url": "https://app.featurebyte.com/api/v1",
            "api_token": "API_TOKEN_VALUE",
        },
    }


def test_configurations_malformed():
    """
    Test creating configuration from malformed config file
    """
    with pytest.raises(ValueError) as exc_info:
        Configurations("tests/fixtures/malformed_config.ini")
    assert str(exc_info.value) == "Invalid settings in section: snowflake 数据库"
