"""
Test config parser
"""
import pytest

from featurebyte.config import Configurations, GitSettings, LoggingSettings
from featurebyte.enum import SourceType
from featurebyte.models.credential import CredentialType


def test_configurations():
    """
    Test creating configuration from config file
    """
    config = Configurations("tests/fixtures/sample_config.yaml")

    # one database source with section name as key
    assert len(config.feature_stores) == 1
    expected_db_source_dict = {
        "type": SourceType.SNOWFLAKE,
        "details": {
            "account": "sf_account",
            "warehouse": "COMPUTE_WH",
            "database": "FEATUREBYTE",
            "sf_schema": "FEATUREBYTE",
        },
    }
    feature_store = config.feature_stores["Snowflake FeatureStøre"]
    assert feature_store.dict() == expected_db_source_dict

    # one credential with db source as key
    assert len(config.credentials) == 1
    assert config.credentials[feature_store].dict() == {
        "name": "Snowflake FeatureStøre",
        "source": expected_db_source_dict,
        "credential_type": CredentialType.USERNAME_PASSWORD,
        "credential": {"username": "user", "password": "password"},
    }

    # logging settings
    assert config.logging == LoggingSettings(
        level="INFO",
        serialize=True,
    )

    # git settings
    assert config.git == GitSettings(
        remote_url="git@github.com:featurebyte/playground.git",
        key_path="~/.ssh/id_rsa",
        branch="test",
    )

    # other settings
    assert config.settings == {
        "featurebyte": {
            "api_url": "https://app.featurebyte.com/api/v1",
            "api_token": "API_TOKEN_VALUE",
        },
    }


def test_configurations_malformed_datasource():
    """
    Test creating configuration from malformed config file
    """
    with pytest.raises(ValueError) as exc_info:
        Configurations("tests/fixtures/malformed_config.yaml")
    assert str(exc_info.value) == "Invalid settings for datasource: Snowflake FeatureStøre"
    raise AssertionError("Fail on purpose")
