"""
Test feature store controller
"""
import shutil
import tempfile
from unittest.mock import patch

import pytest

from featurebyte.config import Configurations, LoggingSettings
from featurebyte.models.credential import Credential, CredentialType, UsernamePasswordCredential


@pytest.mark.asyncio
async def test_persist_credential(app_container):
    """
    Test persist credential has an update as expected
    """
    with tempfile.NamedTemporaryFile(mode="w") as file_handle:
        config_no_profile_file_name = "tests/fixtures/config/config_no_profile.yaml"
        config_file_name = file_handle.name
        shutil.copy2(config_no_profile_file_name, config_file_name)
        config = Configurations(config_file_name)
        # Assert no credentials exist
        assert len(config.credentials) == 0
        # Assert basic logging config exists
        initial_logging_settings = LoggingSettings(
            level="INFO",
            serialize=True,
        )
        assert config.logging == initial_logging_settings

        # Write creds to file
        feature_store_name = "random_feature_store_name"
        cred = Credential(
            name=feature_store_name,
            credential_type=CredentialType.USERNAME_PASSWORD,
            credential=UsernamePasswordCredential(
                username="random_username",
                password="random_password",
            ),
        )
        with patch("featurebyte.routes.feature_store.controller.Configurations") as mock_config:
            mock_config.return_value = config
            controller = app_container.feature_store_controller
            did_update = await controller.persist_credentials_if_needed(cred, feature_store_name)
            assert did_update

        # Reload config
        new_config = Configurations(config_file_name)
        # Assert credentials exist
        assert len(new_config.credentials) == 1
        loaded_creds = new_config.credentials.get(feature_store_name)
        assert loaded_creds == cred
        # Assert existing configs are still there
        assert new_config.logging == initial_logging_settings
