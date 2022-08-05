"""
Test FastAPI app
"""
from unittest.mock import patch

import pytest
from bson.objectid import ObjectId

from featurebyte.app import _get_credential, _get_persistent
from featurebyte.config import Configurations


async def test_get_credential():
    """
    Test get_credential works as expected
    """
    config = Configurations("tests/fixtures/config_git_persistent.yaml")
    feature_store_name = list(config.credentials.keys())[0]

    with patch("featurebyte.app.Configurations") as mock_config:
        mock_config.return_value = config
        credential = await _get_credential(
            user_id=ObjectId(), feature_store_name=feature_store_name
        )
    assert credential == config.credentials[feature_store_name]


def test_get_persistent():
    """
    Test get_persistent works as expected
    """
    config = Configurations("tests/fixtures/config_git_persistent.yaml")

    with patch("featurebyte.app.Configurations") as mock_config:
        mock_config.return_value = config
        with patch("featurebyte.app.GitDB") as mock_git:
            with pytest.raises(ValueError):
                mock_git.side_effect = ValueError()
                _get_persistent()
    mock_git.assert_called_once_with(**config.git.dict())
