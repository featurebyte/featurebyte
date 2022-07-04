"""
Test FastAPI app
"""
from unittest.mock import patch

import pytest
from bson.objectid import ObjectId

from featurebyte.app import _get_credential, _get_persistent
from featurebyte.config import Configurations


def test_get_credential():
    """
    Test get_credential works as expected
    """
    config = Configurations("tests/fixtures/sample_config.yaml")
    db_source = list(config.credentials.keys())[0]

    with patch("featurebyte.app.Configurations") as mock_config:
        mock_config.return_value = config
        credential = _get_credential(user_id=ObjectId(), db_source=db_source)
    assert credential == config.credentials[db_source]


def test_get_persistent():
    """
    Test get_persistent works as expected
    """
    config = Configurations("tests/fixtures/sample_config.yaml")

    with patch("featurebyte.app.Configurations") as mock_config:
        mock_config.return_value = config
        with patch("featurebyte.app.GitDB") as mock_git:
            with pytest.raises(ValueError):
                mock_git.side_effect = ValueError()
                _get_persistent()
    mock_git.assert_called_once_with(**config.git.dict())
