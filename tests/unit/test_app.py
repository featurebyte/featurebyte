"""
Test FastAPI app
"""
import time
from unittest.mock import patch

import pytest
from bson.objectid import ObjectId

from featurebyte.app import get_app
from featurebyte.config import Configurations
from featurebyte.utils.credential import get_credential
from featurebyte.utils.persistent import get_persistent


@pytest.mark.asyncio
async def test_get_credential():
    """
    Test get_credential works as expected
    """
    config = Configurations("tests/fixtures/config_git_persistent.yaml")
    feature_store_name = list(config.credentials.keys())[0]

    with patch("featurebyte.utils.credential.Configurations") as mock_config:
        mock_config.return_value = config
        credential = await get_credential(user_id=ObjectId(), feature_store_name=feature_store_name)
    assert credential == config.credentials[feature_store_name]


def test_get_persistent():
    """
    Test get_persistent works as expected
    """
    config = Configurations("tests/fixtures/config_git_persistent.yaml")

    with patch("featurebyte.utils.persistent.Configurations") as mock_config:
        mock_config.return_value = config
        with patch("featurebyte.utils.persistent.GitDB") as mock_git:
            with pytest.raises(ValueError):
                mock_git.side_effect = ValueError()
                get_persistent()
    mock_git.assert_called_once_with(**config.git.dict())


def test_get_app__loading_time():
    """Test app loading time (to detect changes that increase loading app time)"""
    start = time.time()
    get_app()
    elapsed_time = time.time() - start
    assert elapsed_time < 4
