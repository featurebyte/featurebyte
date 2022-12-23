"""
Test FastAPI app
"""
import os
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
    config = Configurations("tests/fixtures/config.yaml")
    feature_store_name = list(config.credentials.keys())[0]

    with patch("featurebyte.utils.credential.Configurations") as mock_config:
        mock_config.return_value = config
        credential = await get_credential(user_id=ObjectId(), feature_store_name=feature_store_name)
    assert credential == config.credentials[feature_store_name]


@patch.dict(os.environ, {"MONGODB_URI": "mongodb://localhost:27022"}, clear=True)
def test_get_persistent():
    """
    Test get_persistent works as expected
    """
    with patch("featurebyte.utils.persistent.MongoDB") as mock_mongodb:
        with pytest.raises(ValueError):
            mock_mongodb.side_effect = ValueError()
            get_persistent()
    mock_mongodb.assert_called_once_with(uri="mongodb://localhost:27022")


def test_get_app__loading_time():
    """Test app loading time (to detect changes that increase loading app time)"""
    start = time.time()
    get_app()
    elapsed_time = time.time() - start
    assert elapsed_time < 27
