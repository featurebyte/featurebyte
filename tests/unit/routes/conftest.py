"""
Fixture for API unit tests
"""
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from featurebyte.app import app
from featurebyte.persistent import GitDB


@pytest.fixture()
def test_api_client():
    """
    Test API client
    """
    with patch("featurebyte.app._get_persistent") as mock_get_persistent:
        mock_get_persistent.return_value = GitDB()
        with TestClient(app) as client:
            yield client
