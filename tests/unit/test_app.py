"""
Test FastAPI app
"""

import importlib
import os
import time
from http import HTTPStatus
from unittest.mock import patch

import pytest

from featurebyte import get_version
from featurebyte.app import get_app
from featurebyte.config import Configurations
from featurebyte.utils import persistent


@patch.dict(os.environ, {"MONGODB_URI": "mongodb://localhost:27022"}, clear=True)
def test_get_persistent():
    """
    Test get_persistent works as expected
    """
    importlib.reload(persistent)
    with patch("featurebyte.utils.persistent.MongoDB.__init__") as mock_mongodb:
        with pytest.raises(ValueError):
            mock_mongodb.side_effect = ValueError()
            persistent.MongoDBImpl()
    mock_mongodb.assert_called_once_with(uri="mongodb://localhost:27022", database="featurebyte")


@pytest.mark.flaky(reruns=3)
def test_get_app__loading_time():
    """Test app loading time (to detect changes that increase loading app time)"""
    start = time.time()
    get_app()
    elapsed_time = time.time() - start
    print(f"get_app took {elapsed_time} seconds")
    assert elapsed_time < 10


def test_get_status():
    """Test app get status"""
    response = Configurations().get_client().get("/status")
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {"sdk_version": get_version()}


def test_open_api_schema():
    """Test app openapi schema"""
    response = Configurations().get_client().get("/openapi.json")
    assert response.status_code == HTTPStatus.OK
    schema = response.json()
    assert "openapi" in schema
    assert "info" in schema
    assert "paths" in schema
    assert "components" in schema
    assert schema["components"]["schemas"]["CatalogCreate"]["properties"][
        "default_feature_store_ids"
    ] == {
        "type": "array",
        "items": {"type": "string"},
        "title": "Default Feature Store Ids",
    }
