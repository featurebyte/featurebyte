"""
Fixture for API unit tests
"""
import mongomock
import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def test_api_client():
    """
    Test API client
    """
    with mongomock.patch(servers=(("localhost", 27017),)):
        from featurebyte.app import app  # pylint: disable=import-outside-toplevel
        from featurebyte.routes.unified_api_settings import (  # pylint: disable=import-outside-toplevel
            storage,
        )

        with TestClient(app) as client:
            yield client
            # clean up database
            storage._client.drop_database("featurebyte")  # pylint: disable=protected-access
