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
        from featurebyte.app import app, persistent  # pylint: disable=import-outside-toplevel

        with TestClient(app) as client:
            yield client
            # clean up database
            persistent._client.drop_database("featurebyte")  # pylint: disable=protected-access
