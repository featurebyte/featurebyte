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
        from featurebyte.app import app, get_persistent  # pylint: disable=import-outside-toplevel

        with TestClient(app) as client:
            yield client
            # clean up database
            get_persistent()._client.drop_database(  # pylint: disable=protected-access
                "featurebyte"
            )
