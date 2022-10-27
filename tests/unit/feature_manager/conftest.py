"""
Common test fixtures used across unit test directories related to feature manager
"""
import pytest


@pytest.fixture(autouse=True)
def use_mock_get_client(mock_get_client):
    """
    Use mock get client for route tests
    """
    yield
