"""
Fixture for API unit tests
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
from bson.objectid import ObjectId
from fastapi.testclient import TestClient

from featurebyte import UsernamePasswordCredential
from featurebyte.app import app
from featurebyte.enum import SourceType
from featurebyte.session.base import BaseSession
from featurebyte.session.snowflake import SnowflakeSession


@pytest.fixture(scope="session")
def user_id():
    """
    Mock user id
    """
    return ObjectId()


@pytest.fixture()
def api_client_persistent(persistent, user_id, temp_storage):
    """
    Test API client
    """
    with patch("featurebyte.app.get_persistent") as mock_get_persistent:
        with patch("featurebyte.app.get_temp_storage") as mock_get_temp_storage:
            with patch("featurebyte.app.User") as mock_user:
                mock_user.return_value.id = user_id
                mock_get_persistent.return_value = persistent
                mock_get_temp_storage.return_value = temp_storage
                with TestClient(app) as client:
                    yield client, persistent


@pytest.fixture(name="mock_get_session")
def get_mocked_get_session_fixture(session_manager, snowflake_execute_query):
    """
    Returns a mocked get_feature_store_session.
    """
    _, _ = session_manager, snowflake_execute_query
    with patch(
        "featurebyte.service.session_manager.SessionManagerService.get_feature_store_session"
    ) as mocked_get_session:
        mocked_get_session.return_value = Mock(
            name="MockedSession",
            spec=BaseSession,
            source_type=SourceType.SNOWFLAKE,
            database_name="sf_database",
            schema_name="sf_schema",
        )
        yield mocked_get_session


@pytest.fixture(autouse=True)
def get_mock_get_session_fixture(session_manager, snowflake_execute_query):
    """
    Returns a mocked get_feature_store_session.
    """
    _, _ = session_manager, snowflake_execute_query
    with patch(
        "featurebyte.service.session_manager.SessionManagerService.get_feature_store_session"
    ) as mocked_get_session:
        mocked_get_session.return_value = SnowflakeSession(
            source_type=SourceType.SNOWFLAKE,
            account="sf_account",
            warehouse="sf_warehouse",
            database="sf_database",
            sf_schema="sf_schema",
            database_credential=UsernamePasswordCredential(
                username="username",
                password="password",
            ),
        )
        yield mocked_get_session


@pytest.fixture(name="get_credential")
def get_credential_fixture(credentials):
    """
    get_credential fixture
    """

    async def get_credential(user_id, feature_store_name):
        _ = user_id
        return credentials.get(feature_store_name)

    return get_credential
