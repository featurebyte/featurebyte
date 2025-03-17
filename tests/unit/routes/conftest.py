"""
Fixture for API unit tests
"""

from __future__ import annotations

from functools import partial
from unittest.mock import Mock, patch

import pytest
from bson.objectid import ObjectId
from fastapi.testclient import TestClient

from featurebyte import UsernamePasswordCredential
from featurebyte.app import app
from featurebyte.enum import SourceType
from featurebyte.session.base import BaseSession
from featurebyte.session.snowflake import SnowflakeSession


@pytest.fixture(name="patched_validate_row_index", autouse=True)
def patched_validate_row_index_fixture():
    """
    Patched validate_output_row_index to be a no-op
    """
    with patch("featurebyte.session.session_helper.validate_output_row_index") as patched:
        yield patched


@pytest.fixture()
def api_client_persistent(persistent, user_id, temp_storage):
    """
    Test API client
    """
    with patch("featurebyte.app.MongoDBImpl") as mock_get_persistent:
        with patch("featurebyte.app.get_temp_storage") as mock_get_temp_storage:
            with patch("featurebyte.app.User") as mock_user:
                mock_user.return_value.id = user_id
                mock_get_persistent.return_value = persistent
                mock_get_temp_storage.return_value = temp_storage
                with TestClient(app) as client:
                    yield client, persistent


@pytest.fixture(name="mock_get_session")
def get_mocked_get_session_fixture(snowflake_execute_query, adapter, source_info):
    """
    Returns a mocked get_feature_store_session.
    """
    _ = snowflake_execute_query
    with patch(
        "featurebyte.service.session_manager.SessionManagerService.get_feature_store_session"
    ) as mocked_get_session:
        mocked_get_session.return_value = Mock(
            name="MockedSession",
            spec=BaseSession,
            source_type=SourceType.SNOWFLAKE,
            database_name="sf_database",
            schema_name="sf_schema",
            get_source_info=Mock(return_value=source_info),
            adapter=adapter,
        )
        yield mocked_get_session


@pytest.fixture(autouse=True)
def get_mock_get_session_fixture(snowflake_execute_query):
    """
    Returns a mocked get_feature_store_session.
    """
    _ = snowflake_execute_query
    with patch(
        "featurebyte.service.session_manager.SessionManagerService.get_session"
    ) as mocked_get_session:
        mocked_get_session.return_value = SnowflakeSession(
            source_type=SourceType.SNOWFLAKE,
            account="sf_account",
            warehouse="sf_warehouse",
            database_name="sf_database",
            schema_name="sf_schema",
            role_name="TESTING",
            database_credential=UsernamePasswordCredential(
                username="username",
                password="password",
            ),
        )
        yield mocked_get_session


@pytest.fixture(name="location")
def location_fixture():
    """
    location fixture
    """
    return {
        "feature_store_id": ObjectId(),
        "table_details": {
            "database_name": "fb_database",
            "schema_name": "fb_schema",
            "table_name": "fb_materialized_table",
        },
    }


@pytest.fixture(name="create_observation_table")
def create_observation_table_fixture(
    test_api_client_persistent, location, default_catalog_id, user_id
):
    """
    simulate creating observation table for target input, target_id and context_id
    """

    _, persistent = test_api_client_persistent

    async def create_observation_table(
        ob_table_id,
        use_case_id=None,
        target_input=True,
        target_id=None,
        context_id=None,
        context_empty=False,
        entity_id=None,
        table_with_missing_data=None,
    ):
        ob_table_id = ObjectId(ob_table_id)

        if not target_id:
            target_id = ObjectId()
        else:
            target_id = ObjectId(target_id)

        if context_empty:
            context_id = None
        else:
            if not context_id:
                context_id = ObjectId()
            else:
                context_id = ObjectId(context_id)

        request_input = {
            "target_id": target_id,
            "observation_table_id": ob_table_id,
            "type": "dataframe",
        }
        if not target_input:
            request_input = {
                "columns": None,
                "columns_rename_mapping": None,
                "source": location,
                "type": "source_table",
            }

        use_case_ids = []
        if use_case_id:
            use_case_ids.append(ObjectId(use_case_id))

        primary_entity_ids = []
        if entity_id:
            primary_entity_ids.append(ObjectId(entity_id))

        await persistent.insert_one(
            collection_name="observation_table",
            document={
                "_id": ob_table_id,
                "name": "observation_table_from_target_input",
                "request_input": request_input,
                "location": location,
                "columns_info": [
                    {"name": "a", "dtype": "INT"},
                    {"name": "b", "dtype": "INT"},
                    {"name": "c", "dtype": "INT"},
                ],
                "num_rows": 1000,
                "most_recent_point_in_time": "2023-01-15T10:00:00",
                "table_with_missing_data": table_with_missing_data,
                "context_id": context_id,
                "use_case_ids": use_case_ids,
                "primary_entity_ids": primary_entity_ids,
                "catalog_id": ObjectId(default_catalog_id),
                "user_id": user_id,
            },
            user_id=user_id,
        )

    return partial(create_observation_table)
