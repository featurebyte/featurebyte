"""
This module contains session to Snowflake integration tests.
"""

import pytest

from featurebyte.session.manager import SessionManager
from featurebyte.session.snowflake import SnowflakeSession


@pytest.mark.asyncio
async def test_schema_initializer(config, snowflake_feature_store):
    """
    Test the session initialization in snowflake works properly.
    """
    session_manager = SessionManager(credentials=config.credentials)
    session = await session_manager.get_session(snowflake_feature_store)
    assert isinstance(session, SnowflakeSession)

    # query for the data in the metadata schema table
    get_version_query = "SELECT * FROM METADATA_SCHEMA"
    results = await session.execute_query(get_version_query)

    # verify that we only have one row
    assert results is not None
    working_schema_version_column = "WORKING_SCHEMA_VERSION"
    assert len(results[working_schema_version_column]) == 1
    # check that this is set to the default value
    assert int(results[working_schema_version_column][0]) == 1

    # Try to retrieve the session again - this should trigger a re-initialization
    # Verify that there's still only one row in table
    session = await session_manager.get_session(snowflake_feature_store)
    results = await session.execute_query(get_version_query)
    assert results is not None
    assert len(results[working_schema_version_column]) == 1
