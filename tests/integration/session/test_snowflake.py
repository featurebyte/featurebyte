"""
This module contains session to Snowflake integration tests.
"""

import pytest

from featurebyte.session.snowflake import SnowflakeSchemaInitializer, SnowflakeSession


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_schema_initializer(
    config,
    session_without_datasets,
    feature_store,
    feature_store_credential,
    session_manager_service,
):
    """
    Test the session initialization in snowflake works properly.
    """
    db_session = session_without_datasets
    assert isinstance(db_session, SnowflakeSession)
    initializer = SnowflakeSchemaInitializer(db_session)

    # query for the table in the metadata schema table
    get_version_query = "SELECT * FROM METADATA_SCHEMA"
    results = await db_session.execute_query(get_version_query)

    # verify that we only have one row
    assert results is not None
    working_schema_version_column = "WORKING_SCHEMA_VERSION"
    assert len(results[working_schema_version_column]) == 1
    # check that this is set to the default value
    assert (
        int(results[working_schema_version_column][0]) == initializer.current_working_schema_version
    )

    # Try to retrieve the session again - this should trigger a re-initialization
    # Verify that there's still only one row in table
    db_session = await session_manager_service.get_session(feature_store, feature_store_credential)
    results = await db_session.execute_query(get_version_query)
    assert results is not None
    assert len(results[working_schema_version_column]) == 1
    assert (
        int(results[working_schema_version_column][0]) == initializer.current_working_schema_version
    )
