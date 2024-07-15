"""
This module contains session to DataBricks Unity integration tests.
"""

import pytest

from featurebyte.session.databricks_unity import (
    DatabricksUnitySchemaInitializer,
    DatabricksUnitySession,
)
from featurebyte.session.manager import SessionManager


@pytest.mark.parametrize("source_type", ["databricks_unity"], indirect=True)
@pytest.mark.asyncio
async def test_schema_initializer(config, feature_store, credentials_mapping):
    """
    Test the session initialization in snowflake works properly.
    """
    _ = config
    session_manager = SessionManager(credentials=credentials_mapping)
    session = await session_manager.get_session(feature_store)
    assert isinstance(session, DatabricksUnitySession)
    initializer = DatabricksUnitySchemaInitializer(session)

    # query for the table in the metadata schema table
    get_version_query = "SELECT * FROM METADATA_SCHEMA"
    results = await session.execute_query(get_version_query)

    # verify that we only have one row
    assert results is not None
    working_schema_version_column = "WORKING_SCHEMA_VERSION"
    assert len(results[working_schema_version_column]) == 1
    # check that this is set to the default value
    assert int(results[working_schema_version_column][0]) == initializer.current_working_schema_version

    # Try to retrieve the session again - this should trigger a re-initialization
    # Verify that there's still only one row in table
    session = await session_manager.get_session(feature_store)
    results = await session.execute_query(get_version_query)
    assert results is not None
    assert len(results[working_schema_version_column]) == 1
    assert int(results[working_schema_version_column][0]) == initializer.current_working_schema_version


@pytest.mark.parametrize("source_type", ["databricks_unity"], indirect=True)
@pytest.mark.asyncio
async def test_list_tables(config, feature_store, credentials_mapping):
    """
    Test the session initialization in snowflake works properly.
    """
    _ = config
    session_manager = SessionManager(credentials=credentials_mapping)
    session = await session_manager.get_session(feature_store)

    tables = await session.list_tables(database_name="demo_datasets", schema_name="grocery")
    assert [table.dict() for table in tables] == [
        {
            "name": "invoiceitems",
            "description": "The grocery item details within each invoice, including the "
            "quantity, total cost, discount applied, and product ID.",
        },
        {
            "name": "groceryproduct",
            "description": "The product group description for each grocery product.",
        },
        {"name": "__invoiceitems", "description": None},
        {"name": "__groceryinvoice", "description": None},
        {
            "name": "groceryinvoice",
            "description": "Grocery invoice details, containing the timestamp and the " "total amount of the invoice.",
        },
        {"description": None, "name": "__grocerycustomer"},
        {
            "name": "grocerycustomer",
            "description": "Customer details, including their name, address, and date of " "birth.",
        },
    ]
