"""
This module contains session to DataBricks Unity integration tests.
"""

import os
from unittest.mock import patch

import pytest
from bson import ObjectId
from numpy.testing import assert_allclose

from featurebyte.models.credential import AccessTokenCredential, CredentialModel
from featurebyte.session.databricks_unity import (
    DatabricksUnitySchemaInitializer,
    DatabricksUnitySession,
)


@pytest.mark.parametrize("source_type", ["databricks_unity"], indirect=True)
@pytest.mark.asyncio
async def test_decimal_casting(session_without_datasets):
    """
    Test large decimal without scale information
    """
    session = session_without_datasets
    # In this case, the cursor's description doesn't provide the scale information for the decimal.
    # Make sure the conversion via pyarrow doesn't fail.
    df = await session.execute_query(
        "SELECT CAST(123456789012345678901234567890123456.90 AS DECIMAL(38, 2)) AS result"
    )
    assert_allclose(df["result"].iloc[0], 1.2345678901234568e35)


@pytest.mark.parametrize("source_type", ["databricks_unity"], indirect=True)
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
    _ = config
    db_session = session_without_datasets
    assert isinstance(db_session, DatabricksUnitySession)
    initializer = DatabricksUnitySchemaInitializer(db_session)

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


@pytest.mark.parametrize("source_type", ["databricks_unity"], indirect=True)
@pytest.mark.asyncio
async def test_list_tables(config, session_without_datasets):
    """
    Test the session initialization in snowflake works properly.
    """
    _ = config
    session = session_without_datasets

    tables = await session.list_tables(database_name="demo_datasets", schema_name="grocery")

    def _sort_by_name(_tables):
        return sorted(_tables, key=lambda x: x["name"])

    def _filter_grocery(_tables):
        # Make the test more robust against randomly added tables by filtering by certain keywords
        out = []
        for table in _tables:
            table_name = table["name"]
            if "grocery" in table_name or "invoiceitems" in table_name:
                out.append(table)
        return out

    assert _filter_grocery(
        _sort_by_name([table.model_dump() for table in tables])
    ) == _sort_by_name([
        {
            "name": "invoiceitems",
            "description": "The grocery item details within each invoice, including the "
            "quantity, total cost, discount applied, and product ID.",
        },
        {
            "name": "groceryproduct",
            "description": "The product group description for each grocery product.",
        },
        {
            "name": "groceryproduct_with_embeddings",
            "description": "The product group description for each grocery product.",
        },
        {"name": "__invoiceitems", "description": None},
        {"name": "__groceryinvoice", "description": None},
        {
            "name": "groceryinvoice",
            "description": "Grocery invoice details, containing the timestamp and the "
            "total amount of the invoice.",
        },
        {"description": None, "name": "__grocerycustomer"},
        {
            "name": "grocerycustomer",
            "description": "Customer details, including their name, address, and date of birth.",
        },
    ])


@pytest.mark.parametrize("source_type", ["databricks_unity"], indirect=True)
@pytest.mark.asyncio
async def test_access_token_credential(
    config,
    feature_store,
    session_manager_service,
):
    """
    Access feature store using access token credential.
    """
    _ = config

    feature_store_credential = CredentialModel(
        name="databricks_featurestore",
        feature_store_id=ObjectId(),
        database_credential=AccessTokenCredential(
            access_token=os.getenv("DATABRICKS_ACCESS_TOKEN", ""),
        ),
    )
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("DATABRICKS_CLIENT_ID", None)
        os.environ.pop("DATABRICKS_CLIENT_SECRET", None)
        db_session = await session_manager_service.get_session(
            feature_store, feature_store_credential
        )
    results = await db_session.execute_query("SHOW DATABASES")
    assert results is not None
