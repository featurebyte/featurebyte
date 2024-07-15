"""
This module contains session to Spark integration tests.
"""

from collections import OrderedDict

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.session.base_spark import BaseSparkSession
from featurebyte.session.manager import SessionManager


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_schema_initializer(config, feature_store, credentials_mapping, session):
    """
    Test the session initialization in spark works properly.
    """
    session_manager = SessionManager(credentials=credentials_mapping)
    assert isinstance(session, BaseSparkSession)
    initializer = session.initializer()

    assert await session.list_databases() == ["spark_catalog"]
    assert session.schema_name.lower() in await session.list_schemas(database_name="spark_catalog")
    tables = await session.list_tables(database_name="spark_catalog", schema_name=session.schema_name)
    table_names = [table.name for table in tables]
    assert "metadata_schema" in table_names
    column_details = await session.list_table_schema(
        database_name="spark_catalog", schema_name=session.schema_name, table_name="metadata_schema"
    )
    assert column_details == OrderedDict([
        (
            "WORKING_SCHEMA_VERSION",
            ColumnSpecWithDescription(name="WORKING_SCHEMA_VERSION", dtype="INT", description=None),
        ),
        (
            "MIGRATION_VERSION",
            ColumnSpecWithDescription(name="MIGRATION_VERSION", dtype="INT", description=None),
        ),
        (
            "FEATURE_STORE_ID",
            ColumnSpecWithDescription(name="FEATURE_STORE_ID", dtype="VARCHAR", description=None),
        ),
        (
            "CREATED_AT",
            ColumnSpecWithDescription(name="CREATED_AT", dtype="TIMESTAMP", description=None),
        ),
    ])

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


@pytest.mark.parametrize("source_type", ["spark", "databricks_unity"], indirect=True)
@pytest.mark.asyncio
async def test_session_timezone(session):
    """
    Test session configurations
    """
    result = await session.execute_query("SELECT current_timezone() AS timezone")
    assert result["timezone"].iloc[0] in ["UTC", "Etc/UTC"]


@pytest.mark.parametrize("source_type", ["spark", "databricks_unity"], indirect=True)
@pytest.mark.asyncio
async def test_register_table(session):
    """
    Test the session register_table in spark works properly.
    """
    df_training_events = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00.123456789"] * 2 + ["2001-01-03 10:00:00.123456789"] * 3),
        "üser id": [1, 2, 3, 4, 5],
    })
    table_name = "test_table_test_register_table"
    await session.register_table(table_name=table_name, dataframe=df_training_events)
    df_retrieve = await session.execute_query(f"SELECT * FROM {table_name}")
    assert_frame_equal(df_retrieve, df_training_events, check_dtype=False)
