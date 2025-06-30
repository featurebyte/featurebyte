"""
This module contains session to Spark integration tests.
"""

from collections import OrderedDict

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from featurebyte.query_graph.model.column_info import ColumnSpecWithDetails
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.session.base_spark import BaseSparkSession


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_schema_initializer(
    config,
    feature_store,
    session_without_datasets,
    feature_store_credential,
    session_manager_service,
):
    """
    Test the session initialization in spark works properly.
    """

    db_session = session_without_datasets
    assert isinstance(db_session, BaseSparkSession)
    initializer = db_session.initializer()

    assert await db_session.list_databases() == ["spark_catalog"]
    assert db_session.schema_name.lower() in await db_session.list_schemas(
        database_name="spark_catalog"
    )
    tables = await db_session.list_tables(
        database_name="spark_catalog", schema_name=db_session.schema_name
    )
    table_names = [table.name for table in tables]
    assert "metadata_schema" in table_names
    column_details = await db_session.list_table_schema(
        database_name="spark_catalog",
        schema_name=db_session.schema_name,
        table_name="metadata_schema",
    )
    assert column_details == OrderedDict([
        (
            "WORKING_SCHEMA_VERSION",
            ColumnSpecWithDetails(name="WORKING_SCHEMA_VERSION", dtype="INT", description=None),
        ),
        (
            "MIGRATION_VERSION",
            ColumnSpecWithDetails(name="MIGRATION_VERSION", dtype="INT", description=None),
        ),
        (
            "FEATURE_STORE_ID",
            ColumnSpecWithDetails(name="FEATURE_STORE_ID", dtype="VARCHAR", description=None),
        ),
        (
            "CREATED_AT",
            ColumnSpecWithDetails(name="CREATED_AT", dtype="TIMESTAMP", description=None),
        ),
    ])

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
        "POINT_IN_TIME": pd.to_datetime(
            ["2001-01-02 10:00:00.123456789"] * 2 + ["2001-01-03 10:00:00.123456789"] * 3
        ),
        "üser id": [1, 2, 3, 4, 5],
    })
    table_name = "test_table_test_register_table"
    await session.register_table(table_name=table_name, dataframe=df_training_events)
    df_retrieve = await session.execute_query(f"SELECT * FROM {table_name}")
    assert_frame_equal(df_retrieve, df_training_events, check_dtype=False)


@pytest.mark.parametrize("source_type", ["spark", "databricks_unity"], indirect=True)
@pytest.mark.asyncio
async def test_partitioned_table_column_info(session):
    """
    Test list columns of a partitioned table.
    """
    df_training_events = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime(
            ["2001-01-02 10:00:00.123456789"] * 2 + ["2001-01-03 10:00:00.123456789"] * 3
        ),
        "üser id": [1, 2, 3, 4, 5],
    })
    table_name = "test_table_test_register_table"
    await session.register_table(table_name=table_name, dataframe=df_training_events)

    partitioned_table_name = "test_table_test_partitioned_table"
    source_info = session.get_source_info()
    await session.create_table_as(
        table_details=TableDetails(
            database_name=source_info.database_name,
            schema_name=source_info.schema_name,
            table_name=partitioned_table_name,
        ),
        select_expr=f"SELECT * FROM `{source_info.database_name}`.`{source_info.schema_name}`.{table_name}",
        partition_keys=["POINT_IN_TIME"],
    )
    columns = await session.list_table_schema(
        database_name=source_info.database_name,
        schema_name=source_info.schema_name,
        table_name=partitioned_table_name,
    )
    assert "POINT_IN_TIME" in columns
    assert "üser id" in columns
    assert columns == OrderedDict([
        (
            "POINT_IN_TIME",
            ColumnSpecWithDetails(
                name="POINT_IN_TIME",
                dtype="TIMESTAMP",
                dtype_metadata=None,
                is_partition_key=True,
                description=None,
            ),
        ),
        (
            "üser id",
            ColumnSpecWithDetails(
                name="üser id",
                dtype="INT",
                dtype_metadata=None,
                is_partition_key=False,
                description=None,
            ),
        ),
    ])
