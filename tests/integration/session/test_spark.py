"""
This module contains session to Spark integration tests.
"""
from collections import OrderedDict

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from featurebyte.session.manager import SessionManager
from featurebyte.session.spark import SparkSchemaInitializer, SparkSession


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_schema_initializer(config, feature_store):
    """
    Test the session initialization in spark works properly.
    """
    session_manager = SessionManager(credentials=config.credentials)
    session = await session_manager.get_session(feature_store)
    assert isinstance(session, SparkSession)
    initializer = SparkSchemaInitializer(session)

    assert await session.list_databases() == ["spark_catalog"]
    assert session.schema_name in await session.list_schemas(database_name="spark_catalog")
    assert "metadata_schema" in await session.list_tables(
        database_name="spark_catalog", schema_name=session.schema_name
    )
    column_details = await session.list_table_schema(
        database_name="spark_catalog", schema_name=session.schema_name, table_name="metadata_schema"
    )
    assert column_details == OrderedDict(
        [
            ("WORKING_SCHEMA_VERSION", "INT"),
            ("MIGRATION_VERSION", "INT"),
            ("FEATURE_STORE_ID", "VARCHAR"),
            ("CREATED_AT", "TIMESTAMP"),
        ]
    )

    # query for the table in the metadata schema table
    get_version_query = "SELECT * FROM METADATA_SCHEMA"
    results = await session.execute_query(get_version_query)

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
    session = await session_manager.get_session(feature_store)
    results = await session.execute_query(get_version_query)
    assert results is not None
    assert len(results[working_schema_version_column]) == 1
    assert (
        int(results[working_schema_version_column][0]) == initializer.current_working_schema_version
    )


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_session_timezone(session):
    """
    Test session configurations
    """
    result = await session.execute_query("SELECT current_timezone() AS timezone")
    assert result["timezone"].iloc[0] == "UTC"


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_register_table(session):
    """
    Test the session register_table in spark works properly.
    """
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(
                ["2001-01-02 10:00:00.123456789"] * 2 + ["2001-01-03 10:00:00.123456789"] * 3
            ),
            "üser id": [1, 2, 3, 4, 5],
        }
    )
    await session.register_table(table_name="test_table", dataframe=df_training_events)
    df_retrieve = await session.execute_query("SELECT * FROM test_table")
    assert_frame_equal(df_retrieve, df_training_events, check_dtype=False)


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_register_udfs(session):
    """
    Test the session registered udfs properly.
    """
    spark_session = session
    test_table = pd.DataFrame(
        {
            "group": ["A"] * 2 + ["B"] * 2,
            "item": ["apple", "orange", "apple", "orange"],
            "value": [1, 2, 3, 4],
        }
    )
    await spark_session.register_table(table_name="test_table", dataframe=test_table)

    result = await spark_session.execute_query(
        "select `group`, OBJECT_AGG(`item`, `value`) AS counts FROM test_table GROUP BY `group` ORDER BY `group`;"
    )
    assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "group": ["A", "B"],
                "counts": ['{"apple":1,"orange":2}', '{"apple":3,"orange":4}'],
            }
        ),
    )

    result = await spark_session.execute_query(
        "select `group`, F_COUNT_DICT_ENTROPY(OBJECT_AGG(`item`, `value`)) AS counts FROM test_table GROUP BY `group` ORDER BY `group`;"
    )
    assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "group": ["A", "B"],
                "counts": [
                    -(np.log(1 / 3) * 1 / 3 + np.log(2 / 3) * 2 / 3),
                    -(np.log(3 / 7) * 3 / 7 + np.log(4 / 7) * 4 / 7),
                ],
            }
        ),
    )
