"""
This module contains session to BigQuery integration tests.
"""

import os
from collections import OrderedDict
from decimal import Decimal
from unittest.mock import patch

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import session_cache
from featurebyte.session.bigquery import BigQuerySchemaInitializer, BigQuerySession


@pytest.mark.parametrize("source_type", ["bigquery"], indirect=True)
@pytest.mark.asyncio
async def test_schema_initializer(
    config,
    session_without_datasets,
    feature_store,
    feature_store_credential,
    session_manager_service,
):
    """
    Test the session initialization in bigquery works properly.
    """
    _ = config
    db_session = session_without_datasets
    assert isinstance(db_session, BigQuerySession)
    initializer = BigQuerySchemaInitializer(db_session)

    # query for the table in the metadata schema table
    get_version_query = (
        f"SELECT * FROM {db_session.project_name}.{db_session.dataset_name}.METADATA_SCHEMA"
    )
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

    # test list functions
    functions = await initializer.list_objects("USER FUNCTIONS")
    assert set(functions.name.tolist()).issuperset({"F_INDEX_TO_TIMESTAMP", "F_TIMESTAMP_TO_INDEX"})

    # test list tables
    tables = await initializer.list_objects("TABLES")
    assert set(tables.name.tolist()).issuperset({"METADATA_SCHEMA"})


@pytest.mark.parametrize("source_type", ["bigquery"], indirect=True)
@pytest.mark.asyncio
async def test_list_tables(config, session_without_datasets):
    """
    Test the session initialization in bigquery works properly.
    """
    _ = config
    db_session = session_without_datasets
    tables = await db_session.list_tables(
        database_name=db_session.project_name, schema_name="demo_datasets"
    )
    assert [table.dict() for table in tables] == [
        {"name": "__grocerycustomer", "description": None},
        {"name": "__groceryinvoice", "description": None},
        {"name": "__invoiceitems", "description": None},
        {
            "name": "grocerycustomer",
            "description": None,
        },
        {
            "name": "groceryinvoice",
            "description": None,
        },
        {
            "name": "groceryproduct",
            "description": None,
        },
        {
            "name": "invoiceitems",
            "description": None,
        },
    ]


@pytest.mark.parametrize("source_type", ["bigquery"], indirect=True)
@pytest.mark.asyncio
async def test_list_table_schema(config, session_without_datasets):
    _ = config
    session = session_without_datasets
    schema = await session.list_table_schema(
        database_name=session.project_name, schema_name="demo_datasets", table_name="groceryinvoice"
    )
    assert schema == OrderedDict([
        (
            "GroceryInvoiceGuid",
            ColumnSpecWithDescription(name="GroceryInvoiceGuid", dtype="VARCHAR", description=None),
        ),
        (
            "GroceryCustomerGuid",
            ColumnSpecWithDescription(
                name="GroceryCustomerGuid", dtype="VARCHAR", description=None
            ),
        ),
        (
            "Timestamp",
            ColumnSpecWithDescription(name="Timestamp", dtype="TIMESTAMP", description=None),
        ),
        (
            "tz_offset",
            ColumnSpecWithDescription(name="tz_offset", dtype="VARCHAR", description=None),
        ),
        (
            "record_available_at",
            ColumnSpecWithDescription(
                name="record_available_at", dtype="TIMESTAMP", description=None
            ),
        ),
        ("Amount", ColumnSpecWithDescription(name="Amount", dtype="FLOAT", description=None)),
    ])


@pytest.mark.parametrize("source_type", ["bigquery"], indirect=True)
@pytest.mark.asyncio
async def test_get_table_details(config, session_without_datasets):
    _ = config
    session = session_without_datasets
    details = await session.get_table_details(
        database_name=session.project_name, schema_name="demo_datasets", table_name="groceryinvoice"
    )
    assert details.dict() == {
        "details": {
            "kind": "bigquery#table",
            "etag": "0IcsjoqRbIZEJ3K/m3Z+bg==",
            "id": "vpc-host-prod-xa739-xz970:demo_datasets.groceryinvoice",
            "selfLink": "https://bigquery.googleapis.com/bigquery/v2/projects/vpc-host-prod-xa739-xz970/datasets/demo_datasets/tables/groceryinvoice",
            "tableReference": {
                "projectId": "vpc-host-prod-xa739-xz970",
                "datasetId": "demo_datasets",
                "tableId": "groceryinvoice",
            },
            "numBytes": "0",
            "numLongTermBytes": "0",
            "numRows": "0",
            "creationTime": "1762855921592",
            "lastModifiedTime": "1762927950238",
            "type": "VIEW",
            "view": {
                "query": "SELECT * FROM `vpc-host-prod-xa739-xz970.demo_datasets.__groceryinvoice`\nWHERE record_available_at <= CURRENT_TIMESTAMP()",
                "useLegacySql": False,
            },
            "location": "US",
            "numTotalLogicalBytes": "0",
            "numActiveLogicalBytes": "0",
            "numLongTermLogicalBytes": "0",
        },
        "fully_qualified_name": "`vpc-host-prod-xa739-xz970`.`demo_datasets`.`groceryinvoice`",
        "description": "Grocery invoice details, containing the timestamp and the total amount of the invoice.",
    }


@pytest.mark.parametrize("source_type", ["bigquery"], indirect=True)
@pytest.mark.asyncio
async def test_register_table(config, session_without_datasets):
    _ = config
    session = session_without_datasets
    df = pd.DataFrame({"a": [1, 2, 3], "date": pd.date_range("2021-01-01", periods=3)})
    await session.register_table(table_name="test_table", dataframe=df)


@pytest.mark.parametrize("source_type", ["bigquery"], indirect=True)
@pytest.mark.parametrize(
    "timestamp_column, expected",
    [
        (
            pd.to_datetime(["2021-01-01 10:00:00"]),
            [pd.Timestamp("2021-01-01 10:00:00")],
        ),
        (
            pd.to_datetime(["2021-01-01 10:00:00+08:00"]),
            [pd.Timestamp("2021-01-01 02:00:00")],
        ),
        # Mixed offsets with python datetime objects in Series
        (
            pd.Series([
                pd.Timestamp("2021-01-01 10:00:00+0800").to_pydatetime(),
                pd.Timestamp("2021-01-01 10:00:00-0800").to_pydatetime(),
            ]),
            [
                pd.Timestamp("2021-01-01 02:00:00"),
                pd.Timestamp("2021-01-01 18:00:00"),
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_register_table_timestamp_type(
    config,
    session_without_datasets,
    timestamp_column,
    expected,
):
    """
    Test timestamp handling in register_table
    """
    _ = config
    session = session_without_datasets
    df = pd.DataFrame({"ts_col": timestamp_column})
    table_name = f"test_register_table_timestamp_type_{ObjectId()}"
    await session.register_table(table_name=table_name, dataframe=df)
    result = await session.execute_query(f"SELECT * FROM {table_name}")
    assert result["ts_col"].tolist() == expected


@pytest.mark.parametrize("source_type", ["bigquery"], indirect=True)
@pytest.mark.asyncio
async def test_list_table_schema_decimal(config, session_without_datasets):
    """
    Test handling of DECIMAL numeric type without scale
    """
    _ = config
    session = session_without_datasets
    await session.execute_query("CREATE TABLE TABLE_DECIMAL AS SELECT CAST(123.45 AS DECIMAL) AS A")
    table_schema = await session.list_table_schema(
        "TABLE_DECIMAL",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    assert table_schema["A"].dtype == DBVarType.FLOAT
    df = await session.execute_query("SELECT * FROM TABLE_DECIMAL")
    assert df.iloc[0]["A"] == Decimal("123.450000000")


@pytest.mark.parametrize("source_type", ["bigquery"], indirect=True)
@pytest.mark.asyncio
async def test_list_table_schema_decimal_parameterized(config, session_without_datasets):
    """
    Test handling of parameterized DECIMAL numeric type
    """
    _ = config
    session = session_without_datasets
    await session.execute_query("CREATE TABLE TABLE_DECIMAL_SCALE (A DECIMAL(10, 3))")
    await session.execute_query("INSERT INTO TABLE_DECIMAL_SCALE (A) VALUES (1.234)")
    table_schema = await session.list_table_schema(
        "TABLE_DECIMAL_SCALE",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    assert table_schema["A"].dtype == DBVarType.FLOAT
    df = await session.execute_query("SELECT * FROM TABLE_DECIMAL_SCALE")
    assert df.iloc[0]["A"] == Decimal("1.234")


@pytest.mark.parametrize("source_type", ["bigquery"], indirect=True)
@pytest.mark.parametrize(
    "hostname, bqstorage_client_expected",
    [
        ("featurebyte-worker-io", False),
        ("featurebyte-worker-cpu", True),
    ],
)
@pytest.mark.asyncio
async def test_create_session__bqstorage_client(
    feature_store, feature_store_credential, hostname, bqstorage_client_expected
):
    """
    Test bqstorage client is disabled when running on an IO worker.
    """
    session_cache.clear()
    with patch.dict(os.environ, {"HOSTNAME": hostname}):
        session = await SessionManagerService.get_session(feature_store, feature_store_credential)
    if bqstorage_client_expected:
        assert session.connection._bqstorage_client is not None
    else:
        assert session.connection._bqstorage_client is None
