"""
This module contains session to BigQuery integration tests.
"""

from collections import OrderedDict

import pandas as pd
import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.session.bigquery import BigQuerySchemaInitializer, BigQuerySession
from featurebyte.session.manager import SessionManager


@pytest.mark.parametrize("source_type", ["bigquery"], indirect=True)
@pytest.mark.asyncio
async def test_schema_initializer(
    config, session_without_datasets, feature_store, credentials_mapping
):
    """
    Test the session initialization in bigquery works properly.
    """
    _ = config
    session = session_without_datasets
    assert isinstance(session, BigQuerySession)
    initializer = BigQuerySchemaInitializer(session)

    # query for the table in the metadata schema table
    get_version_query = (
        f"SELECT * FROM {session.project_name}.{session.dataset_name}.METADATA_SCHEMA"
    )
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
    session_manager = SessionManager(credentials=credentials_mapping)
    session = await session_manager.get_session(feature_store)
    results = await session.execute_query(get_version_query)
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
    session = session_without_datasets
    tables = await session.list_tables(
        database_name=session.project_name, schema_name="demo_datasets"
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
            "table_catalog": "vpc-host-prod-xa739-xz970",
            "table_schema": "demo_datasets",
            "table_name": "groceryinvoice",
            "table_type": "VIEW",
            "is_insertable_into": "NO",
            "is_typed": "NO",
            "creation_time": 1720878239654,
            "base_table_catalog": None,
            "base_table_schema": None,
            "base_table_name": None,
            "snapshot_time_ms": None,
            "ddl": (
                "CREATE VIEW `vpc-host-prod-xa739-xz970.demo_datasets.groceryinvoice`\nOPTIONS(\n  "
                'description="Grocery invoice details, containing the timestamp and the total amount of the invoice."'
                "\n)\nAS SELECT * FROM demo_datasets.__groceryinvoice\nWHERE "
                "record_available_at <= CURRENT_TIMESTAMP();"
            ),
            "default_collation_name": "NULL",
            "upsert_stream_apply_watermark": None,
            "replica_source_catalog": None,
            "replica_source_schema": None,
            "replica_source_name": None,
            "replication_status": None,
            "replication_error": None,
            "is_change_history_enabled": "NO",
            "sync_status": None,
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
@pytest.mark.asyncio
async def test_list_table_schema_decimal(config, session_without_datasets):
    """
    Test handling of DECIMAL numeric type without scale
    """
    _ = config
    session = session_without_datasets
    await session.execute_query(
        "CREATE TABLE TABLE_WITH_DECIMAL AS SELECT CAST(123.45 AS DECIMAL) AS A"
    )
    table_schema = await session.list_table_schema(
        "TABLE_WITH_DECIMAL",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    assert table_schema["A"].dtype == DBVarType.FLOAT
