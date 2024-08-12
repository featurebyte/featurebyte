"""
This module contains session to BigQuery integration tests.
"""

import pytest
from google.cloud.bigquery import StandardSqlTypeNames

from featurebyte.enum import DBVarType
from featurebyte.session.bigquery import BigQuerySchemaInitializer, BigQuerySession
from featurebyte.session.manager import SessionManager


@pytest.mark.parametrize("source_type", ["bigquery"], indirect=True)
@pytest.mark.asyncio
async def test_schema_initializer(config, feature_store, credentials_mapping):
    """
    Test the session initialization in bigquery works properly.
    """
    _ = config
    session_manager = SessionManager(credentials=credentials_mapping)
    session = await session_manager.get_session(feature_store)
    assert isinstance(session, BigQuerySession)
    initializer = BigQuerySchemaInitializer(session)

    # query for the table in the metadata schema table
    get_version_query = f"SELECT * FROM {feature_store.details.project_name}.{feature_store.details.dataset_name}.METADATA_SCHEMA"
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


@pytest.mark.parametrize("source_type", ["bigquery"], indirect=True)
@pytest.mark.asyncio
async def test_list_tables(config, feature_store, credentials_mapping):
    """
    Test the session initialization in bigquery works properly.
    """
    _ = config
    session_manager = SessionManager(credentials=credentials_mapping)
    session = await session_manager.get_session(feature_store)

    tables = await session.list_tables(
        database_name=feature_store.details.project_name, schema_name="demo_datasets"
    )
    assert [table.dict() for table in tables] == [
        {"name": "__grocerycustomer", "description": ""},
        {"name": "__groceryinvoice", "description": None},
        {"name": "__invoiceitems", "description": None},
        {
            "name": "grocerycustomer",
            "description": "Customer details, including their name, address, and date of birth.",
        },
        {
            "name": "groceryinvoice",
            "description": "Grocery invoice details, containing the timestamp and the "
            "total amount of the invoice.",
        },
        {
            "name": "groceryproduct",
            "description": "The product group description for each grocery product.",
        },
        {
            "name": "invoiceitems",
            "description": "The grocery item details within each invoice, including the "
            "quantity, total cost, discount applied, and product ID.",
        },
    ]


@pytest.mark.parametrize(
    "bigquery_var_info,scale,expected",
    [
        ("INTEGER", 0, DBVarType.INT),
        ("INT64", 0, DBVarType.INT),
        ("BOOLEAN", 0, DBVarType.BOOL),
        ("BOOL", 0, DBVarType.BOOL),
        ("FLOAT", 0, DBVarType.FLOAT),
        ("FLOAT64", 0, DBVarType.FLOAT),
        ("STRING", 0, DBVarType.VARCHAR),
        ("BYTES", 0, DBVarType.BINARY),
        ("TIMESTAMP", 0, DBVarType.TIMESTAMP),
        ("DATETIME", 0, DBVarType.TIMESTAMP),
        ("DATE", 0, DBVarType.DATE),
        ("TIME", 0, DBVarType.TIME),
        ("NUMERIC", 0, DBVarType.INT),
        ("NUMERIC", 1, DBVarType.FLOAT),
        ("BIGNUMERIC", 0, DBVarType.INT),
        ("BIGNUMERIC", 1, DBVarType.FLOAT),
        ("STRUCT", 0, DBVarType.DICT),
        ("RECORD", 0, DBVarType.DICT),
        (StandardSqlTypeNames.INTERVAL, 0, DBVarType.TIMEDELTA),
        (StandardSqlTypeNames.ARRAY, 0, DBVarType.ARRAY),
        ("GEOGRAPHY", 0, DBVarType.UNKNOWN),
        (StandardSqlTypeNames.JSON, 0, DBVarType.UNKNOWN),
        (StandardSqlTypeNames.RANGE, 0, DBVarType.UNKNOWN),
    ],
)
def test_convert_to_internal_variable_type(bigquery_var_info, scale, expected):
    """
    Test convert_to_internal_variable_type
    """
    assert BigQuerySession._convert_to_internal_variable_type(bigquery_var_info, scale) == expected  # pylint: disable=protected-access
