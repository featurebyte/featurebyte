"""
Unit test for BigQuerySession
"""

import os
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.dbapi.cursor import Column
from google.cloud.bigquery.table import Row

from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.session.bigquery import BigQuerySchemaInitializer, BigQuerySession


@pytest.fixture
def bigquery_session_dict():
    """
    BigQuerySession parameters
    """
    return {
        "project_name": "some-bigquery-project",
        "dataset_name": "some-bigquery-dataset",
        "database_credential": {
            "type": "GOOGLE",
            "service_account_info": {"client_email": "test@featurebyte.com"},
        },
    }


class MockIterator(list):
    """
    Mocked BigQuery iterator
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.index = 0
        self.next_page_token = None

    def __iter__(self):
        return self

    def __next__(self):
        if self.index >= len(self):
            raise StopIteration
        result = self[self.index]
        self.index += 1
        return result


class MockBigQueryClient:
    """
    Mocked BigQuery client
    """

    load_table_from_dataframe = AsyncMock()

    def list_projects(self, page_token=None):
        return MockIterator([
            Mock(project_id="vpc-host-prod-xa739-xz970"),
            Mock(project_id="vpc-host-nonprod-xa739-xz970"),
        ])

    def list_datasets(self, project, page_token=None):
        return MockIterator([Mock(dataset_id="demo_datasets"), Mock(dataset_id="creditcard")])

    def list_tables(self, dataset, page_token=None):
        return MockIterator([Mock(table_id="transactions"), Mock(table_id="calls")])

    def get_table(self, table):
        return Mock(
            description="some description",
            schema=[
                SchemaField("col_binary", "BYTES", description="Binary Column"),
                SchemaField("col_bool", "BOOLEAN", description="Boolean Column"),
                SchemaField("col_date", "DATE", description="Date Column"),
                SchemaField(
                    "col_numeric", "NUMERIC", description="Numeric Column", scale=1, precision=10
                ),
                SchemaField("col_double", "FLOAT", description="Double Column"),
                SchemaField("col_float", "FLOAT", description="Float Column"),
                SchemaField("col_int", "INTEGER", description="Int Column"),
                SchemaField("col_interval", "INTERVAL", description="Interval Column"),
                SchemaField("col_timestamp", "TIMESTAMP", description="Timestamp Column"),
                SchemaField("col_array", "ARRAY", description="Array Column"),
                SchemaField("col_record", "RECORD", description="Record Column"),
                SchemaField("col_struct", "STRUCT", description="Struct Column"),
                SchemaField("col_string", "STRING", description="String Column"),
                SchemaField("col_unknown", "GEOGRAPHY", description="Unknown Column"),
            ],
        )

    def close(self):
        pass


class MockBigQueryConnection:
    """
    Mocked BigQuery connection
    """

    def __init__(self, *args, **kwargs):
        self.description = None
        self.result_rows = None
        self.returned_count = 0

    def cursor(self):
        return self

    def fetchall(self):
        return self.result_rows[:]

    def fetchmany(self, size):
        records = self.fetchall()
        if self.returned_count >= len(records):
            return []
        partial_records = records[self.returned_count : self.returned_count + size]
        self.returned_count += size
        return partial_records

    def execute(self, *args, **kwargs):
        self.description = [
            Column("a", "INTEGER", None, None, None, None, None),
            Column("b", "INTEGER", None, None, None, None, None),
            Column("c", "INTEGER", None, None, None, None, None),
        ]
        field_to_index = {"a": 0, "b": 1, "c": 2}
        self.result_rows = [
            Row([1, 2, 3], field_to_index),
            Row([100, 200, 300], field_to_index),
        ]
        self.returned_count = 0
        return self

    def close(self):
        pass


@pytest.fixture
def bigquery_connection():
    """
    Mock bigquery connector in featurebyte.session.bigquery module
    """
    with (
        patch("featurebyte.session.bigquery.service_account.Credentials.from_service_account_info"),
        patch("featurebyte.session.bigquery.bigquery.Client.__new__") as mock_client,
        patch("featurebyte.session.bigquery.Connection.__new__") as mock_connector,
    ):
        with patch("featurebyte.session.bigquery.Connection", autospec=True) as mock_connector:
            mock_client.return_value = MockBigQueryClient()
            mock_connector.return_value = MockBigQueryConnection()
            yield mock_connector


@pytest.fixture(name="patched_bigquery_session_cls")
def patched_bigquery_session_cls_fixture(
    bigquery_session_dict,
):
    """Fixture for a patched session class"""
    with patch("featurebyte.session.bigquery.BigQuerySession", autospec=True) as patched_class:
        mock_session_obj = patched_class.return_value
        mock_session_obj.database_name = bigquery_session_dict["project_name"]
        mock_session_obj.schema_name = bigquery_session_dict["dataset_name"]
        yield patched_class


@pytest.mark.usefixtures("bigquery_connection")
@pytest.mark.asyncio
async def test_bigquery_session(bigquery_session_dict):
    """
    Test BigQuerySession
    """
    session = BigQuerySession(**bigquery_session_dict)

    assert session.project_name == "some-bigquery-project"
    assert session.dataset_name == "some-bigquery-dataset"
    assert session.database_credential.service_account_info == {
        "client_email": "test@featurebyte.com"
    }
    assert await session.list_databases() == [
        "vpc-host-prod-xa739-xz970",
        "vpc-host-nonprod-xa739-xz970",
    ]
    assert await session.list_schemas(database_name="vpc-host-prod-xa739-xz970") == [
        "demo_datasets",
        "creditcard",
    ]
    tables = await session.list_tables(
        database_name="vpc-host-prod-xa739-xz970", schema_name="creditcard"
    )
    assert [table.name for table in tables] == [
        "transactions",
        "calls",
    ]
    assert await session.list_table_schema(
        database_name="hive_metastore", schema_name="default", table_name="transactions"
    ) == {
        "col_binary": ColumnSpecWithDescription(
            name="col_binary", dtype=DBVarType.BINARY, description="Binary Column"
        ),
        "col_bool": ColumnSpecWithDescription(
            name="col_bool", dtype=DBVarType.BOOL, description="Boolean Column"
        ),
        "col_date": ColumnSpecWithDescription(
            name="col_date", dtype=DBVarType.DATE, description="Date Column"
        ),
        "col_double": ColumnSpecWithDescription(
            name="col_double", dtype=DBVarType.FLOAT, description="Double Column"
        ),
        "col_float": ColumnSpecWithDescription(
            name="col_float", dtype=DBVarType.FLOAT, description="Float Column"
        ),
        "col_int": ColumnSpecWithDescription(
            name="col_int", dtype=DBVarType.INT, description="Int Column"
        ),
        "col_interval": ColumnSpecWithDescription(
            name="col_interval", dtype=DBVarType.TIMEDELTA, description="Interval Column"
        ),
        "col_timestamp": ColumnSpecWithDescription(
            name="col_timestamp", dtype=DBVarType.TIMESTAMP, description="Timestamp Column"
        ),
        "col_array": ColumnSpecWithDescription(
            name="col_array", dtype=DBVarType.ARRAY, description="Array Column"
        ),
        "col_record": ColumnSpecWithDescription(
            name="col_record", dtype=DBVarType.DICT, description="Record Column"
        ),
        "col_numeric": ColumnSpecWithDescription(
            name="col_numeric", dtype=DBVarType.FLOAT, description="Numeric Column"
        ),
        "col_struct": ColumnSpecWithDescription(
            name="col_struct", dtype=DBVarType.DICT, description="Struct Column"
        ),
        "col_string": ColumnSpecWithDescription(
            name="col_string", dtype=DBVarType.VARCHAR, description="String Column"
        ),
        "col_unknown": ColumnSpecWithDescription(
            name="col_unknown", dtype=DBVarType.UNKNOWN, description="Unknown Column"
        ),
    }
    df_result = await session.execute_query("SELECT * FROM table")
    df_expected = pd.DataFrame({"a": [1, 100], "b": [2, 200], "c": [3, 300]}, dtype="int64")
    pd.testing.assert_frame_equal(df_result, df_expected)


@pytest.mark.asyncio
async def test_bigquery_register_table(bigquery_session_dict, bigquery_connection):
    """
    Test BigQuery session register_table
    """
    session = BigQuerySession(**bigquery_session_dict)
    df = pd.DataFrame(
        {
            "point_in_time": pd.to_datetime(["2022-01-01", "2022-01-02", "2022-01-03"]),
            "cust_id": [1, 2, 3],
        },
    )
    await session.register_table("my_view", df)
    session._client.load_table_from_dataframe.assert_called_once_with(
        df, "`some-bigquery-project`.`some-bigquery-dataset`.`my_view`"
    )


def test_bigquery_schema_initializer__sql_objects(patched_bigquery_session_cls):
    session = patched_bigquery_session_cls()
    sql_objects = BigQuerySchemaInitializer(session).get_sql_objects()
    for item in sql_objects:
        item["filename"] = os.path.basename(item["filename"])
        item["type"] = item["type"].value
    expected = [
        {
            "type": "function",
            "filename": "F_INDEX_TO_TIMESTAMP.sql",
            "identifier": "F_INDEX_TO_TIMESTAMP",
        },
        {
            "type": "function",
            "filename": "F_TIMESTAMP_TO_INDEX.sql",
            "identifier": "F_TIMESTAMP_TO_INDEX",
        },
    ]

    def _sorted_result(lst):
        return sorted(lst, key=lambda x: x["filename"])

    assert _sorted_result(sql_objects) == _sorted_result(expected)
