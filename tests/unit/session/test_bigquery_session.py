"""
Unit test for BigQuerySession
"""

import datetime
import os
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from dateutil import tz
from google.cloud.bigquery import SchemaField, StandardSqlTypeNames
from google.cloud.bigquery.dbapi.cursor import Column
from google.cloud.bigquery.table import Row

from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.session.bigquery import (
    BigQuerySchemaInitializer,
    BigQuerySession,
    convert_to_internal_variable_type,
)


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

    def list_projects(self, page_token=None):
        return MockIterator([
            Mock(project_id="vpc-host-prod-xa739-xz970"),
            Mock(project_id="vpc-host-nonprod-xa739-xz970"),
        ])

    def list_datasets(self, project, page_token=None):
        return MockIterator([Mock(dataset_id="creditcard"), Mock(dataset_id="demo_datasets")])

    def list_tables(self, dataset, page_token=None):
        return MockIterator([Mock(table_id="calls"), Mock(table_id="transactions")])

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
                SchemaField("col_json", "JSON", description="JSON Column"),
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

    _query_rows = Mock(
        schema=[
            SchemaField(name="a", field_type="INTEGER"),
            SchemaField(name="b", field_type="INTEGER"),
            SchemaField(name="c", field_type="INTEGER"),
        ]
    )

    def __init__(self, *args, **kwargs):
        self.description = None
        self.result_rows = None
        self.returned_count = 0
        self.query_job = Mock()

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
        patch("featurebyte.session.bigquery.Client.__new__") as mock_client,
        patch("featurebyte.session.bigquery.Connection.__new__") as mock_connector,
    ):
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
        "vpc-host-nonprod-xa739-xz970",
        "vpc-host-prod-xa739-xz970",
    ]
    assert await session.list_schemas(database_name="vpc-host-prod-xa739-xz970") == [
        "creditcard",
        "demo_datasets",
    ]
    tables = await session.list_tables(
        database_name="vpc-host-prod-xa739-xz970", schema_name="creditcard"
    )
    assert [table.name for table in tables] == [
        "calls",
        "transactions",
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
        "col_json": ColumnSpecWithDescription(
            name="col_json", dtype=DBVarType.DICT, description="JSON Column"
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


def test_bigquery_schema_initializer__sql_objects(patched_bigquery_session_cls):
    session = patched_bigquery_session_cls()
    sql_objects = BigQuerySchemaInitializer(session).get_sql_objects()
    for item in sql_objects:
        item["filename"] = os.path.basename(item["filename"])
        item["type"] = item["type"].value
    expected = [
        {
            "type": "function",
            "filename": "F_COUNT_DICT_COSINE_SIMILARITY.sql",
            "identifier": "F_COUNT_DICT_COSINE_SIMILARITY",
        },
        {
            "type": "function",
            "filename": "F_COUNT_DICT_ENTROPY.sql",
            "identifier": "F_COUNT_DICT_ENTROPY",
        },
        {
            "type": "function",
            "filename": "F_COUNT_DICT_LEAST_FREQUENT.sql",
            "identifier": "F_COUNT_DICT_LEAST_FREQUENT",
        },
        {
            "type": "function",
            "filename": "F_COUNT_DICT_MOST_FREQUENT.sql",
            "identifier": "F_COUNT_DICT_MOST_FREQUENT",
        },
        {
            "type": "function",
            "filename": "F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE.sql",
            "identifier": "F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE",
        },
        {
            "type": "function",
            "filename": "F_COUNT_DICT_MOST_FREQUENT_VALUE.sql",
            "identifier": "F_COUNT_DICT_MOST_FREQUENT_VALUE",
        },
        {
            "type": "function",
            "filename": "F_COUNT_DICT_NORMALIZE.sql",
            "identifier": "F_COUNT_DICT_NORMALIZE",
        },
        {
            "type": "function",
            "filename": "F_COUNT_DICT_NUM_UNIQUE.sql",
            "identifier": "F_COUNT_DICT_NUM_UNIQUE",
        },
        {"type": "function", "filename": "F_GET_RANK.sql", "identifier": "F_GET_RANK"},
        {
            "type": "function",
            "filename": "F_GET_RELATIVE_FREQUENCY.sql",
            "identifier": "F_GET_RELATIVE_FREQUENCY",
        },
        {"type": "function", "filename": "F_GET_VALUE.sql", "identifier": "F_GET_VALUE"},
        {
            "type": "function",
            "filename": "F_INDEX_TO_TIMESTAMP.sql",
            "identifier": "F_INDEX_TO_TIMESTAMP",
        },
        {"type": "function", "filename": "F_OBJECT_DELETE.sql", "identifier": "OBJECT_DELETE"},
        {
            "type": "function",
            "filename": "F_TIMESTAMP_TO_INDEX.sql",
            "identifier": "F_TIMESTAMP_TO_INDEX",
        },
        {
            "type": "function",
            "filename": "F_TIMEZONE_OFFSET_TO_SECOND.sql",
            "identifier": "F_TIMEZONE_OFFSET_TO_SECOND",
        },
        {
            "type": "function",
            "filename": "F_VECTOR_AGGREGATE_MAX.sql",
            "identifier": "F_VECTOR_AGGREGATE_MAX",
        },
        {
            "type": "function",
            "filename": "F_VECTOR_AGGREGATE_SIMPLE_AVERAGE.sql",
            "identifier": "F_VECTOR_AGGREGATE_SIMPLE_AVERAGE",
        },
        {
            "type": "function",
            "filename": "F_VECTOR_AGGREGATE_SIMPLE_AVG.sql",
            "identifier": "F_VECTOR_AGGREGATE_SIMPLE_AVG",
        },
        {
            "type": "function",
            "filename": "F_VECTOR_AGGREGATE_SUM.sql",
            "identifier": "F_VECTOR_AGGREGATE_SUM",
        },
        {
            "type": "function",
            "filename": "F_VECTOR_COSINE_SIMILARITY.sql",
            "identifier": "F_VECTOR_COSINE_SIMILARITY",
        },
    ]

    def _sorted_result(lst):
        return sorted(lst, key=lambda x: x["filename"])

    assert _sorted_result(sql_objects) == _sorted_result(expected)


@pytest.mark.parametrize(
    "bigquery_var_info,scale,expected",
    [
        ("INTEGER", 0, DBVarType.INT),
        ("BOOLEAN", 0, DBVarType.BOOL),
        ("FLOAT", 0, DBVarType.FLOAT),
        ("STRING", 0, DBVarType.VARCHAR),
        ("BYTES", 0, DBVarType.BINARY),
        ("TIMESTAMP", 0, DBVarType.TIMESTAMP),
        ("DATETIME", 0, DBVarType.TIMESTAMP),
        ("DATE", 0, DBVarType.DATE),
        ("TIME", 0, DBVarType.TIME),
        ("NUMERIC", None, DBVarType.FLOAT),
        ("NUMERIC", 0, DBVarType.INT),
        ("NUMERIC", 1, DBVarType.FLOAT),
        ("BIGNUMERIC", None, DBVarType.FLOAT),
        ("BIGNUMERIC", 0, DBVarType.INT),
        ("BIGNUMERIC", 1, DBVarType.FLOAT),
        ("RECORD", 0, DBVarType.DICT),
        (StandardSqlTypeNames.INTERVAL, 0, DBVarType.TIMEDELTA),
        (StandardSqlTypeNames.ARRAY, 0, DBVarType.ARRAY),
        ("GEOGRAPHY", 0, DBVarType.UNKNOWN),
        (StandardSqlTypeNames.JSON, 0, DBVarType.DICT),
        (StandardSqlTypeNames.RANGE, 0, DBVarType.UNKNOWN),
    ],
)
def test_convert_to_internal_variable_type(bigquery_var_info, scale, expected):
    """
    Test convert_to_internal_variable_type
    """
    assert convert_to_internal_variable_type(bigquery_var_info, scale) == expected  # pylint: disable=protected-access


@pytest.mark.parametrize(
    "value,expected",
    [
        (
            {"date": datetime.datetime(2023, 1, 1, 8, 0, 0, tzinfo=tz.gettz("Asia/Singapore"))},
            '{"date": "2023-01-01T00:00:00"}',
        ),
        ({"date": datetime.datetime(2023, 1, 1, 8, 0, 0)}, '{"date": "2023-01-01T08:00:00"}'),
    ],
)
def test_json_serialization_handler(value, expected):
    """
    Test json_serialization_handler
    """

    assert BigQuerySession._format_value(value) == expected  # pylint: disable=protected-access
