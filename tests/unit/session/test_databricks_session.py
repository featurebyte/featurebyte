"""
Unit test for DatabricksSession
"""
import os
from unittest import mock
from unittest.mock import call

import pandas as pd
import pytest

from featurebyte.enum import DBVarType
from featurebyte.session.base_spark import BaseSparkSchemaInitializer
from featurebyte.session.databricks import DatabricksSession


@pytest.fixture
def databricks_session_dict():
    """
    DatabricksSession parameters
    """
    return {
        "host": "some-databricks-hostname",
        "http_path": "some-databricks-http-endpoint",
        "featurebyte_catalog": "hive_metastore",
        "featurebyte_schema": "featurebyte",
        "storage_type": "s3",
        "storage_url": "http://some-url/bucket",
        "storage_spark_url": "http://some-url/bucket",
        "database_credential": {
            "type": "ACCESS_TOKEN",
            "access_token": "some-databricks-access-token",
        },
        "storage_credential": {
            "s3_access_key_id": "some-key-id",
            "s3_secret_access_key": "some-key-secret",
        },
    }


class MockDatabricksConnection:
    """
    Mocked Databricks connection
    """

    def __init__(self, *args, **kwargs):
        self.description = None
        self.result_rows = None

    def cursor(self):
        return self

    def catalogs(self):
        self.description = [["TABLE_CAT"]]
        self.result_rows = [["hive_metastore"], ["samples"]]
        return self

    def schemas(self, *args, **kwargs):
        self.description = [["TABLE_SCHEM", "STRING"], ["TABLE_CATALOG", "STRING"]]
        self.result_rows = [
            ["default", "hive_metastore"],
            ["demo", "hive_metastore"],
        ]
        return self

    def tables(self, *args, **kwargs):
        self.description = [["TABLE_CAT"], ["TABLE_SCHEM"], ["TABLE_NAME"]]
        self.result_rows = [
            ["hive_metastore", "default", "transactions"],
            ["hive_metastore", "default", "calls"],
        ]
        return self

    def columns(self, *args, **kwargs):
        self.description = [["COLUMN_NAME"], ["TYPE_NAME"]]
        self.result_rows = [
            ["col_binary", "BINARY"],
            ["col_bool", "BOOLEAN"],
            ["col_date", "DATE"],
            ["col_decimal", "DECIMAL"],
            ["col_double", "DOUBLE"],
            ["col_float", "FLOAT"],
            ["col_int", "INT"],
            ["col_interval", "INTERVAL"],
            ["col_void", "VOID"],
            ["col_timestamp", "TIMESTAMP"],
            ["col_array", "ARRAY"],
            ["col_map", "MAP"],
            ["col_struct", "STRUCT"],
            ["col_string", "STRING"],
            ["col_unknown", "UNKNOWN"],
        ]
        return self

    def fetchall(self):
        return self.result_rows[:]

    def fetchall_arrow(self):
        columns = [cols[0] for cols in self.description]
        data = []
        for row in self.result_rows:
            data.append({k: v for (k, v) in zip(columns, row)})
        df = pd.DataFrame(data)
        mock_arrow_table = mock.Mock()
        mock_arrow_table.to_pandas.return_value = df
        return mock_arrow_table

    def fetchmany_arrow(self, size):
        mock_dataframe = self.fetchall_arrow().to_pandas()
        for i in range(mock_dataframe.shape[0], size):
            yield mock_dataframe.iloc[i:size]

    def execute(self, *args, **kwargs):
        self.description = [["a", "INT"], ["b", "INT"], ["c", "INT"]]
        self.result_rows = [
            [1, 2, 3],
            [100, 200, 300],
        ]
        return self

    def close(self):
        pass


@pytest.fixture
def databricks_connection():
    """
    Mock databricks connector in featurebyte.session.databricks module
    """
    with mock.patch("featurebyte.session.databricks.databricks_sql") as mock_connector:
        mock_connector.connect.return_value = MockDatabricksConnection()
        yield mock_connector


@pytest.fixture(name="patched_databricks_session_cls")
def patched_databricks_session_cls_fixture(
    databricks_session_dict,
):
    """Fixture for a patched session class"""
    with mock.patch(
        "featurebyte.session.databricks.DatabricksSession", autospec=True
    ) as patched_class:
        mock_session_obj = patched_class.return_value
        mock_session_obj.database_name = databricks_session_dict["featurebyte_catalog"]
        mock_session_obj.schema_name = databricks_session_dict["featurebyte_schema"]
        yield patched_class


@pytest.mark.usefixtures("databricks_connection")
@pytest.mark.asyncio
async def test_databricks_session(databricks_session_dict):
    """
    Test DatabricksSession
    """
    with mock.patch("featurebyte.session.base_spark.S3SimpleStorage", autospec=True) as _:
        session = DatabricksSession(**databricks_session_dict)

    assert session.host == "some-databricks-hostname"
    assert session.http_path == "some-databricks-http-endpoint"
    assert session.database_credential.access_token == "some-databricks-access-token"
    assert await session.list_databases() == ["hive_metastore", "samples"]
    assert await session.list_schemas(database_name="hive_metastore") == ["default", "demo"]
    assert await session.list_tables(database_name="hive_metastore", schema_name="default") == [
        "transactions",
        "calls",
    ]
    assert await session.list_table_schema(
        database_name="hive_metastore", schema_name="default", table_name="transactions"
    ) == {
        "col_binary": DBVarType.BINARY,
        "col_bool": DBVarType.BOOL,
        "col_date": DBVarType.DATE,
        "col_decimal": DBVarType.FLOAT,
        "col_double": DBVarType.FLOAT,
        "col_float": DBVarType.FLOAT,
        "col_int": DBVarType.INT,
        "col_interval": DBVarType.TIMEDELTA,
        "col_void": DBVarType.VOID,
        "col_timestamp": DBVarType.TIMESTAMP,
        "col_array": DBVarType.ARRAY,
        "col_map": DBVarType.MAP,
        "col_struct": DBVarType.STRUCT,
        "col_string": DBVarType.VARCHAR,
        "col_unknown": DBVarType.UNKNOWN,
    }
    df_result = await session.execute_query("SELECT * FROM table")
    df_expected = pd.DataFrame({"a": [1, 100], "b": [2, 200], "c": [3, 300]})
    pd.testing.assert_frame_equal(df_result, df_expected)


@pytest.mark.parametrize("temporary", [True, False])
@pytest.mark.asyncio
async def test_databricks_register_table(databricks_session_dict, databricks_connection, temporary):
    """
    Test Databricks session register_table
    """
    with mock.patch(
        "featurebyte.session.databricks.DatabricksSession.execute_query"
    ) as mock_execute_query:
        with mock.patch("featurebyte.session.base_spark.S3SimpleStorage", autospec=True) as _:
            with mock.patch(
                "featurebyte.session.databricks.pd.DataFrame.to_parquet", autospec=True
            ) as _:
                session = DatabricksSession(**databricks_session_dict)
                df = pd.DataFrame(
                    {
                        "point_in_time": pd.to_datetime(["2022-01-01", "2022-01-02", "2022-01-03"]),
                        "cust_id": [1, 2, 3],
                    },
                )
                if temporary:
                    expected = "CREATE OR REPLACE TEMPORARY VIEW"
                else:
                    expected = "CREATE TABLE"
                await session.register_table("my_view", df, temporary)

                if temporary:
                    assert mock_execute_query.call_args_list[0][0][0].startswith(expected)
                else:
                    assert mock_execute_query.call_args_list[-1][0][0].startswith(expected)


def test_databricks_sql_connector_not_available(databricks_session_dict):
    """
    Simulate missing databricks-sql-connector dependency
    """
    with mock.patch("featurebyte.session.base_spark.S3SimpleStorage", autospec=True) as _:
        with mock.patch("featurebyte.session.databricks.HAS_DATABRICKS_SQL_CONNECTOR", False):
            with pytest.raises(RuntimeError) as exc:
                _ = DatabricksSession(**databricks_session_dict)
            assert str(exc.value) == "databricks-sql-connector is not available"


def test_databricks_schema_initializer__sql_objects(patched_databricks_session_cls):
    session = patched_databricks_session_cls()
    sql_objects = BaseSparkSchemaInitializer(session).get_sql_objects()
    for item in sql_objects:
        item["filename"] = os.path.basename(item["filename"])
        item["type"] = item["type"].value
    expected = [
        {
            "type": "table",
            "filename": "T_ONLINE_STORE_MAPPING.sql",
            "identifier": "ONLINE_STORE_MAPPING",
        },
        {
            "type": "table",
            "filename": "T_TILE_FEATURE_MAPPING.sql",
            "identifier": "TILE_FEATURE_MAPPING",
        },
        {"type": "table", "filename": "T_TILE_JOB_MONITOR.sql", "identifier": "TILE_JOB_MONITOR"},
        {
            "type": "table",
            "filename": "T_TILE_MONITOR_SUMMARY.sql",
            "identifier": "TILE_MONITOR_SUMMARY",
        },
        {"type": "table", "filename": "T_TILE_REGISTRY.sql", "identifier": "TILE_REGISTRY"},
    ]

    def _sorted_result(lst):
        return sorted(lst, key=lambda x: x["filename"])

    assert _sorted_result(sql_objects) == _sorted_result(expected)


@pytest.mark.asyncio
async def test_databricks_schema_initializer__commands(patched_databricks_session_cls):
    """Test Databricks schema initializer dispatches the correct queries"""
    session = patched_databricks_session_cls()
    initializer = BaseSparkSchemaInitializer(session)

    await initializer.list_functions()
    assert session.execute_query.call_args_list == [call("SHOW USER FUNCTIONS IN featurebyte")]

    assert await initializer.list_procedures() == []
    assert session.execute_query.call_args_list == [call("SHOW USER FUNCTIONS IN featurebyte")]
