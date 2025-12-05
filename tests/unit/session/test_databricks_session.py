"""
Unit test for DatabricksSession
"""

import os
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pytest
from databricks.sdk.service.catalog import CatalogInfo, ColumnInfo, SchemaInfo, TableInfo

from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.dtype import NestedFieldMetadata, PartitionMetadata
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
        "catalog_name": "hive_metastore",
        "schema_name": "featurebyte",
        "storage_path": "dbfs:/some/path",
        "database_credential": {
            "type": "ACCESS_TOKEN",
            "access_token": "some-databricks-access-token",
        },
    }


class MockDatabricksConnection:
    """
    Mocked Databricks connection
    """

    def __init__(self, *args, **kwargs):
        self.description = None
        self.result_rows = None
        self.returned_count = 0

    def cursor(self):
        return self

    def fetchall(self):
        return self.result_rows[:]

    def fetchall_arrow(self):
        columns = [cols[0] for cols in self.description]
        data = []
        for row in self.result_rows:
            data.append({k: v for (k, v) in zip(columns, row)})
        df = pd.DataFrame(data)
        return pa.Table.from_pandas(df)

    def fetchmany_arrow(self, size):
        mock_dataframe = self.fetchall_arrow().to_pandas()
        if self.returned_count >= len(mock_dataframe):
            mock_dataframe = pd.DataFrame(columns=mock_dataframe.columns)
        else:
            mock_dataframe = mock_dataframe.iloc[self.returned_count : self.returned_count + size]
            self.returned_count += size
        return pa.Table.from_pandas(mock_dataframe)

    def execute(self, *args, **kwargs):
        query = args[0]
        self.description = [["a", "INT"], ["b", "INT"], ["c", "INT"]]
        self.result_rows = [
            [1, 2, 3],
            [100, 200, 300],
        ]
        self.returned_count = 0
        return self

    def close(self):
        pass


@pytest.fixture
def databricks_connection():
    """
    Mock databricks connector in featurebyte.session.databricks module
    """
    with patch("featurebyte.session.databricks.databricks_sql") as mock_connector:
        mock_connector.connect.return_value = MockDatabricksConnection()
        yield mock_connector


@pytest.fixture(name="patched_databricks_session_cls")
def patched_databricks_session_cls_fixture(
    databricks_session_dict,
):
    """Fixture for a patched session class"""
    with patch("featurebyte.session.databricks.DatabricksSession", autospec=True) as patched_class:
        mock_session_obj = patched_class.return_value
        mock_session_obj.database_name = databricks_session_dict["catalog_name"]
        mock_session_obj.schema_name = databricks_session_dict["schema_name"]
        yield patched_class


@pytest.mark.usefixtures("databricks_connection")
@patch("featurebyte.session.databricks.WorkspaceClient")
@pytest.mark.asyncio
async def test_databricks_session(mock_workspace_client, databricks_session_dict):
    """
    Test DatabricksSession
    """
    mock_workspace_client = mock_workspace_client.return_value
    mock_workspace_client.catalogs.list.return_value = [
        CatalogInfo(name="hive_metastore"),
        CatalogInfo(name="samples"),
    ]
    mock_workspace_client.schemas.list.return_value = [
        SchemaInfo(name="default"),
        SchemaInfo(name="demo"),
    ]
    mock_workspace_client.tables.list.return_value = [
        TableInfo(name="transactions"),
        TableInfo(name="calls"),
    ]
    mock_workspace_client.tables.get.return_value = TableInfo(
        name="transactions",
        columns=[
            ColumnInfo(name="col_binary", type_text="BINARY", comment="Binary Column"),
            ColumnInfo(name="col_bool", type_text="BOOLEAN", comment="Boolean Column"),
            ColumnInfo(name="col_date", type_text="DATE", comment="Date Column", partition_index=0),
            ColumnInfo(name="col_decimal", type_text="DECIMAL", comment="Decimal Column"),
            ColumnInfo(name="col_double", type_text="DOUBLE", comment="Double Column"),
            ColumnInfo(name="col_float", type_text="FLOAT", comment="Float Column"),
            ColumnInfo(name="col_int", type_text="INT", comment="Int Column", partition_index=1),
            ColumnInfo(name="col_interval", type_text="INTERVAL", comment="Interval Column"),
            ColumnInfo(name="col_void", type_text="VOID", comment="Void Column"),
            ColumnInfo(name="col_timestamp", type_text="TIMESTAMP", comment="Timestamp Column"),
            ColumnInfo(name="col_array", type_text="ARRAY", comment="Array Column"),
            ColumnInfo(name="col_map", type_text="MAP", comment="Map Column"),
            ColumnInfo(name="col_struct", type_text="STRUCT<a:INT>", comment="Struct Column"),
            ColumnInfo(name="col_string", type_text="STRING", comment="String Column"),
            ColumnInfo(name="col_unknown", type_text="UNKNOWN", comment="Unknown Column"),
        ],
    )

    session = DatabricksSession(**databricks_session_dict)
    assert session.host == "some-databricks-hostname"
    assert session.http_path == "some-databricks-http-endpoint"
    assert session.database_credential.access_token == "some-databricks-access-token"
    assert await session.list_databases() == ["hive_metastore", "samples"]
    assert await session.list_schemas(database_name="hive_metastore") == ["default", "demo"]
    tables = await session.list_tables(database_name="hive_metastore", schema_name="default")
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
            name="col_date",
            dtype=DBVarType.DATE,
            description="Date Column",
            partition_metadata=PartitionMetadata(is_partition_key_candidate=True),
        ),
        "col_double": ColumnSpecWithDescription(
            name="col_double", dtype=DBVarType.FLOAT, description="Double Column"
        ),
        "col_float": ColumnSpecWithDescription(
            name="col_float", dtype=DBVarType.FLOAT, description="Float Column"
        ),
        "col_int": ColumnSpecWithDescription(
            name="col_int",
            dtype=DBVarType.INT,
            description="Int Column",
            partition_metadata=PartitionMetadata(is_partition_key_candidate=True),
        ),
        "col_interval": ColumnSpecWithDescription(
            name="col_interval", dtype=DBVarType.TIMEDELTA, description="Interval Column"
        ),
        "col_void": ColumnSpecWithDescription(
            name="col_void", dtype=DBVarType.VOID, description="Void Column"
        ),
        "col_timestamp": ColumnSpecWithDescription(
            name="col_timestamp", dtype=DBVarType.TIMESTAMP, description="Timestamp Column"
        ),
        "col_array": ColumnSpecWithDescription(
            name="col_array", dtype=DBVarType.ARRAY, description="Array Column"
        ),
        "col_map": ColumnSpecWithDescription(
            name="col_map", dtype=DBVarType.DICT, description="Map Column"
        ),
        "col_decimal": ColumnSpecWithDescription(
            name="col_decimal", dtype=DBVarType.FLOAT, description="Decimal Column"
        ),
        "col_struct_a": ColumnSpecWithDescription(
            name="col_struct_a",
            dtype=DBVarType.INT,
            description="Struct Column",
            nested_field_metadata=NestedFieldMetadata(parent_column_name="col_struct", keys=["a"]),
        ),
        "col_string": ColumnSpecWithDescription(
            name="col_string", dtype=DBVarType.VARCHAR, description="String Column"
        ),
        "col_unknown": ColumnSpecWithDescription(
            name="col_unknown", dtype=DBVarType.UNKNOWN, description="Unknown Column"
        ),
    }
    df_result = await session.execute_query("SELECT * FROM table")
    df_expected = pd.DataFrame({"a": [1, 100], "b": [2, 200], "c": [3, 300]}, dtype="int32")
    pd.testing.assert_frame_equal(df_result, df_expected)


@pytest.mark.asyncio
async def test_databricks_register_table(databricks_session_dict, databricks_connection):
    """
    Test Databricks session register_table
    """
    with patch(
        "featurebyte.session.databricks.DatabricksSession.execute_query"
    ) as mock_execute_query:
        with (
            patch("featurebyte.session.databricks.WorkspaceClient"),
            patch("featurebyte.session.databricks.DbfsExt"),
            patch("featurebyte.session.databricks.pd.DataFrame.to_parquet", autospec=True),
        ):
            session = DatabricksSession(**databricks_session_dict)
            df = pd.DataFrame(
                {
                    "point_in_time": pd.to_datetime(["2022-01-01", "2022-01-02", "2022-01-03"]),
                    "cust_id": [1, 2, 3],
                },
            )
            expected = "CREATE OR REPLACE TABLE"
            await session.register_table("my_view", df)
            assert mock_execute_query.call_args_list[-1][0][0].startswith(expected)


def test_databricks_sql_connector_not_available(databricks_session_dict):
    """
    Simulate missing databricks-sql-connector dependency
    """
    with patch("featurebyte.session.databricks.HAS_DATABRICKS_SQL_CONNECTOR", False):
        with pytest.raises(RuntimeError) as exc:
            _ = DatabricksSession(**databricks_session_dict)
        assert str(exc.value) == "databricks-sql-connector is not available"


def test_databricks_schema_initializer__sql_objects(patched_databricks_session_cls):
    session = patched_databricks_session_cls()
    sql_objects = BaseSparkSchemaInitializer(session).get_sql_objects()
    for item in sql_objects:
        item["filename"] = os.path.basename(item["filename"])
        item["type"] = item["type"].value
    expected = []

    def _sorted_result(lst):
        return sorted(lst, key=lambda x: x["filename"])

    assert _sorted_result(sql_objects) == _sorted_result(expected)
