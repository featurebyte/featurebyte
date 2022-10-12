"""
SnowflakeSession class
"""
from __future__ import annotations

from typing import Any, OrderedDict

import asyncio
import collections
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import Field
from snowflake import connector
from snowflake.connector.constants import QueryStatus
from snowflake.connector.cursor import ASYNC_NO_DATA_MAX_RETRY, ASYNC_RETRY_PATTERN
from snowflake.connector.errors import DatabaseError, NotSupportedError, OperationalError
from snowflake.connector.pandas_tools import write_pandas

from featurebyte.enum import DBVarType, SourceType
from featurebyte.exception import CredentialsError
from featurebyte.session.base import BaseSchemaInitializer, BaseSession
from featurebyte.session.enum import SnowflakeDataType


class SnowflakeSession(BaseSession):
    """
    Snowflake session class
    """

    account: str
    warehouse: str
    database: str
    sf_schema: str
    username: str
    password: str
    source_type: SourceType = Field(SourceType.SNOWFLAKE, const=True)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

        try:
            self._connection = connector.connect(
                user=data["username"],
                password=data["password"],
                account=data["account"],
                warehouse=data["warehouse"],
                database=data["database"],
                schema=data["sf_schema"],
            )
        except (OperationalError, DatabaseError) as exc:
            raise CredentialsError("Invalid credentials provided.") from exc

    async def initialize(self) -> None:
        # If the featurebyte schema does not exist, the self._connection can still be created
        # without errors. Below checks whether the schema actually exists. If not, it will be
        # created and initialized with custom functions and procedures.
        await SnowflakeSchemaInitializer(self).initialize()

    @property
    def schema_name(self) -> str:
        return self.sf_schema

    @property
    def database_name(self) -> str:
        return self.database

    def make_async_query_request(self, query: str) -> Any:
        """
        Execute async query

        Parameters
        ----------
        query: str
            sql query to execute

        Returns
        -------
        Any
            Query ID
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute_async(query)
            query_id = cursor.sfqid
            return query_id
        finally:
            cursor.close()

    async def fetch_async_query(
        self, query_id: Any, timeout: int, output_path: Path | None
    ) -> pd.DataFrame | None:
        """
        Execute SQL queries

        Parameters
        ----------
        query_id: Any
            Query ID used to fetch results
        timeout: int
            timeout in seconds
        output_path: Path | None
            path to store results

        Returns
        -------
        pd.DataFrame | None
            Query result as a pandas DataFrame if the query expects result

        Raises
        ------
        DatabaseError
            Invalid query id
        """
        # pylint: disable=protected-access
        no_data_counter = 0
        retry_pattern_pos = 0
        start_time = time.time()
        status = QueryStatus.RUNNING
        while time.time() - start_time < timeout:
            status = self.connection.get_query_status_throw_if_error(query_id)
            if not self.connection.is_still_running(status):
                break
            if status == QueryStatus.NO_DATA:  # pragma: no cover
                no_data_counter += 1
                if no_data_counter > ASYNC_NO_DATA_MAX_RETRY:
                    raise DatabaseError(
                        "Cannot retrieve data on the status of this query. No information returned "
                        f"from server for query '{query_id}'"
                    )
            await asyncio.sleep(0.5 * ASYNC_RETRY_PATTERN[retry_pattern_pos])  # Same wait as JDBC
            # If we can advance in ASYNC_RETRY_PATTERN then do so
            if retry_pattern_pos < (len(ASYNC_RETRY_PATTERN) - 1):
                retry_pattern_pos += 1
        if status != QueryStatus.SUCCESS:
            raise DatabaseError(
                f"Status of query '{query_id}' is {status.name}, results are unavailable"
            )

        get_result_sql = f"select * from table(result_scan('{query_id}'))"

        # return result as dataframe
        if not output_path:
            return await self.execute_query(get_result_sql)

        # save result to parquet file
        cursor = self.connection.cursor()
        try:
            cursor.execute(get_result_sql)
            index = 0
            for table in cursor.fetch_arrow_batches():
                end_index = index + len(table)
                # add index column so that order can be recovered later
                table = table.append_column(
                    "__index__", pa.array(range(index, end_index), pa.int64())
                )
                pq.write_to_dataset(table, root_path=output_path)
                index = end_index
            return None
        finally:
            cursor.close()

    async def execute_async_query(
        self, query: str, timeout: int = 180, output_path: Path | None = None
    ) -> pd.DataFrame | None:
        """
        Execute SQL queries

        Parameters
        ----------
        query: str
            sql query to execute
        timeout: int
            timeout in seconds
        output_path: Path | None
            path to store results

        Returns
        -------
        pd.DataFrame | None
            Query result as a pandas DataFrame if the query expects result
        """

        query_id = self.make_async_query_request(query)
        return await self.fetch_async_query(
            query_id=query_id,
            timeout=timeout,
            output_path=output_path,
        )

    async def list_databases(self) -> list[str]:
        """
        Execute SQL query to retrieve database names

        Returns
        -------
        list[str]
        """
        databases = await self.execute_query("SHOW DATABASES")
        output = []
        if databases is not None:
            output.extend(databases["name"])
        return output

    async def list_schemas(self, database_name: str | None = None) -> list[str]:
        """
        Execute SQL query to retrieve schema names

        Parameters
        ----------
        database_name: str | None
            Database name

        Returns
        -------
        list[str]
        """
        schemas = await self.execute_query(f'SHOW SCHEMAS IN DATABASE "{database_name}"')
        output = []
        if schemas is not None:
            output.extend(schemas["name"])
        return output

    async def list_tables(
        self, database_name: str | None = None, schema_name: str | None = None
    ) -> list[str]:
        tables = await self.execute_query(
            f'SHOW TABLES IN SCHEMA "{database_name}"."{schema_name}"'
        )
        views = await self.execute_query(f'SHOW VIEWS IN SCHEMA "{database_name}"."{schema_name}"')
        output = []
        if tables is not None:
            output.extend(tables["name"])
        if views is not None:
            output.extend(views["name"])
        return output

    def fetch_query_result_impl(self, cursor: Any) -> pd.DataFrame | None:
        """
        Fetch the result of executed SQL query from connection cursor

        This is an implementation specific to Snowflake that is more efficient when applicable.

        Parameters
        ----------
        cursor : Any
            The connection cursor

        Returns
        -------
        pd.DataFrame | None
            Query result as a pandas DataFrame if the query expects result
        """
        if cursor.description:
            try:
                result = cursor.fetch_pandas_all()
                return result
            except NotSupportedError:
                # fetch_pandas_all() raises NotSupportedError when: 1) The executed query does not
                # support it. Currently, only SELECT statements are supported; 2) pyarrow is not
                # available as a dependency.
                return super().fetch_query_result_impl(cursor)
        return None

    async def register_temp_table(self, table_name: str, dataframe: pd.DataFrame) -> None:
        schema = self.get_columns_schema_from_dataframe(dataframe)
        await self.execute_query(
            f"""
            CREATE OR REPLACE TEMP TABLE {table_name}(
                {schema}
            )
            """
        )
        dataframe = self._prep_dataframe_before_write_pandas(dataframe)
        write_pandas(self._connection, dataframe, table_name)

    @staticmethod
    def _convert_to_internal_variable_type(snowflake_var_info: dict[str, Any]) -> DBVarType:
        to_internal_variable_map = {
            SnowflakeDataType.FIXED: DBVarType.INT,
            SnowflakeDataType.REAL: DBVarType.FLOAT,
            SnowflakeDataType.BINARY: DBVarType.BINARY,
            SnowflakeDataType.BOOLEAN: DBVarType.BOOL,
            SnowflakeDataType.DATE: DBVarType.DATE,
            SnowflakeDataType.TIME: DBVarType.TIME,
        }
        if snowflake_var_info["type"] in to_internal_variable_map:
            return to_internal_variable_map[snowflake_var_info["type"]]
        if snowflake_var_info["type"] == SnowflakeDataType.TEXT:
            return DBVarType.CHAR if snowflake_var_info["length"] == 1 else DBVarType.VARCHAR
        if snowflake_var_info["type"] in {
            SnowflakeDataType.TIMESTAMP_LTZ,
            SnowflakeDataType.TIMESTAMP_NTZ,
            SnowflakeDataType.TIMESTAMP_TZ,
        }:
            return DBVarType.TIMESTAMP
        raise ValueError(f"Not supported data type '{snowflake_var_info}'")

    async def list_table_schema(
        self,
        table_name: str | None,
        database_name: str | None = None,
        schema_name: str | None = None,
    ) -> OrderedDict[str, DBVarType]:
        schema = await self.execute_query(
            f'SHOW COLUMNS IN "{database_name}"."{schema_name}"."{table_name}"'
        )
        column_name_type_map = collections.OrderedDict()
        if schema is not None:
            for _, (column_name, var_info) in schema[["column_name", "data_type"]].iterrows():
                column_name_type_map[column_name] = self._convert_to_internal_variable_type(
                    json.loads(var_info)
                )
        return column_name_type_map

    @staticmethod
    def get_columns_schema_from_dataframe(dataframe: pd.DataFrame) -> str:
        """Get schema that can be used in CREATE TABLE statement from pandas DataFrame

        Parameters
        ----------
        dataframe : pd.DataFrame
            Input DataFrame

        Returns
        -------
        str
        """
        schema = []
        for colname, dtype in dataframe.dtypes.to_dict().items():
            if pd.api.types.is_datetime64_any_dtype(dataframe[colname]):
                db_type = "DATETIME"
            elif pd.api.types.is_float_dtype(dtype):
                db_type = "DOUBLE"
            elif pd.api.types.is_integer_dtype(dtype):
                db_type = "INT"
            else:
                db_type = "VARCHAR"
            schema.append(f'"{colname}" {db_type}')
        schema_str = ", ".join(schema)
        return schema_str

    @staticmethod
    def _prep_dataframe_before_write_pandas(dataframe: pd.DataFrame) -> pd.DataFrame:
        # Ideally we should avoid making a copy, but so far the only way to get write_pandas() to
        # create DATETIME type columns in Snowflake for datetime columns in DataFrame is to specify
        # DATETIME type in the schema when creating table, and convert the dtype in DataFrame to
        # object before calling write_pandas(). A copy is made to prevent unintended side effects.
        dataframe = dataframe.copy()
        for date_col in dataframe.select_dtypes(include=[np.datetime64]):
            dataframe[date_col] = dataframe[date_col].astype(str)
        return dataframe


class SnowflakeSchemaInitializer(BaseSchemaInitializer):
    """Snowflake schema initializer class"""

    @property
    def sql_directory_name(self) -> str:
        return "snowflake"

    async def create_schema(self) -> None:
        create_schema_query = f"CREATE SCHEMA {self.session.schema_name}"
        await self.session.execute_query(create_schema_query)

    async def list_functions(self) -> list[str]:
        df_result = await self.session.execute_query(
            f"SHOW USER FUNCTIONS IN DATABASE {self.session.database_name}"
        )
        out = []
        if df_result is not None:
            df_result = df_result[df_result["schema_name"] == self.session.schema_name]
            out.extend(df_result["name"])
        return out

    async def list_procedures(self) -> list[str]:
        df_result = await self.session.execute_query(
            f"SHOW PROCEDURES IN DATABASE {self.session.database_name}"
        )
        out = []
        if df_result is not None:
            df_result = df_result[df_result["schema_name"] == self.session.schema_name]
            out.extend(df_result["name"])
        return out
