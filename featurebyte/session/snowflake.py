"""
SnowflakeSession class
"""
from __future__ import annotations

from typing import Any, OrderedDict

import collections
import json

import numpy as np
import pandas as pd
from pydantic import Field
from snowflake import connector
from snowflake.connector.errors import DatabaseError, NotSupportedError, OperationalError
from snowflake.connector.pandas_tools import write_pandas

from featurebyte.common.utils import create_new_arrow_stream_writer, pa_table_to_record_batches
from featurebyte.enum import DBVarType, SourceType
from featurebyte.exception import CredentialsError
from featurebyte.session.base import BaseSchemaInitializer, BaseSession
from featurebyte.session.enum import SnowflakeDataType


class SnowflakeSession(BaseSession):
    """
    Snowflake session class


    References
    ----------
    Precision & scale:
    https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#impact-of-precision-and-scale-on-storage-size
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

    def fetch_query_stream_impl(self, cursor: Any, output_pipe: Any) -> None:
        """
        Stream results from cursor in batches

        Parameters
        ----------
        cursor : Any
            The connection cursor
        output_pipe: Any
            Output pipe buffer
        """
        # fetch results in batches and write to the stream
        try:
            writer = None
            for table in cursor.fetch_arrow_batches():
                if not writer:
                    writer = create_new_arrow_stream_writer(output_pipe, table.schema)
                for batch in pa_table_to_record_batches(table):
                    writer.write_batch(batch)
            if not writer:
                # Arrow batch is empty, need to return empty table with schema
                table = cursor.get_result_batches()[0].to_arrow()
                batch = pa_table_to_record_batches(table)[0]
                writer = create_new_arrow_stream_writer(output_pipe, batch.schema)
                writer.write_batch(batch)
            writer.close()
        except NotSupportedError:
            super().fetch_query_stream_impl(cursor, output_pipe)

    async def register_table(
        self, table_name: str, dataframe: pd.DataFrame, temporary: bool = True
    ) -> None:
        schema = self.get_columns_schema_from_dataframe(dataframe)
        if temporary:
            create_command = "CREATE OR REPLACE TEMP TABLE"
        else:
            create_command = "CREATE OR REPLACE TABLE"
        await self.execute_query(
            f"""
            {create_command} {table_name}(
                {schema}
            )
            """
        )
        dataframe = self._prep_dataframe_before_write_pandas(dataframe)
        write_pandas(self._connection, dataframe, table_name)

    @staticmethod
    def _convert_to_internal_variable_type(snowflake_var_info: dict[str, Any]) -> DBVarType:
        to_internal_variable_map = {
            SnowflakeDataType.REAL: DBVarType.FLOAT,
            SnowflakeDataType.BINARY: DBVarType.BINARY,
            SnowflakeDataType.BOOLEAN: DBVarType.BOOL,
            SnowflakeDataType.DATE: DBVarType.DATE,
            SnowflakeDataType.TIME: DBVarType.TIME,
        }
        if snowflake_var_info["type"] in to_internal_variable_map:
            return to_internal_variable_map[snowflake_var_info["type"]]
        if snowflake_var_info["type"] == SnowflakeDataType.FIXED:
            # scale is defined as number of digits following the decimal point (see link in reference)
            return DBVarType.INT if snowflake_var_info["scale"] == 0 else DBVarType.FLOAT
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

    @property
    def current_working_schema_version(self) -> int:
        return 9

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
