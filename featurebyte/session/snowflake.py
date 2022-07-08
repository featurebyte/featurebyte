"""
SnowflakeSession class
"""
from __future__ import annotations

from typing import Any

import json
import os
from enum import Enum

import numpy as np
import pandas as pd
from pydantic import Field
from snowflake import connector
from snowflake.connector.pandas_tools import write_pandas

import featurebyte
from featurebyte.enum import DBVarType, SourceType
from featurebyte.logger import logger
from featurebyte.session.base import BaseSession
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

        self._connection = connector.connect(
            user=data["username"],
            password=data["password"],
            account=data["account"],
            warehouse=data["warehouse"],
            database=data["database"],
            schema=data["sf_schema"],
        )

        # If the featurebyte schema does not exist, the self._connection can still be created
        # without errors. Below checks whether the schema actually exists. If not, it will be
        # created and initialized with custom functions and procedures.
        SchemaInitializer(self).initialize()

    def list_databases(self) -> list[str]:
        """
        Execute SQL query to retrieve database names

        Returns
        -------
        list[str]
        """
        databases = self.execute_query("SHOW DATABASES")
        output = []
        if databases is not None:
            output.extend(databases["name"])
        return output

    def list_schemas(self, database_name: str | None = None) -> list[str]:
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
        database_name = database_name or self.database
        schemas = self.execute_query(f'SHOW SCHEMAS IN DATABASE "{database_name}"')
        output = []
        if schemas is not None:
            output.extend(schemas["name"])
        return output

    def list_tables(
        self, database_name: str | None = None, schema_name: str | None = None
    ) -> list[str]:
        database_name = database_name or self.database
        schema_name = schema_name or self.sf_schema
        tables = self.execute_query(f'SHOW TABLES IN SCHEMA "{database_name}"."{schema_name}"')
        views = self.execute_query(f'SHOW VIEWS IN SCHEMA "{database_name}"."{schema_name}"')
        output = []
        if tables is not None:
            output.extend(tables["name"])
        if views is not None:
            output.extend(views["name"])
        return output

    def register_temp_table(self, table_name: str, dataframe: pd.DataFrame) -> None:
        schema = self.get_columns_schema_from_dataframe(dataframe)
        self.execute_query(
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

    def list_table_schema(
        self,
        table_name: str | None,
        database_name: str | None = None,
        schema_name: str | None = None,
    ) -> dict[str, DBVarType]:
        database_name = database_name or self.database
        schema_name = schema_name or self.sf_schema
        schema = self.execute_query(
            f'SHOW COLUMNS IN "{database_name}"."{schema_name}"."{table_name}"'
        )
        column_name_type_map = {}
        if schema is not None:
            column_name_type_map = {
                column_name: self._convert_to_internal_variable_type(json.loads(var_info))
                for _, (column_name, var_info) in schema[["column_name", "data_type"]].iterrows()
            }
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


class SqlObjectType(str, Enum):
    """Enum for type of SQL objects to initialize in Snowflake"""

    FUNCTION = "function"
    PROCEDURE = "procedure"
    TABLE = "table"


class SchemaInitializer:
    """Responsible for initializing featurebyte schema

    Parameters
    ----------
    session : SnowflakeSession
        Snowflake session object
    """

    def __init__(self, session: SnowflakeSession):
        self.session = session

    def schema_exists(self) -> bool:
        """Check whether the featurebyte schema exists

        Returns
        -------
        bool
        """
        show_schemas_result = self.session.execute_query("SHOW SCHEMAS")
        if show_schemas_result is not None:
            available_schemas = show_schemas_result["name"].tolist()
        else:
            available_schemas = []
        return self.session.sf_schema in available_schemas

    def initialize(self) -> None:
        """Initialize the featurebyte schema if it doesn't exist"""

        if not self.schema_exists():
            logger.debug(f"Initializing schema {self.session.sf_schema}")
            create_schema_query = f"CREATE SCHEMA {self.session.sf_schema}"
            self.session.execute_query(create_schema_query)

        self.register_missing_objects()

    def register_missing_objects(self) -> None:
        """Detect database objects that are missing and register them"""

        sql_objects = self.get_sql_objects()
        sql_objects_by_type: dict[SqlObjectType, list[dict[str, Any]]] = {
            SqlObjectType.FUNCTION: [],
            SqlObjectType.PROCEDURE: [],
            SqlObjectType.TABLE: [],
        }
        for sql_object in sql_objects:
            sql_objects_by_type[sql_object["type"]].append(sql_object)

        self.register_missing_functions(sql_objects_by_type[SqlObjectType.FUNCTION])
        self.register_missing_procedures(sql_objects_by_type[SqlObjectType.PROCEDURE])
        self.create_missing_tables(sql_objects_by_type[SqlObjectType.TABLE])

    def register_missing_functions(self, functions: list[dict[str, Any]]) -> None:
        """Register functions defined in the snowflake sql directory

        Parameters
        ----------
        functions : list[dict[str, Any]]
            List of functions to register
        """
        df_result = self.session.execute_query(
            f"SHOW USER FUNCTIONS IN DATABASE {self.session.database}"
        )
        if df_result is None:
            return
        df_result = df_result[df_result["schema_name"] == self.session.sf_schema]
        existing = set(df_result["name"].tolist())
        for item in functions:
            if item["identifier"] not in existing:
                self._register_sql_object(item)

    def register_missing_procedures(self, procedures: list[dict[str, Any]]) -> None:
        """Register procedures defined in the snowflake sql directory

        Parameters
        ----------
        procedures: list[dict[str, Any]]
            List of procedures to register
        """
        df_result = self.session.execute_query(
            f"SHOW PROCEDURES IN DATABASE {self.session.database}"
        )
        if df_result is None:
            return
        df_result = df_result[df_result["schema_name"] == self.session.sf_schema]
        existing = set(df_result["name"].tolist())
        for item in procedures:
            if item["identifier"] not in existing:
                self._register_sql_object(item)

    def create_missing_tables(self, tables: list[dict[str, Any]]) -> None:
        """Create tables defined in snowflake sql directory

        Parameters
        ----------
        tables: list[dict[str, Any]]
            List of tables to register
        """
        df_result = self.session.execute_query(
            f'SHOW TABLES IN SCHEMA "{self.session.database}"."{self.session.sf_schema}"'
        )
        if df_result is None:
            return
        existing = set(df_result["name"].tolist())
        for item in tables:
            if item["identifier"] not in existing:
                self._register_sql_object(item)

    def _register_sql_object(self, item: dict[str, Any]) -> None:
        logger.debug(f'Registering {item["identifier"]}')
        with open(item["filename"], encoding="utf-8") as f_handle:
            query = f_handle.read()
        self.session.execute_query(query)

    @staticmethod
    def get_sql_objects() -> list[dict[str, Any]]:
        """Find all the objects defined in the sql directory

        Returns
        -------
        list[str]
        """
        sql_directory = os.path.join(os.path.dirname(featurebyte.__file__), "sql", "snowflake")
        output = []

        for filename in os.listdir(sql_directory):

            sql_object_type = None
            if filename.startswith("F_"):
                sql_object_type = SqlObjectType.FUNCTION
            elif filename.startswith("SP_"):
                sql_object_type = SqlObjectType.PROCEDURE
            elif filename.startswith("T_"):
                sql_object_type = SqlObjectType.TABLE

            identifier = filename.replace(".sql", "")
            if sql_object_type == SqlObjectType.TABLE:
                # Table naming convention does not include "T_" prefix
                identifier = identifier[len("T_") :]

            full_filename = os.path.join(sql_directory, filename)

            sql_object = {
                "type": sql_object_type,
                "filename": full_filename,
                "identifier": identifier,
            }
            output.append(sql_object)

        return output
