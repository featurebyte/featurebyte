"""
SnowflakeSession class
"""
from __future__ import annotations

from typing import Any

import json
import os

from pydantic import Field
from snowflake import connector

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
        self._init_featurebyte_schema_if_needed()

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

    def _init_featurebyte_schema_if_needed(self) -> None:
        # Note: self._connection.schema changes to None after execute_query() if the specified
        # default schema doesn't exist, so we should not rely on it
        SchemaInitializer(self).initialize()


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
        if self.schema_exists():
            return
        logger.debug(f"Initializing schema {self.session.sf_schema}")
        create_schema_query = f"CREATE SCHEMA {self.session.sf_schema}"
        self.session.execute_query(create_schema_query)
        self.register_custom_functions()

    def register_custom_functions(self) -> None:
        """Register functions and procedures defined in the snowflake sql directory"""
        for sql_filename in self.get_custom_function_sql_filenames():
            with open(sql_filename, encoding="utf-8") as file_handle:
                self.session.execute_query(file_handle.read())

    @staticmethod
    def get_custom_function_sql_filenames() -> list[str]:
        """Find the sourcefiles for all user defined functions and stored procedure

        Returns
        -------
        list[str]
        """
        sql_directory = os.path.join(
            os.path.dirname(featurebyte.__file__), "..", "sql", "snowflake"
        )
        output = []
        for filename in os.listdir(sql_directory):
            if filename.startswith("F_") or filename.startswith("SP_"):
                full_filename = os.path.join(sql_directory, filename)
                output.append(full_filename)
        return output
