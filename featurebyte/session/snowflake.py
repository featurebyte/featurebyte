"""
SnowflakeSession class
"""
from __future__ import annotations

from typing import Any

import json
import os
from enum import Enum

from pydantic import Field
from snowflake import connector

import featurebyte
from featurebyte.config import Configurations
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
            schema=self._get_featurebyte_schema_name(),
        )

        # If the featurebyte schema does not exist, the self._connection can still be created
        # without errors. Below checks whether the schema actually exists. If not, it will be
        # created and initialized with custom functions and procedures.
        self._init_featurebyte_schema_if_needed()

    def list_tables(self) -> list[str]:
        tables = self.execute_query(f'SHOW TABLES IN SCHEMA "{self.database}"."{self.sf_schema}"')
        views = self.execute_query(f'SHOW VIEWS IN SCHEMA "{self.database}"."{self.sf_schema}"')
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

    def list_table_schema(self, table_name: str) -> dict[str, DBVarType]:
        schema = self.execute_query(
            f'SHOW COLUMNS IN "{self.database}"."{self.sf_schema}"."{table_name}"'
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
        SchemaInitializer(self, self._get_featurebyte_schema_name()).initialize()

    def _get_featurebyte_schema_name(self) -> str:
        return Configurations().snowflake.featurebyte_schema


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
    featurebyte_schema_name : str
        Featurebyte schema name
    """

    def __init__(self, session: SnowflakeSession, featurebyte_schema_name: str):
        self.session = session
        self.featurebyte_schema_name = featurebyte_schema_name

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
        return self.featurebyte_schema_name in available_schemas

    def initialize(self) -> None:
        """Initialize the featurebyte schema if it doesn't exist"""
        if not self.schema_exists():
            logger.debug(f"Initializing schema {self.featurebyte_schema_name}")
            create_schema_query = f"CREATE SCHEMA {self.featurebyte_schema_name}"
            self.session.execute_query(create_schema_query)
            self.register_missing_objects()
        self.register_missing_objects()

    def register_missing_objects(self) -> None:
        """Detect database objects that are missing and register them"""
        sql_objects = self.get_sql_objects()
        sql_objects_by_type = {
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
        """Register functions defined in the snowflake sql directory"""
        df = self.session.execute_query(f"SHOW USER FUNCTIONS IN DATABASE {self.session.database}")
        df = df[df["schema_name"] == self.featurebyte_schema_name]
        existing = set(df["name"].tolist())
        for item in functions:
            if item["identifier"] not in existing:
                self._register_sql_object(item)

    def register_missing_procedures(self, procedures: list[dict[str, Any]]) -> None:
        """Register procedures defined in the snowflake sql directory"""
        df = self.session.execute_query(f"SHOW PROCEDURES IN DATABASE {self.session.database}")
        df = df[df["schema_name"] == self.featurebyte_schema_name]
        existing = set(df["name"].tolist())
        for item in procedures:
            if item["identifier"] not in existing:
                self._register_sql_object(item)

    def create_missing_tables(self, tables: list[dict[str, Any]]) -> None:
        """Create tables defined in snowflake sql directory"""
        df = self.session.execute_query(
            f'SHOW TABLES IN SCHEMA "{self.session.database}"."{self.featurebyte_schema_name}"'
        )
        existing = set(df["name"].tolist())
        for item in tables:
            if item["identifier"] not in existing:
                self._register_sql_object(item)

    def _register_sql_object(self, item: dict[str, Any]):
        logger.debug(f'Registering {item["identifier"]}')
        with open(item["filename"]) as f:
            query = f.read()
        self.session.execute_query(query)

    @staticmethod
    def get_sql_objects() -> list[dict[str, Any]]:
        """Find all the objects defined in the sql directory

        Returns
        -------
        list[str]
        """
        sql_directory = os.path.join(
            os.path.dirname(featurebyte.__file__), "..", "sql", "snowflake"
        )
        output = []
        for filename in os.listdir(sql_directory):
            full_filename = os.path.join(sql_directory, filename)
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
            sql_object = {
                "type": sql_object_type,
                "filename": full_filename,
                "identifier": identifier,
            }
            output.append(sql_object)
        return output
