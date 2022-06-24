"""
SnowflakeSession class
"""
from __future__ import annotations

from typing import Any, Optional

import json
import os

from pydantic import Field
from snowflake import connector

from featurebyte.enum import DBVarType, SourceType
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
    username: Optional[str] = Field(default=None)
    password: Optional[str] = Field(default=None)
    source_type: SourceType = Field(SourceType.SNOWFLAKE, const=True)

    def __init__(self, **data: Any) -> None:
        if data.get("username") is None:
            data["username"] = os.getenv("SNOWFLAKE_USER")
        if data.get("password") is None:
            data["password"] = os.getenv("SNOWFLAKE_PASSWORD")
        if not data.get("username") or not data.get("password"):
            raise ValueError("Username or password is empty!")
        super().__init__(**data)

        self._connection = connector.connect(
            user=data["username"],
            password=data["password"],
            account=data["account"],
            warehouse=data["warehouse"],
            database=data["database"],
            schema=data["sf_schema"],
        )

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
