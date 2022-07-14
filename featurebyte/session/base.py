"""
Session class
"""
from __future__ import annotations

from typing import Any

from abc import abstractmethod

import pandas as pd
from pydantic import BaseModel, PrivateAttr

from featurebyte.enum import DBVarType, SourceType


class BaseSession(BaseModel):
    """
    Abstract session class to extract data warehouse table metadata & execute query
    """

    source_type: SourceType
    _connection: Any = PrivateAttr(default=None)

    @property
    def connection(self) -> Any:
        """
        Session connection object

        Returns
        -------
        Any
        """
        return self._connection

    @abstractmethod
    def list_databases(self) -> list[str]:
        """
        Execute SQL query to retrieve database names

        Returns
        -------
        list[str]
        """

    @abstractmethod
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

    @abstractmethod
    def list_tables(
        self, database_name: str | None = None, schema_name: str | None = None
    ) -> list[str]:
        """
        Execute SQL query to retrieve table names

        Parameters
        ----------
        database_name: str | None
            Database name
        schema_name: str | None
            Schema name

        Returns
        -------
        list[str]
        """

    @abstractmethod
    def list_table_schema(
        self,
        table_name: str | None,
        database_name: str | None = None,
        schema_name: str | None = None,
    ) -> dict[str, DBVarType]:
        """
        Execute SQL query to retrieve table schema of a given table name and convert the
        schema type to internal variable type

        Parameters
        ----------
        database_name: str | None
            Database name
        schema_name: str | None
            Schema name
        table_name: str
            Table name

        Returns
        -------
        dict[str, DBVarType]
        """

    def execute_query(self, query: str) -> pd.DataFrame | None:
        """
        Execute SQL query

        Parameters
        ----------
        query: str
            sql query to execute

        Returns
        -------
        pd.DataFrame
            return pandas DataFrame if the query expect output
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            import time

            tic = time.time()
            result = self.fetch_query_result_impl(cursor)
            elapsed = time.time() - tic
            print(f"Fetching query result took {elapsed:.2f}s")
            return result
        finally:
            cursor.close()

    def fetch_query_result_impl(self, cursor: Any) -> pd.DataFrame | None:
        if cursor.description:
            all_rows = cursor.fetchall()
            columns = [row[0] for row in cursor.description]
            return pd.DataFrame(all_rows, columns=columns)
        return None

    @abstractmethod
    def register_temp_table(self, table_name: str, dataframe: pd.DataFrame) -> None:
        """
        Register a temporary table

        Parameters
        ----------
        table_name : str
            Temp table name
        dataframe : pd.DataFrame
            DataFrame to register
        """
