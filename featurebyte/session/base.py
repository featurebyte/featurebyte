"""
Session class
"""
from __future__ import annotations

from typing import Any, AsyncGenerator, OrderedDict

import os
import shutil
import tempfile
from abc import abstractmethod
from pathlib import Path

import aiofiles
import pandas as pd
from pydantic import BaseModel, PrivateAttr

from featurebyte.enum import DBVarType, SourceType


class BaseSession(BaseModel):
    """
    Abstract session class to extract data warehouse table metadata & execute query
    """

    source_type: SourceType
    _connection: Any = PrivateAttr(default=None)

    async def initialize(self) -> None:
        """
        Initialize session
        """

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
    async def list_databases(self) -> list[str]:
        """
        Execute SQL query to retrieve database names

        Returns
        -------
        list[str]
        """

    @abstractmethod
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

    @abstractmethod
    async def list_tables(
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
    async def list_table_schema(
        self,
        table_name: str | None,
        database_name: str | None = None,
        schema_name: str | None = None,
    ) -> OrderedDict[str, DBVarType]:
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
        OrderedDict[str, DBVarType]
        """

    async def execute_query(self, query: str) -> pd.DataFrame | None:
        """
        Execute SQL query

        Parameters
        ----------
        query: str
            sql query to execute

        Returns
        -------
        pd.DataFrame | None
            Query result as a pandas DataFrame if the query expects result
        """
        return self.execute_query_blocking(query)

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
        _ = timeout
        result = await self.execute_query(query)
        if output_path is None:
            return result
        assert isinstance(result, pd.DataFrame)
        result.to_parquet(output_path)
        return None

    async def get_async_query_stream(
        self, query: str, timeout: int = 180, chunk_size: int = 255 * 1024
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream results from asynchronous query as parquet zip archive

        Parameters
        ----------
        query: str
            sql query to execute
        timeout: int
            timeout in seconds
        chunk_size: int
            Size of each chunk in the stream

        Yields
        ------
        bytes
            Byte chunk
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # execute query and save result to parquet file
            parquet_path = Path(os.path.join(tmpdir, "data.parquet"))
            await self.execute_async_query(query=query, timeout=timeout, output_path=parquet_path)

            # create zip archive from output parquet
            assert os.path.exists(parquet_path)
            shutil.make_archive(str(parquet_path), "zip", root_dir=tmpdir, base_dir="data.parquet")

            # stream zip file
            async with aiofiles.open(f"{parquet_path}.zip", "rb") as file_obj:
                while True:
                    chunk = await file_obj.read(chunk_size)
                    if len(chunk) == 0:
                        break
                    yield chunk

    def execute_query_blocking(self, query: str) -> pd.DataFrame | None:
        """
        Execute SQL query without await

        Parameters
        ----------
        query: str
            sql query to execute

        Returns
        -------
        pd.DataFrame | None
            Query result as a pandas DataFrame if the query expects result
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            result = self.fetch_query_result_impl(cursor)
            return result
        finally:
            cursor.close()

    def fetch_query_result_impl(self, cursor: Any) -> pd.DataFrame | None:
        """
        Fetch the result of executed SQL query from connection cursor

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
            all_rows = cursor.fetchall()
            columns = [row[0] for row in cursor.description]
            return pd.DataFrame(all_rows, columns=columns)
        return None

    @abstractmethod
    async def register_temp_table(self, table_name: str, dataframe: pd.DataFrame) -> None:
        """
        Register a temporary table

        Parameters
        ----------
        table_name : str
            Temp table name
        dataframe : pd.DataFrame
            DataFrame to register
        """
