"""
Session class
"""
from __future__ import annotations

from typing import Any, AsyncGenerator, OrderedDict

import os
import shutil
import tempfile
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path

import aiofiles
import pandas as pd
from pydantic import BaseModel, PrivateAttr

from featurebyte.common.path_util import get_package_root
from featurebyte.enum import DBVarType, SourceType
from featurebyte.logger import logger


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

    @property
    @abstractmethod
    def schema_name(self) -> str:
        """
        Returns the name of the working schema that stores featurebyte assets

        Returns
        -------
        str
        """

    @property
    @abstractmethod
    def database_name(self) -> str:
        """
        Returns the name of the working database that stores featurebyte assets

        Returns
        -------
        str
        """


class SqlObjectType(str, Enum):
    """Enum for type of SQL objects to initialize in Snowflake"""

    FUNCTION = "function"
    PROCEDURE = "procedure"
    TABLE = "table"


class BaseSchemaInitializer(ABC):
    """Responsible for initializing featurebyte schema

    Parameters
    ----------
    session : BaseSession
        Session object
    """

    def __init__(self, session: BaseSession):
        self.session = session

    @abstractmethod
    async def create_schema(self) -> None:
        """Create the featurebyte working schema"""

    @abstractmethod
    async def list_functions(self) -> list[str]:
        """Retrieve list of functions in the working schema"""

    @abstractmethod
    async def list_procedures(self) -> list[str]:
        """Retrieve list of procedures in the working schema"""

    @property
    @abstractmethod
    def sql_directory_name(self) -> str:
        """Directory name containing the SQL initialization scripts"""

    async def initialize(self) -> None:
        """Entry point to set up the featurebyte working schema"""

        if not await self.schema_exists():
            logger.debug(f"Initializing schema {self.session.schema_name}")
            await self.create_schema()

        await self.register_missing_objects()

    async def schema_exists(self) -> bool:
        """Check whether the featurebyte schema exists

        Returns
        -------
        bool
        """
        available_schemas = await self.session.list_schemas(
            database_name=self.session.database_name
        )
        return self.session.schema_name in available_schemas

    async def register_missing_functions(self, functions: list[dict[str, Any]]) -> None:
        """Register functions defined in the snowflake sql directory

        Parameters
        ----------
        functions : list[dict[str, Any]]
            List of functions to register
        """
        existing = await self.list_functions()
        items = [item for item in functions if item["identifier"] not in existing]
        await self._register_sql_objects(items)

    async def register_missing_procedures(self, procedures: list[dict[str, Any]]) -> None:
        """Register procedures defined in the snowflake sql directory

        Parameters
        ----------
        procedures: list[dict[str, Any]]
            List of procedures to register
        """
        existing = await self.list_procedures()
        items = [item for item in procedures if item["identifier"] not in existing]
        await self._register_sql_objects(items)

    async def create_missing_tables(self, tables: list[dict[str, Any]]) -> None:
        """Create tables defined in snowflake sql directory

        Parameters
        ----------
        tables: list[dict[str, Any]]
            List of tables to register
        """
        existing = await self.session.list_tables(
            database_name=self.session.database_name, schema_name=self.session.schema_name
        )
        items = [item for item in tables if item["identifier"] not in existing]
        await self._register_sql_objects(items)

    async def register_missing_objects(self) -> None:
        """Detect database objects that are missing and register them"""

        sql_objects = self.get_sql_objects()
        sql_objects_by_type: dict[SqlObjectType, list[dict[str, Any]]] = {
            SqlObjectType.FUNCTION: [],
            SqlObjectType.PROCEDURE: [],
            SqlObjectType.TABLE: [],
        }
        for sql_object in sql_objects:
            sql_objects_by_type[sql_object["type"]].append(sql_object)

        await self.register_missing_functions(sql_objects_by_type[SqlObjectType.FUNCTION])
        await self.register_missing_procedures(sql_objects_by_type[SqlObjectType.PROCEDURE])
        await self.create_missing_tables(sql_objects_by_type[SqlObjectType.TABLE])

    async def _register_sql_objects(self, items: list[dict[str, Any]]) -> None:
        for item in items:
            logger.debug(f'Registering {item["identifier"]}')
            async with aiofiles.open(item["filename"], encoding="utf-8") as f_handle:
                query = await f_handle.read()
            await self.session.execute_query(query)

    def get_sql_objects(self) -> list[dict[str, Any]]:
        """Find all the objects defined in the sql directory

        Returns
        -------
        list[str]
        """
        sql_directory = os.path.join(get_package_root(), "sql", self.sql_directory_name)
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
