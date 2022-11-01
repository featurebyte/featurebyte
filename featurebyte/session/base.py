"""
Session class
"""
from __future__ import annotations

from typing import Any, AsyncGenerator, OrderedDict

import asyncio

try:
    # fcntl is not available on Windows
    import fcntl

    FCNTL_AVAILABLE = True
except ImportError:
    FCNTL_AVAILABLE = False

import os
import threading
import time
from abc import ABC, abstractmethod

import aiofiles
import pandas as pd
import pyarrow as pa
from pydantic import BaseModel, PrivateAttr

from featurebyte.common.path_util import get_package_root
from featurebyte.common.utils import (
    create_new_arrow_stream_writer,
    dataframe_from_arrow_stream,
    pa_table_to_record_batches,
)
from featurebyte.enum import DBVarType, SourceType, StrEnum
from featurebyte.exception import QueryExecutionTimeOut
from featurebyte.logger import logger


class RunThread(threading.Thread):
    """
    Thread to execute query
    """

    def __init__(self, cursor: Any, query: str, out_fd: int, fetch_query_stream_impl: Any) -> None:
        self.cursor = cursor
        self.query = query
        self.out_fd = out_fd
        self.fetch_query_stream_impl = fetch_query_stream_impl
        self.exception: Exception | None = None
        self.is_terminated = False
        super().__init__()

    def run(self) -> None:
        """
        Run async function
        """
        try:
            # execute query
            self.cursor.execute(self.query)
            if self.is_terminated:
                return

            # stream result back via pipe
            if self.cursor.description:
                output_pipe = os.fdopen(self.out_fd, "wb")
                try:
                    self.fetch_query_stream_impl(self.cursor, output_pipe)
                finally:
                    output_pipe.close()

        except Exception as exc:  # pylint: disable=broad-except
            self.exception = exc


class BaseSession(BaseModel):
    """
    Abstract session class to extract data warehouse table metadata & execute query
    """

    source_type: SourceType
    _connection: Any = PrivateAttr(default=None)
    _unique_id: int = PrivateAttr(default=0)

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

    def generate_session_unique_id(self) -> str:
        """Generate unique id within the session

        Returns
        -------
        str
        """
        self._unique_id += 1
        return str(self._unique_id)

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
        # fetch all results into a single dataframe and write batched records to the stream
        dataframe = self.fetch_query_result_impl(cursor)
        table = pa.Table.from_pandas(dataframe)
        writer = create_new_arrow_stream_writer(output_pipe, table.schema)
        for batch in pa_table_to_record_batches(table):
            writer.write_batch(batch)

    async def get_async_query_stream(
        self, query: str, timeout: float = 600
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream results from asynchronous query as compressed arrow bytestream

        Parameters
        ----------
        query: str
            sql query to execute
        timeout: float
            timeout in seconds

        Yields
        ------
        bytes
            Byte chunk

        Raises
        ------
        exception
            Exception raised during query execution
        QueryExecutionTimeOut
            Query execution timed out
        """

        cursor = self.connection.cursor()
        in_fd, out_fd = os.pipe()
        input_pipe = os.fdopen(in_fd, "rb")
        if FCNTL_AVAILABLE:
            # set pipe to non-blocking if fcntl is available
            fcntl.fcntl(input_pipe, fcntl.F_SETFL, os.O_NONBLOCK)
        thread = RunThread(cursor, query, out_fd, self.fetch_query_stream_impl)
        thread.daemon = True
        thread.start()

        start_time = time.time()

        try:
            while True:

                # check for timeout
                if timeout and time.time() - start_time > timeout:
                    thread.is_terminated = True
                    raise QueryExecutionTimeOut(f"Execution timeout {timeout}s exceeded.")

                # read data from the pipe
                chunk = input_pipe.read()
                if chunk:
                    yield chunk

                if not thread.is_alive():
                    # read any remaining data from the pipe
                    chunk = input_pipe.read()
                    if chunk:
                        yield chunk
                    break

                # asynchronous sleep
                await asyncio.sleep(0.2)

            if thread.exception:
                raise thread.exception
        finally:
            cursor.close()
            input_pipe.close()
            thread.join(timeout=0)

    async def execute_query(self, query: str, timeout: float = 600) -> pd.DataFrame | None:
        """
        Execute SQL query

        Parameters
        ----------
        query: str
            sql query to execute
        timeout: float
            timeout in seconds

        Returns
        -------
        pd.DataFrame | None
            Query result as a pandas DataFrame if the query expects result
        """
        bytestream = self.get_async_query_stream(query=query, timeout=timeout)
        data = b""
        async for chunk in bytestream:
            data += chunk
        if not data:
            return None
        return dataframe_from_arrow_stream(data)

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

    async def register_temp_table_with_query(self, table_name: str, query: str) -> None:
        """
        Register a temporary table using a Select query

        Parameters
        ----------
        table_name : str
            Temp table name
        query : str
            SQL query for the table
        """
        await self.execute_query(f"CREATE OR REPLACE TEMPORARY TABLE {table_name} AS {query}")

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


class SqlObjectType(StrEnum):
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
        available_schemas = self._normalize_casings(
            await self.session.list_schemas(database_name=self.session.database_name)
        )
        return self._normalize_casing(self.session.schema_name) in available_schemas

    async def register_missing_functions(self, functions: list[dict[str, Any]]) -> None:
        """Register functions defined in the snowflake sql directory

        Parameters
        ----------
        functions : list[dict[str, Any]]
            List of functions to register
        """
        existing = self._normalize_casings(await self.list_functions())
        items = [item for item in functions if item["identifier"] not in existing]
        await self._register_sql_objects(items)

    async def register_missing_procedures(self, procedures: list[dict[str, Any]]) -> None:
        """Register procedures defined in the snowflake sql directory

        Parameters
        ----------
        procedures: list[dict[str, Any]]
            List of procedures to register
        """
        existing = self._normalize_casings(await self.list_procedures())
        items = [item for item in procedures if item["identifier"] not in existing]
        await self._register_sql_objects(items)

    async def create_missing_tables(self, tables: list[dict[str, Any]]) -> None:
        """Create tables defined in snowflake sql directory

        Parameters
        ----------
        tables: list[dict[str, Any]]
            List of tables to register
        """
        existing = self._normalize_casings(
            await self.session.list_tables(
                database_name=self.session.database_name, schema_name=self.session.schema_name
            )
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

            if not filename.endswith(".sql"):
                continue

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
                "identifier": self._normalize_casing(identifier),
            }
            output.append(sql_object)

        return output

    @classmethod
    def _normalize_casing(cls, identifier: str) -> str:
        # Some database warehouses convert the names returned from list_tables(), list_schemas(),
        # list_functions() etc to always upper case (Snowflake) or lower case (Databricks). To unify
        # the handling between different engines, this converts the identifiers used internally for
        # initialization purpose to be always upper case.
        return identifier.upper()

    @classmethod
    def _normalize_casings(cls, identifiers: list[str]) -> list[str]:
        return [cls._normalize_casing(x) for x in identifiers]
