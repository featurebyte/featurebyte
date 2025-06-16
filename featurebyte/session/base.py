"""
Session class
"""

from __future__ import annotations

import asyncio
import contextvars
import ctypes
import functools
import os
import threading
import time
from abc import ABC, abstractmethod
from asyncio import events
from io import BytesIO
from random import randint
from typing import Any, AsyncGenerator, ClassVar, Dict, Literal, Optional, OrderedDict, Type

import aiofiles
import pandas as pd
import pyarrow as pa
from bson import ObjectId
from cachetools import TTLCache
from pydantic import BaseModel, ConfigDict, PrivateAttr
from sqlglot import expressions
from sqlglot.expressions import Expression, Select

from featurebyte.common.path_util import get_package_root
from featurebyte.common.utils import (
    create_new_arrow_stream_writer,
    dataframe_from_arrow_table,
)
from featurebyte.enum import InternalName, MaterializedTableNamePrefix, SourceType, StrEnum
from featurebyte.exception import (
    DataWarehouseOperationError,
    QueryExecutionTimeOut,
    TaskRevokeExceptions,
)
from featurebyte.logging import get_logger
from featurebyte.models.user_defined_function import UserDefinedFunctionModel
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.table import TableDetails, TableSpec
from featurebyte.query_graph.node.schema import TableDetails as NodeTableDetails
from featurebyte.query_graph.sql.adapter import BaseAdapter, get_sql_adapter
from featurebyte.query_graph.sql.common import (
    get_fully_qualified_table_name,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.source_info import SourceInfo

INTERACTIVE_SESSION_TIMEOUT_SECONDS = 30
NON_INTERACTIVE_SESSION_TIMEOUT_SECONDS = 1200
MINUTES_IN_SECONDS = 60
HOUR_IN_SECONDS = 60 * MINUTES_IN_SECONDS
INTERACTIVE_QUERY_TIMEOUT_SECONDS = 30
DEFAULT_EXECUTE_QUERY_TIMEOUT_SECONDS = 10 * MINUTES_IN_SECONDS
LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS = 24 * HOUR_IN_SECONDS
APPLICATION_NAME = "FeatureByte"
MAX_WAIT_TIME_FOR_QUERY_HANDLER = (
    30  # max seconds to wait for query handler to be available for query cancellation
)
session_cache: TTLCache[Any, Any] = TTLCache(maxsize=1024, ttl=600)

logger = get_logger(__name__)


async def to_thread(
    func: Any, timeout: float, error_handler: Any, /, *args: Any, **kwargs: Any
) -> Any:
    """
    Run blocking function in a thread pool and wait for the result.
    From asyncio.to_thread implementation which is only available in Python 3.9

    Parameters
    ----------
    func : Any
        Function to run in a thread pool.
    timeout : float
        Timeout in seconds.
    error_handler : Any
        Error handler function to call on error.
    *args : Any
        Positional arguments to `func`.
    **kwargs : Any
        Keyword arguments to `func`.

    Returns
    -------
    Any

    # noqa: DAR401
    """
    loop = events.get_running_loop()
    ctx = contextvars.copy_context()

    def _func_wrapper(func: Any, thread_info: Dict[str, Any], *args: Any, **kwargs: Any) -> Any:
        thread_info["tid"] = threading.get_ident()
        return func(*args, **kwargs)

    def _raise_exception_in_thread(thread_id: int) -> None:
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(thread_id), ctypes.py_object(asyncio.exceptions.CancelledError)
        )
        if res > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
            raise ValueError("Exception raise failure")
        if res:
            logger.info("Raised exception in thread")

    thread_info: Dict[str, int] = {}
    func_call = functools.partial(ctx.run, _func_wrapper, func, thread_info, *args, **kwargs)

    try:
        coro = loop.run_in_executor(None, func_call)
        start_time = time.time()
        while time.time() - start_time < timeout:
            # let the event loop run
            await asyncio.sleep(0.1)
            if coro.done():
                return coro.result()
            if coro.cancelled():
                raise asyncio.exceptions.CancelledError
        coro.cancel()
        raise asyncio.exceptions.TimeoutError
    except (asyncio.exceptions.TimeoutError,) + TaskRevokeExceptions:
        if error_handler:
            try:
                await error_handler()
            except Exception:
                logger.error("Error handling exception", exc_info=True)

        tid = thread_info.get("tid")
        if tid:
            _raise_exception_in_thread(thread_info["tid"])
        raise


class QueryMetadata(BaseModel):
    """
    Query metadata model
    """

    query_id: Optional[str] = None

    model_config = ConfigDict(
        validate_assignment=True,
    )


class BaseSession(BaseModel):
    """
    Abstract session class to extract data warehouse table metadata & execute query
    """

    source_type: SourceType
    database_name: str = ""
    schema_name: str = ""
    _connection: Any = PrivateAttr(default=None)
    _cache_key: Any = PrivateAttr(default=None)
    _no_schema_error: ClassVar[Type[Exception]] = Exception

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._initialize_connection()

    def _initialize_connection(self) -> None:
        """Initialize connection"""

    def __del__(self) -> None:
        # close connection
        if self._connection is not None:
            self._connection.close()

    @property
    def metadata_schema(self) -> str:
        """
        Metadata schema name

        Returns
        -------
        str
        """
        if self.source_type == SourceType.TEST:
            return "METADATA_SCHEMA"

        fully_qualified_table_name = get_fully_qualified_table_name({
            "table_name": "METADATA_SCHEMA",
            "schema_name": self.schema_name,
            "database_name": self.database_name,
        })
        return sql_to_string(fully_qualified_table_name, source_type=self.source_type)

    def set_cache_key(self, key: Any) -> None:
        """
        Set hash key used to cache session object

        Parameters
        ----------
        key: Any
            Hash key
        """
        self._cache_key = key

    async def clone_if_not_threadsafe(self) -> BaseSession:
        """
        Create a new session object from a session that is not threadsafe

        Returns
        -------
        BaseSession
        """
        if self.is_threadsafe():
            return self
        new_session = self.model_copy()
        new_session._initialize_connection()
        return new_session

    @property
    def no_schema_error(self) -> Type[Exception]:
        """
        Exception to raise when schema is not found

        Returns
        -------
        Type[Exception]
        """
        return self._no_schema_error

    @property
    def adapter(self) -> BaseAdapter:
        """
        Returns an adapter instance for the session's source type

        Returns
        -------
        BaseAdapter
        """
        return get_sql_adapter(source_info=self.get_source_info())

    def get_source_info(self) -> SourceInfo:
        """
        Get source information

        Returns
        -------
        SourceInfo
        """
        return SourceInfo(
            database_name=self.database_name,
            schema_name=self.schema_name,
            source_type=self.source_type,
        )

    async def initialize(self) -> None:
        """
        Initialize session
        """
        initializer = self.initializer()
        if initializer is not None:
            await initializer.initialize()

    @abstractmethod
    def initializer(self) -> Optional[BaseSchemaInitializer]:
        """
        Returns an instance of schema initializer
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
    async def _cancel_query(self, cursor: Any, query: str) -> bool:
        """
        Cancel query

        Parameters
        ----------
        cursor : Any
            The connection cursor
        query : str
            SQL query

        Returns
        -------
        bool
            True if query was cancelled, False otherwise
        """

    async def _try_cancel_query(self, cursor: Any, query: str) -> None:
        """
        Cancel query

        Parameters
        ----------
        cursor : Any
            The connection cursor
        query : str
            SQL query
        """
        logger.info(f"Cancelling query: {query}")
        # try to cancel the query for a maximum of MAX_WAIT_TIME_FOR_QUERY_HANDLER seconds
        start = time.time()
        while time.time() - start < MAX_WAIT_TIME_FOR_QUERY_HANDLER:
            if await self._cancel_query(cursor, query):
                return
            await asyncio.sleep(1)

    @classmethod
    def generate_session_unique_id(cls) -> str:
        """Generate unique id within the session

        Returns
        -------
        str
        """
        return str(ObjectId()).upper()

    @abstractmethod
    async def _list_databases(self) -> list[str]:
        pass

    @abstractmethod
    async def _list_schemas(self, database_name: str | None = None) -> list[str]:
        pass

    @abstractmethod
    async def _list_tables(
        self,
        database_name: str | None = None,
        schema_name: str | None = None,
        timeout: float = INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    ) -> list[TableSpec]:
        pass

    async def list_databases(self) -> list[str]:
        """
        Execute SQL query to retrieve database names

        Returns
        -------
        list[str]
        """
        return sorted(await self._list_databases())

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
        return sorted(await self._list_schemas(database_name=database_name))

    async def list_tables(
        self,
        database_name: str | None = None,
        schema_name: str | None = None,
        timeout: float = INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    ) -> list[TableSpec]:
        """
        Execute SQL query to retrieve table names

        Parameters
        ----------
        database_name: str | None
            Database name
        schema_name: str | None
            Schema name
        timeout: float
            Timeout in seconds

        Returns
        -------
        list[TableSpec]
        """
        return sorted(
            await self._list_tables(
                database_name=database_name, schema_name=schema_name, timeout=timeout
            ),
            key=lambda x: x.name,
        )

    @abstractmethod
    async def list_table_schema(
        self,
        table_name: str | None,
        database_name: str | None = None,
        schema_name: str | None = None,
        timeout: float = INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    ) -> OrderedDict[str, ColumnSpecWithDescription]:
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
        timeout: float
            Timeout in seconds

        Returns
        -------
        OrderedDict[str, ColumnSpecWithDescription]
        """

    @abstractmethod
    async def comment_table(self, table_name: str, comment: str) -> None:
        """
        Add comment to a table

        Parameters
        ----------
        table_name: str
            Name of the table to be commented
        comment: str
            Comment to add
        """

    @abstractmethod
    async def comment_column(self, table_name: str, column_name: str, comment: str) -> None:
        """
        Add comment to a column in a table

        Parameters
        ----------
        table_name: str
            Table name of the column
        column_name: str
            Name of the colum to be commented
        comment: str
            Comment to add
        """

    @abstractmethod
    async def register_table(self, table_name: str, dataframe: pd.DataFrame) -> None:
        """
        Register a table

        Parameters
        ----------
        table_name : str
            Temp table name
        dataframe : pd.DataFrame
            DataFrame to register
        """

    @classmethod
    @abstractmethod
    def is_threadsafe(cls) -> bool:
        """
        Whether the session object can be shared across threads. If True, SessionManager will cache
        the session object once it is created.
        """

    def get_fully_qualified_table_name(self, table_name: str) -> str:
        """
        Get fully qualified table name

        Parameters
        ----------
        table_name: str
            Table name

        Returns
        -------
        str
        """
        return sql_to_string(
            get_fully_qualified_table_name({
                "table_name": table_name,
                "schema_name": self.schema_name,
                "database_name": self.database_name,
            }),
            source_type=self.source_type,
        )

    async def get_table_details(
        self,
        table_name: str,
        database_name: str | None = None,
        schema_name: str | None = None,
    ) -> TableDetails:
        """
        Get table details

        Parameters
        ----------
        table_name: str
            Table name
        database_name: str | None
            Database name
        schema_name: str | None
            Schema name

        Returns
        -------
        TableDetails
        """
        _ = database_name, schema_name, table_name
        # return empty details as fallback
        return TableDetails(fully_qualified_name=table_name)

    async def check_user_defined_function(
        self,
        user_defined_function: UserDefinedFunctionModel,
        timeout: float = DEFAULT_EXECUTE_QUERY_TIMEOUT_SECONDS,
    ) -> None:
        """
        Check if a user defined function exists in the feature store

        Parameters
        ----------
        user_defined_function: UserDefinedFunctionModel
            User defined function model
        timeout: float
            Timeout in seconds
        """
        await self.execute_query(
            query=user_defined_function.generate_test_sql(source_info=self.get_source_info()),
            timeout=timeout,
        )

    async def fetch_query_stream_impl(self, cursor: Any) -> AsyncGenerator[pa.RecordBatch, None]:
        """
        Stream pyarrow record batches from cursor

        Parameters
        ----------
        cursor : Any
            The connection cursor

        Yields
        ------
        pa.RecordBatch
            Pyarrow record batch
        """
        # fetch all results into a single dataframe and write batched records to the stream
        dataframe = self.fetch_query_result_impl(cursor)
        yield pa.record_batch(dataframe)

    def _execute_query(self, cursor: Any, query: str, **kwargs: Any) -> Any:
        return cursor.execute(query, **kwargs)

    async def get_async_query_generator(
        self,
        query: str,
        timeout: float = LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS,
        query_metadata: QueryMetadata | None = None,
    ) -> AsyncGenerator[pa.RecordBatch, None]:
        """
        Generate results from asynchronous query as pyarrow record batches

        Parameters
        ----------
        query: str
            sql query to execute
        timeout: float
            timeout in seconds
        query_metadata: QueryMetadata | None
            Query metadata object

        Yields
        ------
        bytes
            Byte chunk

        Raises
        ------
        QueryExecutionTimeOut
            Query execution timed out
        """
        start_time = time.time()
        cursor = self.connection.cursor()
        try:
            # execute in separate thread
            await to_thread(
                self._execute_query,
                timeout,
                lambda: self._try_cancel_query(cursor, query),
                cursor,
                query,
                **self.get_additional_execute_query_kwargs(),
            )

            if query_metadata:
                query_metadata.query_id = self.get_query_id(cursor)

            if not cursor.description:
                return

            batches = self.fetch_query_stream_impl(cursor)
            async for batch in batches:
                yield batch
                # check for timeout
                if timeout and time.time() - start_time > timeout:
                    raise QueryExecutionTimeOut(f"Execution timeout {timeout}s exceeded.")

        except asyncio.exceptions.TimeoutError as exc:
            # raise timeout error
            raise QueryExecutionTimeOut(f"Execution timeout {timeout}s exceeded.") from exc
        finally:
            cursor.close()
            logger.debug(
                "Query completed",
                extra={
                    "duration": time.time() - start_time,
                    "query": query.strip()[:50].replace("\n", " "),
                },
            )

    async def get_async_query_stream(
        self, query: str, timeout: float = LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS
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
        """
        buffer = BytesIO()
        writer = None
        batches = self.get_async_query_generator(query=query, timeout=timeout)
        try:
            async for batch in batches:
                if not writer:
                    writer = create_new_arrow_stream_writer(buffer, batch.schema)
                writer.write_batch(batch)
                buffer.flush()
                buffer.seek(0)
                chunk = buffer.getvalue()
                if chunk:
                    yield chunk
                    buffer.seek(0)
                    buffer.truncate(0)
        finally:
            if writer:
                writer.close()
            buffer.close()

    async def get_working_schema_metadata(self) -> dict[str, Any]:
        """Retrieves the working schema version from the table registered in the
        working schema.

        Returns
        -------
        If no working schema version is found, return -1. This should indicate
        to callers that we probably want to initialize the working schema.
        """

        query = f"SELECT WORKING_SCHEMA_VERSION, FEATURE_STORE_ID FROM {self.metadata_schema}"
        try:
            results = await self.execute_query(query, to_log_error=False)
        except self._no_schema_error:
            # Snowflake and Databricks will error if the table is not initialized
            # We will need to catch more errors here if/when we add support for
            # more platforms.
            return {
                "version": MetadataSchemaInitializer.SCHEMA_NOT_REGISTERED,
            }

        # Check the working schema version if it is already initialized.
        if results is None or results.empty:
            return {"version": MetadataSchemaInitializer.SCHEMA_NO_RESULTS_FOUND}
        return {
            "version": int(results["WORKING_SCHEMA_VERSION"][0]),
            "feature_store_id": results["FEATURE_STORE_ID"][0],
        }

    async def execute_query(
        self,
        query: str,
        timeout: float = DEFAULT_EXECUTE_QUERY_TIMEOUT_SECONDS,
        to_log_error: bool = True,
        query_metadata: QueryMetadata | None = None,
    ) -> pd.DataFrame | None:
        """
        Execute SQL query

        Parameters
        ----------
        query: str
            sql query to execute
        timeout: float
            timeout in seconds
        to_log_error: bool
            If True, log error
        query_metadata: QueryMetadata | None
            Query metadata object

        Returns
        -------
        pd.DataFrame | None
            Query result as a pandas DataFrame if the query expects result

        Raises
        ------
        Exception
            If query execution fails
        """
        try:
            batch_records = self.get_async_query_generator(
                query=query, timeout=timeout, query_metadata=query_metadata
            )
            batches = [batch_record async for batch_record in batch_records]
            if not batches:
                return None
            return dataframe_from_arrow_table(pa.Table.from_batches(batches))
        except Exception as exc:
            if to_log_error:
                logger.error(
                    "Error executing query", extra={"query": query, "source_type": self.source_type}
                )

            # remove session from cache if query fails
            if self._cache_key:
                session_cache.pop(self._cache_key, None)
            raise exc

    async def execute_query_interactive(
        self, query: str, timeout: float = INTERACTIVE_QUERY_TIMEOUT_SECONDS
    ) -> pd.DataFrame | None:
        """
        Execute SQL query that is expected to run for a short time

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
        return await self.execute_query(query=query, timeout=timeout)

    async def execute_query_long_running(
        self,
        query: str,
        timeout: float = LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS,
        to_log_error: bool = True,
        query_metadata: QueryMetadata | None = None,
    ) -> pd.DataFrame | None:
        """
        Execute SQL query that is expected to run for a long time

        Parameters
        ----------
        query: str
            sql query to execute
        timeout: float
            timeout in seconds
        to_log_error: bool
            If True, log error
        query_metadata: QueryMetadata | None
            Query metadata object

        Returns
        -------
        pd.DataFrame | None
            Query result as a pandas DataFrame if the query expects result
        """
        return await self.execute_query(
            query=query, timeout=timeout, to_log_error=to_log_error, query_metadata=query_metadata
        )

    @staticmethod
    def get_query_id(cursor: Any) -> str | None:
        """
        Get query ID from the cursor

        Parameters
        ----------
        cursor: Any
            The connection cursor

        Returns
        -------
        str | None
            Query ID if available, otherwise None
        """
        _ = cursor
        return None

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

        Raises
        ------
        Exception
            If query execution fails
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, **self.get_additional_execute_query_kwargs())
            result = self.fetch_query_result_impl(cursor)
            return result
        except Exception as exc:
            # remove session from cache if query fails
            if self._cache_key:
                session_cache.pop(self._cache_key, None)
            raise exc
        finally:
            cursor.close()

    def get_additional_execute_query_kwargs(self) -> dict[str, Any]:
        """
        Additional keywords arguments to specify when calling execute() on cursor

        Returns
        -------
        dict[str, Any]
        """
        return {}

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

    async def register_table_with_query(
        self, table_name: str, query: str, temporary: bool = True
    ) -> None:
        """
        Register a temporary table using a Select query

        Parameters
        ----------
        table_name : str
            Temp table name
        query : str
            SQL query for the table
        temporary : bool
            If True, register a temporary table
        """
        if temporary:
            create_command = "CREATE OR REPLACE TEMPORARY TABLE"
        else:
            create_command = "CREATE OR REPLACE TABLE"
        await self.execute_query_long_running(f"{create_command} {table_name} AS {query}")

    async def drop_table(
        self,
        table_name: str,
        schema_name: str,
        database_name: str,
        if_exists: bool = False,
        timeout: float = LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS,
        table_is_view: bool = False,
    ) -> None:
        """
        Drop a table

        Parameters
        ----------
        table_name : str
            Table name
        schema_name : str
            Schema name
        database_name : str
            Database name
        if_exists : bool
            If True, drop the table only if it exists
        timeout : float
            Timeout in seconds
        table_is_view : bool
            If True, drop the table as a view

        Raises
        ------
        DataWarehouseOperationError
            If the operation failed
        """

        async def _drop(is_view: bool) -> None:
            fully_qualified_table_name = get_fully_qualified_table_name({
                "table_name": table_name,
                "schema_name": schema_name,
                "database_name": database_name,
            })
            query = sql_to_string(
                expressions.Drop(
                    this=expressions.Table(this=fully_qualified_table_name),
                    kind="VIEW" if is_view else "TABLE",
                    exists=if_exists,
                ),
                source_type=self.source_type,
            )
            await self.execute_query(query, timeout=timeout)

        if table_is_view:
            await _drop(is_view=table_is_view)
        else:
            try:
                await _drop(is_view=False)
            except Exception as exc:
                msg = str(exc)
                if "VIEW" in msg:
                    try:
                        await _drop(is_view=True)
                        return
                    except Exception as exc_view:
                        msg = str(exc_view)
                        raise DataWarehouseOperationError(msg) from exc_view
                raise DataWarehouseOperationError(msg) from exc

    async def drop_tables(
        self,
        table_names: list[str],
        schema_name: str,
        database_name: str,
        if_exists: bool = False,
        timeout: float = LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS,
    ) -> None:
        """
        Drop multiple tables

        Parameters
        ----------
        table_names: list[str]
            List of table names
        schema_name: str
            Schema name
        database_name: str
            Database name
        if_exists: bool
            If True, drop the table only if it exists
        timeout: float
            Timeout in seconds

        Raises
        ------
        Exception
            If multiple errors occurred
        """
        # Create tasks for all drop operations
        errors = []
        for table_name in table_names:
            try:
                await self.drop_table(
                    table_name=table_name,
                    schema_name=schema_name,
                    database_name=database_name,
                    if_exists=if_exists,
                    timeout=timeout,
                )
            except Exception as exc:
                errors.append(exc)

        # If there are any errors, raise an aggregated exception
        if errors:
            error_messages = "; ".join(str(e) for e in errors)
            raise Exception(f"Errors occurred: {error_messages}")

    def format_quoted_identifier(self, identifier_name: str) -> str:
        """
        Quote an identifier using the session's convention and return the result as a string

        Parameters
        ----------
        identifier_name: str
            Identifier name

        Returns
        -------
        str
        """
        return self.sql_to_string(quoted_identifier(identifier_name))

    def sql_to_string(self, expr: Expression) -> str:
        """
        Helper function to convert an Expression to string for the session's source type

        Parameters
        ----------
        expr: Expression
            Expression to convert to string

        Returns
        -------
        str
        """
        return sql_to_string(expr, source_type=self.source_type)

    async def retry_sql(
        self,
        sql: str,
        retry_num: int = 10,
        sleep_interval: int = 5,
        query_metadata: QueryMetadata | None = None,
    ) -> pd.DataFrame | None:
        """
        Retry sql operation

        Parameters
        ----------
        sql: str
            SQL query
        retry_num: int
            Number of retries
        sleep_interval: int
            Sleep interval between retries
        query_metadata: QueryMetadata | None
            Query metadata object

        Returns
        -------
        pd.DataFrame
            Result of the sql operation

        Raises
        ------
        Exception
            if the sql operation fails after retry_num retries
        """

        for i in range(retry_num):
            try:
                return await self.execute_query_long_running(sql, query_metadata=query_metadata)
            except Exception as exc:
                logger.warning(
                    "SQL query failed",
                    extra={"attempt": i, "query": sql.strip()[:50].replace("\n", " ")},
                )
                if i == retry_num - 1:
                    logger.error(
                        "SQL query failed", extra={"attempts": retry_num, "exception": exc}
                    )
                    raise

            random_interval = randint(1, sleep_interval)
            await asyncio.sleep(random_interval)

        return None

    async def table_exists(self, table_details: NodeTableDetails) -> bool:
        """
        Check if table exists

        Parameters
        ----------
        table_details: NodeTableDetails
            Table to check

        Returns
        -------
            True if table exists, False otherwise
        """
        try:
            expr = (
                expressions.Select(expressions=[expressions.Star()])
                .from_(get_fully_qualified_table_name(table_details.model_dump()))
                .limit(0)
            )
            await self.execute_query_long_running(
                sql_to_string(expr, self.source_type), to_log_error=False
            )
            return True
        except self._no_schema_error:
            pass
        return False

    async def create_table_as(
        self,
        table_details: NodeTableDetails | str,
        select_expr: Select | str,
        kind: Literal["TABLE", "VIEW"] = "TABLE",
        partition_keys: list[str] | None = None,
        replace: bool = False,
        exists: bool = False,
        retry: bool = False,
        retry_num: int = 10,
        sleep_interval: int = 5,
        query_metadata: QueryMetadata | None = None,
    ) -> pd.DataFrame | None:
        """
        Create a table using a select statement

        Parameters
        ----------
        table_details: NodeTableDetails | str
            Details or name of the table to be created
        select_expr: Select | str
            Select expression or SQL to create the table
        partition_keys: list[str] | None
            Partition keys
        kind: Literal["TABLE", "VIEW"]
            Kind of table to create
        replace: bool
            Whether to replace the table if exists
        exists: bool
            Whether to create the table only if it doesn't exist
        retry: bool
            Whether to retry the operation
        retry_num: int
            Number of retries
        sleep_interval: int
            Sleep interval between retries
        query_metadata: QueryMetadata | None
            Query metadata object

        Returns
        -------
        pd.DataFrame | None

        Raises
        ------
        self.no_schema_error
            If table creation failed
        """
        if isinstance(table_details, str):
            table_details = NodeTableDetails(
                database_name=None,
                schema_name=None,
                table_name=table_details.upper(),  # use upper case for unquoted identifiers
            )
        if isinstance(select_expr, str):
            select_expr = f"SELECT * FROM ({select_expr})"

        query = sql_to_string(
            self.adapter.create_table_as(
                table_details=table_details,
                select_expr=select_expr,
                kind=kind,
                partition_keys=partition_keys,
                replace=replace,
                exists=exists,
            ),
            source_type=self.source_type,
        )

        try:
            if retry:
                return await self.retry_sql(
                    query,
                    retry_num=retry_num,
                    sleep_interval=sleep_interval,
                    query_metadata=query_metadata,
                )
            if query_metadata is None:
                return await self.execute_query_long_running(query)
            return await self.execute_query_long_running(query, query_metadata=query_metadata)
        except self.no_schema_error:
            if exists:
                # Some connectors like Snowflake raise error even though CREATE TABLE IF EXISTS
                # statement succeeded when there is an existing table.
                if await self.table_exists(table_details):
                    # Exit without raising an error if the table already exists
                    return None
                # Otherwise, this is a legitimate error causing the table creation to fail
                raise
            raise


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
        self.metadata_schema_initializer = MetadataSchemaInitializer(session)

    @abstractmethod
    async def create_schema(self) -> None:
        """Create the featurebyte working schema"""

    @property
    @abstractmethod
    def sql_directory_name(self) -> str:
        """Directory name containing the SQL initialization scripts"""

    @property
    @abstractmethod
    def current_working_schema_version(self) -> int:
        """Gets the current working schema version.

        We should increase this value if we want to re-run the session
        initialization functions (eg. registering new functions, procedures
        and tables).

        We can consider moving this to a config/json file down the line, but
        opting to keep it simple for now.

        Returns
        -------
        int that is the current working schema version.
        """

    @abstractmethod
    async def drop_all_objects_in_working_schema(self) -> None:
        """
        Reset state of working schema by dropping all existing objects (tables, functions,
        procedures, etc)
        """

    @abstractmethod
    async def drop_object(self, object_type: str, name: str) -> None:
        """
        Drop an object of a given type in the working schema

        Parameters
        ----------
        object_type : str
            Type of object to drop
        name : str
            Name of object to drop
        """

    async def initialize(self) -> None:
        """Entry point to set up the featurebyte working schema"""

        if not await self.should_update_schema():
            return

        if not await self.schema_exists():
            logger.debug(f"Initializing schema {self.session.schema_name}")
            await self.create_schema()

        await self.register_missing_objects()

    async def should_update_schema(self) -> bool:
        """Compares the working_schema_version defined in the codebase, with
        what is registered in working schema table.

        Returns
        -------
        This function will return true if:
        - there is no working schema defined, or
        - the working_schema_version defined in the code base is greater than the version number
          registered in the working schema table.
        """
        metadata = await self.session.get_working_schema_metadata()
        registered_working_schema_version = int(metadata["version"])
        if registered_working_schema_version == MetadataSchemaInitializer.SCHEMA_NOT_REGISTERED:
            return True
        if registered_working_schema_version == MetadataSchemaInitializer.SCHEMA_NO_RESULTS_FOUND:
            logger.debug(f"No results found for the working schema {self.session.schema_name}")
            return True
        current_version = self.current_working_schema_version
        return current_version > registered_working_schema_version

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

    async def list_objects(self, object_type: str) -> pd.DataFrame:
        """
        List objects of a given type in the working schema

        Parameters
        ----------
        object_type : str
            Type of object to list

        Returns
        -------
        pd.DataFrame
        """
        query = f"SHOW {object_type} IN {self._schema_qualifier}"
        return await self.session.execute_query(query)

    async def register_missing_functions(self, functions: list[dict[str, Any]]) -> None:
        """Register functions defined in the snowflake sql directory.

        Note that this will overwrite any existing functions. We should be look to handle this
        properly in the future (potentially by versioning) to prevent breaking changes from
        being released automatically.

        Parameters
        ----------
        functions : list[dict[str, Any]]
            List of functions to register
        """
        await self._register_sql_objects(list(functions))

    async def register_missing_procedures(self, procedures: list[dict[str, Any]]) -> None:
        """Register procedures defined in the snowflake sql directory

        Note that this will overwrite any existing procedures. We should be look to handle this
        properly in the future (potentially by versioning) to prevent breaking changes from
        being released automatically.

        Parameters
        ----------
        procedures: list[dict[str, Any]]
            List of procedures to register
        """
        await self._register_sql_objects(list(procedures))

    async def create_missing_tables(self, tables: list[dict[str, Any]]) -> None:
        """Create tables defined in snowflake sql directory

        Parameters
        ----------
        tables: list[dict[str, Any]]
            List of tables to register
        """
        table_specs = await self.session.list_tables(
            database_name=self.session.database_name, schema_name=self.session.schema_name
        )
        existing = self._normalize_casings([table.name for table in table_specs])
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

        await self.metadata_schema_initializer.create_metadata_table()
        await self.register_missing_functions(sql_objects_by_type[SqlObjectType.FUNCTION])
        await self.register_missing_procedures(sql_objects_by_type[SqlObjectType.PROCEDURE])
        await self.create_missing_tables(sql_objects_by_type[SqlObjectType.TABLE])
        await self.metadata_schema_initializer.update_metadata_schema_version(
            self.current_working_schema_version
        )

    @staticmethod
    def _sort_sql_objects(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Order sql objects taking into account dependencies between then

        Parameters
        ----------
        items: list[dict[str, Any]
            SQL items to sort

        Returns
        -------
        list[dict[str, Any]
        """
        depended_items = []
        dependant_items = []
        for item in items:
            if item["identifier"] == "F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE":
                depended_items.append(item)
            else:
                dependant_items.append(item)
        return depended_items + dependant_items

    async def _register_sql_objects(self, items: list[dict[str, Any]]) -> None:
        items = self._sort_sql_objects(items)
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
            elif sql_object_type == SqlObjectType.FUNCTION and identifier == "F_OBJECT_DELETE":
                # OBJECT_DELETE does not include "F_" prefix
                identifier = identifier[len("F_") :]

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
        # list_objects() etc to always upper case (Snowflake) or lower case (Databricks). To unify
        # the handling between different engines, this converts the identifiers used internally for
        # initialization purpose to be always upper case.
        return identifier.upper()

    @classmethod
    def _normalize_casings(cls, identifiers: list[str]) -> list[str]:
        return [cls._normalize_casing(x) for x in identifiers]

    @classmethod
    def remove_materialized_tables(cls, table_names: list[str]) -> list[str]:
        """
        Remove materialized tables from the list of table names

        Parameters
        ----------
        table_names: list[str]
            List of table names to filter

        Returns
        -------
        list[str]
        """
        out = []
        materialized_table_prefixes = {
            cls._normalize_casing(name) for name in MaterializedTableNamePrefix.all()
        }
        for table_name in table_names:
            for prefix in materialized_table_prefixes:
                if cls._normalize_casing(table_name).startswith(prefix):
                    break
            else:
                out.append(table_name)
        return out

    async def list_droppable_tables_in_working_schema(self) -> list[str]:
        """
        List tables in the working schema that can be dropped without losing data. These are the
        tables that will be reinstated by WorkingSchemaService when recreating the working schema.

        Returns
        -------
        list[str]
        """
        tables = await self.session.list_tables(
            self.session.database_name, self.session.schema_name
        )
        table_names = [table.name for table in tables]
        return self.remove_materialized_tables(table_names)

    @property
    def _schema_qualifier(self) -> str:
        db_quoted = sql_to_string(
            quoted_identifier(self.session.database_name), self.session.source_type
        )
        schema_quoted = sql_to_string(
            quoted_identifier(self.session.schema_name), self.session.source_type
        )
        return f"{db_quoted}.{schema_quoted}"


class MetadataSchemaInitializer:
    """Responsible for initializing the metadata schema table
    in the working schema.

    Parameters
    ----------
    session : BaseSession
        Session object
    """

    SCHEMA_NOT_REGISTERED = -1
    SCHEMA_NO_RESULTS_FOUND = -2

    def __init__(self, session: BaseSession):
        self.session = session

    async def create_metadata_table_if_not_exists(self, current_migration_version: int) -> None:
        """Create metadata table if it doesn't exist

        Parameters
        ----------
        current_migration_version: int
            Current migration version
        """
        await self.session.execute_query(
            f"CREATE TABLE IF NOT EXISTS {self.session.metadata_schema} ( "
            "WORKING_SCHEMA_VERSION INT, "
            f"{InternalName.MIGRATION_VERSION} INT, "
            "FEATURE_STORE_ID VARCHAR, "
            "CREATED_AT TIMESTAMP DEFAULT SYSDATE() "
            ") AS "
            "SELECT 0 AS WORKING_SCHEMA_VERSION, "
            f"{current_migration_version} AS {InternalName.MIGRATION_VERSION}, "
            "NULL AS FEATURE_STORE_ID, "
            "SYSDATE() AS CREATED_AT;"
        )

    async def update_metadata_schema_version(self, new_version: int) -> None:
        """Inserts default information into the metadata schema.

        Parameters
        ----------
        new_version : int
            New version to update the working schema to
        """
        update_version_query = f"""UPDATE {self.session.metadata_schema} SET WORKING_SCHEMA_VERSION = {new_version} WHERE 1=1"""
        await self.session.execute_query(update_version_query)

    async def create_metadata_table(self) -> None:
        """Creates metadata schema table. This will be used to help
        optimize and validate parts of the session initialization.
        """
        from featurebyte.migration.run import (
            retrieve_all_migration_methods,
        )

        current_migration_version = max(
            retrieve_all_migration_methods(data_warehouse_migrations_only=True)
        )
        logger.debug("Creating METADATA_SCHEMA table")
        await self.create_metadata_table_if_not_exists(current_migration_version)

    async def update_feature_store_id(self, new_feature_store_id: str) -> None:
        """Inserts default information into the metadata schema.

        Parameters
        ----------
        new_feature_store_id : str
            New feature_store_id to update the working schema to
        """
        update_feature_store_id_query = f"""UPDATE {self.session.metadata_schema} SET FEATURE_STORE_ID = '{new_feature_store_id}' WHERE 1=1"""
        await self.session.execute_query(update_feature_store_id_query)
        logger.debug(f"Updated feature store ID to {new_feature_store_id}")
