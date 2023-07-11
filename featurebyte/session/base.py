"""
Session class
"""
from __future__ import annotations

from typing import Any, AsyncGenerator, ClassVar, Optional, OrderedDict

import asyncio
import contextvars
import functools
import os
import time
from abc import ABC, abstractmethod
from asyncio import events
from io import BytesIO

import aiofiles
import pandas as pd
import pyarrow as pa
from pydantic import BaseModel, PrivateAttr
from sqlglot import expressions

from featurebyte.common.path_util import get_package_root
from featurebyte.common.utils import (
    create_new_arrow_stream_writer,
    dataframe_from_arrow_stream,
    pa_table_to_record_batches,
)
from featurebyte.enum import (
    DBVarType,
    InternalName,
    MaterializedTableNamePrefix,
    SourceType,
    StrEnum,
)
from featurebyte.exception import QueryExecutionTimeOut
from featurebyte.logging import get_logger
from featurebyte.models.user_defined_function import UserDefinedFunctionModel
from featurebyte.query_graph.sql.common import (
    get_fully_qualified_table_name,
    quoted_identifier,
    sql_to_string,
)

MINUTES_IN_SECONDS = 60
HOUR_IN_SECONDS = 60 * MINUTES_IN_SECONDS
DEFAULT_EXECUTE_QUERY_TIMEOUT_SECONDS = 10 * MINUTES_IN_SECONDS
LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS = 24 * HOUR_IN_SECONDS


logger = get_logger(__name__)


async def to_thread(func: Any, timeout: float, /, *args: Any, **kwargs: Any) -> Any:
    """
    Run blocking function in a thread pool and wait for the result.
    From asyncio.to_thread implementation which is only available in Python 3.9

    Parameters
    ----------
    func : Any
        Function to run in a thread pool.
    timeout : float
        Timeout in seconds.
    *args : Any
        Positional arguments to `func`.
    **kwargs : Any
        Keyword arguments to `func`.

    Returns
    -------
    Any
    """
    loop = events.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await asyncio.wait_for(loop.run_in_executor(None, func_call), timeout)


class BaseSession(BaseModel):
    """
    Abstract session class to extract data warehouse table metadata & execute query
    """

    # pylint: disable=too-many-public-methods

    source_type: SourceType
    _connection: Any = PrivateAttr(default=None)
    _unique_id: int = PrivateAttr(default=0)
    _no_schema_error: ClassVar[Any] = Exception

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        # close connection
        self._connection.close()

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
            query=user_defined_function.generate_test_sql(source_type=self.source_type),
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
        table = pa.Table.from_pandas(dataframe)
        for batch in pa_table_to_record_batches(table):
            yield batch

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

        Raises
        ------
        QueryExecutionTimeOut
            Query execution timed out
        """
        start_time = time.time()
        cursor = self.connection.cursor()
        buffer = BytesIO()
        writer = None
        try:
            # execute in separate thread
            await to_thread(cursor.execute, timeout, query)
            if not cursor.description:
                return

            batches = self.fetch_query_stream_impl(cursor)
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

                # check for timeout
                if timeout and time.time() - start_time > timeout:
                    raise QueryExecutionTimeOut(f"Execution timeout {timeout}s exceeded.")

                await asyncio.sleep(0.1)

        except asyncio.exceptions.TimeoutError as exc:
            # raise timeout error
            raise QueryExecutionTimeOut(f"Execution timeout {timeout}s exceeded.") from exc
        finally:
            if writer:
                writer.close()
            buffer.close()
            cursor.close()
            logger.debug(
                "Query completed",
                extra={
                    "duration": time.time() - start_time,
                    "query": query.strip()[:50].replace("\n", " "),
                },
            )

    async def get_working_schema_metadata(self) -> dict[str, Any]:
        """Retrieves the working schema version from the table registered in the
        working schema.

        Returns
        -------
        If no working schema version is found, return -1. This should indicate
        to callers that we probably want to initialize the working schema.
        """

        query = "SELECT WORKING_SCHEMA_VERSION, FEATURE_STORE_ID FROM METADATA_SCHEMA"
        try:
            results = await self.execute_query(query)
        except self._no_schema_error:  # pylint: disable=broad-except
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
        self, query: str, timeout: float = DEFAULT_EXECUTE_QUERY_TIMEOUT_SECONDS
    ) -> pd.DataFrame | None:
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

    async def execute_query_long_running(
        self, query: str, timeout: float = LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS
    ) -> pd.DataFrame | None:
        """
        Execute SQL query that is expected to run for a long time

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
    async def register_table(
        self,
        table_name: str,
        dataframe: pd.DataFrame,
        temporary: bool = True,
    ) -> None:
        """
        Register a table

        Parameters
        ----------
        table_name : str
            Temp table name
        dataframe : pd.DataFrame
            DataFrame to register
        temporary : bool
            If True, register a temporary table
        """

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
        """
        fully_qualified_table_name = get_fully_qualified_table_name(
            {"table_name": table_name, "schema_name": schema_name, "database_name": database_name}
        )
        query = sql_to_string(
            expressions.Drop(
                this=expressions.Table(this=fully_qualified_table_name),
                kind="TABLE",
                exists=if_exists,
            ),
            source_type=self.source_type,
        )
        await self.execute_query(query)

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

    @classmethod
    @abstractmethod
    def is_threadsafe(cls) -> bool:
        """
        Whether the session object can be shared across threads. If True, SessionManager will cache
        the session object once it is created.
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
        table_names = await self.session.list_tables(
            self.session.database_name, self.session.schema_name
        )
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
            "CREATE TABLE IF NOT EXISTS METADATA_SCHEMA ( "
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
        update_version_query = (
            f"""UPDATE METADATA_SCHEMA SET WORKING_SCHEMA_VERSION = {new_version}"""
        )
        await self.session.execute_query(update_version_query)

    async def create_metadata_table(self) -> None:
        """Creates metadata schema table. This will be used to help
        optimize and validate parts of the session initialization.
        """
        from featurebyte.migration.run import (  # pylint: disable=import-outside-toplevel, cyclic-import
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
        update_feature_store_id_query = (
            f"""UPDATE METADATA_SCHEMA SET FEATURE_STORE_ID = '{new_feature_store_id}'"""
        )
        await self.session.execute_query(update_feature_store_id_query)
        logger.debug(f"Updated feature store ID to {new_feature_store_id}")
