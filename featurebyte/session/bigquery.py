"""
BigQuerySession class
"""

from __future__ import annotations

import collections
import datetime
import json
import logging
from typing import Any, AsyncGenerator, OrderedDict, cast

import aiofiles
import pandas as pd
import pyarrow as pa
from google.api_core.gapic_v1.client_info import ClientInfo
from google.auth.exceptions import DefaultCredentialsError, MalformedError
from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference
from google.cloud.bigquery.dbapi import Connection, DatabaseError
from google.cloud.bigquery.enums import StandardSqlTypeNames
from google.cloud.bigquery.table import TableReference
from google.oauth2 import service_account
from pydantic import PrivateAttr

from featurebyte.common.utils import ARROW_METADATA_DB_VAR_TYPE
from featurebyte.enum import DBVarType, InternalName, SourceType
from featurebyte.exception import DataWarehouseConnectionError
from featurebyte.logging import get_logger
from featurebyte.models.credential import GoogleCredential
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.table import TableDetails, TableSpec
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    get_fully_qualified_table_name,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.session.base import (
    APPLICATION_NAME,
    INTERACTIVE_SESSION_TIMEOUT_SECONDS,
    BaseSchemaInitializer,
    BaseSession,
    MetadataSchemaInitializer,
)

logger = get_logger(__name__)

logging.getLogger("bigquery.connector").setLevel(logging.ERROR)

BIGQUERY_BATCH_FETCH_SIZE = 1000

db_vartype_mapping = {
    # Legacy types
    "INTEGER": DBVarType.INT,
    "INT64": DBVarType.INT,
    "BOOLEAN": DBVarType.BOOL,
    "BOOL": DBVarType.BOOL,
    "FLOAT": DBVarType.FLOAT,
    "FLOAT64": DBVarType.FLOAT,
    "STRING": DBVarType.VARCHAR,
    "BYTES": DBVarType.BINARY,
    "TIMESTAMP": DBVarType.TIMESTAMP,
    "DATETIME": DBVarType.TIMESTAMP,
    "DATE": DBVarType.DATE,
    "TIME": DBVarType.TIME,
    "NUMERIC": DBVarType.FLOAT,
    "BIGNUMERIC": DBVarType.FLOAT,
    "STRUCT": DBVarType.DICT,
    "RECORD": DBVarType.DICT,
    # Standard SQL types
    StandardSqlTypeNames.INTERVAL: DBVarType.TIMEDELTA,
    StandardSqlTypeNames.ARRAY: DBVarType.ARRAY,
    # Unsupported types
    "GEOGRAPHY": DBVarType.UNKNOWN,
    StandardSqlTypeNames.JSON: DBVarType.UNKNOWN,
    StandardSqlTypeNames.RANGE: DBVarType.UNKNOWN,
}

pa_type_mapping = {
    # Legacy types
    "INTEGER": pa.int64(),
    "INT64": pa.int64(),
    "BOOLEAN": pa.bool_(),
    "BOOL": pa.bool_(),
    "FLOAT": pa.float64(),
    "FLOAT64": pa.float64(),
    "STRING": pa.string(),
    "BYTES": pa.large_binary(),
    "TIMESTAMP": pa.timestamp("ns", tz=None),
    "DATETIME": pa.timestamp("ns", tz=None),
    "DATE": pa.string(),
    "TIME": pa.time32("ms"),
    "NUMERIC": pa.decimal128(38, 18),
    "BIGNUMERIC": pa.decimal128(38, 18),
    "STRUCT": pa.string(),
    "RECORD": pa.string(),
    # Standard SQL types
    StandardSqlTypeNames.INTERVAL: pa.duration("ns"),
    StandardSqlTypeNames.ARRAY: pa.string(),
    # Unsupported types
    "GEOGRAPHY": DBVarType.UNKNOWN,
    StandardSqlTypeNames.JSON: DBVarType.UNKNOWN,
    StandardSqlTypeNames.RANGE: DBVarType.UNKNOWN,
}


class BigQuerySession(BaseSession):
    """
    BigQuery session class
    """

    _no_schema_error = DatabaseError
    _client: Any = PrivateAttr(default=None)

    project_name: str
    dataset_name: str
    source_type: SourceType = SourceType.BIGQUERY
    database_credential: GoogleCredential

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.database_name = self.project_name
        self.schema_name = self.dataset_name

    def __del__(self) -> None:
        super().__del__()
        # close client
        if self._client is not None:
            self._client.close()

    def _initialize_connection(self) -> None:
        try:
            assert isinstance(self.database_credential.service_account_info, dict)
            credentials = service_account.Credentials.from_service_account_info(
                self.database_credential.service_account_info
            )
            self._client = bigquery.Client(
                project=self.project_name,
                credentials=credentials,
                client_info=ClientInfo(user_agent=APPLICATION_NAME),
            )
            self._connection = Connection(client=self._client)
        except (MalformedError, DefaultCredentialsError) as exc:
            raise DataWarehouseConnectionError(str(exc)) from exc

    def initializer(self) -> BaseSchemaInitializer:
        return BigQuerySchemaInitializer(self)

    @classmethod
    def is_threadsafe(cls) -> bool:
        return True

    async def list_databases(self) -> list[str]:
        """
        Execute SQL query to retrieve database names

        Returns
        -------
        list[str]
        """
        output: list[str] = []
        next_page_token = None
        while True:
            project_iterator = self._client.list_projects(page_token=next_page_token)
            for project in project_iterator:
                output.append(project.project_id)
            next_page_token = project_iterator.next_page_token
            if not next_page_token:
                break
        return output

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
        output: list[str] = []
        next_page_token = None
        while True:
            dataset_iterator = self._client.list_datasets(
                project=database_name, page_token=next_page_token
            )
            for dataset in dataset_iterator:
                output.append(dataset.dataset_id)
            next_page_token = dataset_iterator.next_page_token
            if not next_page_token:
                break
        return output

    async def list_tables(
        self,
        database_name: str | None = None,
        schema_name: str | None = None,
        timeout: float = INTERACTIVE_SESSION_TIMEOUT_SECONDS,
    ) -> list[TableSpec]:
        output = []
        next_page_token = None
        while True:
            table_iterator = self._client.list_tables(
                dataset=f"{database_name}.{schema_name}", page_token=next_page_token
            )
            for table in table_iterator:
                output.append(
                    TableSpec(
                        name=table.table_id, description=self._client.get_table(table).description
                    )
                )
            next_page_token = table_iterator.next_page_token
            if not next_page_token:
                break
        return output

    @staticmethod
    def _convert_to_internal_variable_type(  # pylint: disable=too-many-return-statements
        bigquery_typecode: str, scale: int = 0
    ) -> DBVarType:
        if bigquery_typecode in ("NUMERIC", "BIGNUMERIC"):
            if scale > 0:
                return DBVarType.FLOAT
            return DBVarType.INT
        db_vartype = db_vartype_mapping.get(bigquery_typecode, DBVarType.UNKNOWN)
        if db_vartype == DBVarType.UNKNOWN:
            logger.warning(f"BigQuery: Not supported data type '{bigquery_typecode}'")
        return db_vartype

    @staticmethod
    def _get_pyarrow_type(datatype: str, precision: int = 0, scale: int = 0) -> pa.DataType:
        """
        Get pyarrow type from BigQuery data type

        Parameters
        ----------
        datatype: str
            BigQuery data type
        precision: int
            Precision
        scale: int
            Scale

        Returns
        -------
        pa.DataType
        """
        if datatype == "INTERVAL":
            pyarrow_type = pa.int64()
        elif datatype in {"NUMERIC", "BIGNUMERIC"}:
            if scale > 0:
                pyarrow_type = pa.decimal128(precision, scale)
            else:
                pyarrow_type = pa.int64()
        else:
            pyarrow_type = pa_type_mapping.get(datatype)

        if not pyarrow_type:
            # warn and fallback to string for unrecognized types
            logger.warning("Cannot infer pyarrow type", extra={"datatype": datatype})
            pyarrow_type = pa.string()
        return pyarrow_type

    def _get_schema_from_cursor(self, cursor: Any) -> pa.Schema:
        """
        Get schema from a cursor

        Parameters
        ----------
        cursor: Any
            Cursor to fetch data from

        Returns
        -------
        pa.Schema
        """
        fields = []
        for column in cursor.description:
            field_name = column.name
            field_type = column.type_code
            db_var_type = self._convert_to_internal_variable_type(field_type, column.scale)
            fields.append(
                pa.field(
                    field_name,
                    self._get_pyarrow_type(field_type, column.precision, column.scale),
                    metadata={ARROW_METADATA_DB_VAR_TYPE: db_var_type},
                )
            )
        return pa.schema(fields)

    def fetch_query_result_impl(self, cursor: Any) -> pd.DataFrame | None:
        """
        Fetch the result of executed SQL query from connection cursor

        This is an implementation specific to Snowflake that is more efficient when applicable.

        Parameters
        ----------
        cursor : Any
            The connection cursor

        Returns
        -------
        pd.DataFrame | None
            Query result as a pandas DataFrame if the query expects result
        """
        schema = None
        if cursor.description:
            schema = self._get_schema_from_cursor(cursor)

        if schema:
            records = cursor.fetchall()
            if not records:
                # return empty table to ensure correct schema is returned
                return pa.record_batch([[]] * len(schema), schema=schema)
            table = pa.table(list(zip(*[list(i.values()) for i in records])), schema=schema)
            return table.to_pandas()
        return None

    async def fetch_query_stream_impl(self, cursor: Any) -> AsyncGenerator[pa.RecordBatch, None]:
        schema = None
        if cursor.description:
            schema = self._get_schema_from_cursor(cursor)

        if schema:
            # fetch results in batches
            while True:
                records = cursor.fetchmany(BIGQUERY_BATCH_FETCH_SIZE)
                if not records:
                    # return empty table to ensure correct schema is returned
                    yield pa.record_batch([[]] * len(schema), schema=schema)
                    break
                table = pa.table(list(zip(*[list(i.values()) for i in records])), schema=schema)
                for batch in table.to_batches():
                    yield batch

    async def register_table(self, table_name: str, dataframe: pd.DataFrame) -> None:
        self._client.load_table_from_dataframe(
            dataframe, f"`{self.project_name}`.`{self.dataset_name}`.`{table_name}`"
        )

    async def list_table_schema(
        self,
        table_name: str | None,
        database_name: str | None = None,
        schema_name: str | None = None,
        timeout: float = INTERACTIVE_SESSION_TIMEOUT_SECONDS,
    ) -> OrderedDict[str, ColumnSpecWithDescription]:
        table_ref = TableReference(
            dataset_ref=DatasetReference(project=self.project_name, dataset_id=self.dataset_name),
            table_id=table_name,
        )
        table = self._client.get_table(table_ref)
        column_name_type_map = collections.OrderedDict()
        if table.schema is not None:
            for field in table.schema:
                dtype = self._convert_to_internal_variable_type(
                    bigquery_typecode=field.field_type, scale=field.scale
                )
                column_name_type_map[field.name] = ColumnSpecWithDescription(
                    name=field.name,
                    dtype=dtype,
                    description=field.description or None,
                )

        return column_name_type_map

    async def get_table_details(
        self,
        table_name: str,
        database_name: str | None = None,
        schema_name: str | None = None,
    ) -> TableDetails:
        query = (
            f'SELECT * FROM "{database_name}"."INFORMATION_SCHEMA"."TABLES" WHERE '
            f"\"TABLE_SCHEMA\"='{schema_name}' AND \"TABLE_NAME\"='{table_name}'"
        )
        details = await self.execute_query_interactive(query)
        if details is None or details.shape[0] == 0:
            raise self.no_schema_error(f"Table {table_name} not found.")

        fully_qualified_table_name = get_fully_qualified_table_name({
            "table_name": table_name,
            "schema_name": schema_name,
            "database_name": database_name,
        })
        return TableDetails(
            details=json.loads(details.iloc[0].to_json(orient="index")),
            fully_qualified_name=sql_to_string(
                fully_qualified_table_name, source_type=self.source_type
            ),
        )

    @staticmethod
    def get_columns_schema_from_dataframe(  # pylint: disable=too-many-branches
        dataframe: pd.DataFrame,
    ) -> list[tuple[str, str]]:
        """Get schema that can be used in CREATE TABLE statement from pandas DataFrame

        Parameters
        ----------
        dataframe : pd.DataFrame
            Input DataFrame

        Returns
        -------
        list[tuple[str, str]]
        """
        schema = []
        for colname, dtype in dataframe.dtypes.to_dict().items():
            if dataframe.shape[0] > 0 and isinstance(dataframe[colname].iloc[0], datetime.datetime):
                if dataframe[colname].iloc[0].tzinfo:
                    db_type = "TIMESTAMP_TZ"
                else:
                    db_type = "TIMESTAMP_NTZ"
            elif pd.api.types.is_datetime64_any_dtype(dataframe[colname]):
                if pd.api.types.is_datetime64tz_dtype(dataframe[colname]):
                    db_type = "TIMESTAMP_TZ"
                else:
                    db_type = "TIMESTAMP_NTZ"
            elif pd.api.types.is_bool_dtype(dtype):
                db_type = "BOOLEAN"
            elif pd.api.types.is_float_dtype(dtype):
                db_type = "DOUBLE"
            elif pd.api.types.is_integer_dtype(dtype):
                db_type = "INT"
            elif (
                dataframe.shape[0] > 0
                and dataframe[colname].apply(lambda x: x is None or isinstance(x, list)).all()
            ):
                # Consider the type as an ARRAY if all elements are None, or a list.
                db_type = "ARRAY"
            elif (
                dataframe.shape[0] > 0
                and dataframe[colname].apply(lambda x: x is None or isinstance(x, dict)).all()
            ):
                # Consider the type as an OBJECT if all elements are None, or a dict.
                db_type = "OBJECT"
            else:
                db_type = "VARCHAR"
            schema.append((colname, db_type))
        return schema

    def _format_comment(self, comment: str) -> str:
        return self.sql_to_string(make_literal_value(comment))

    async def comment_table(self, table_name: str, comment: str) -> None:
        formatted_table = self.format_quoted_identifier(table_name)
        query = f"COMMENT ON TABLE {formatted_table} IS {self._format_comment(comment)}"
        await self.execute_query(query)

    async def comment_column(self, table_name: str, column_name: str, comment: str) -> None:
        formatted_table = self.format_quoted_identifier(table_name)
        formatted_column = self.format_quoted_identifier(column_name)
        query = f"COMMENT ON COLUMN {formatted_table}.{formatted_column} IS {self._format_comment(comment)}"
        await self.execute_query(query)


class BigQuerySchemaInitializer(BaseSchemaInitializer):
    """BigQuery schema initializer class"""

    def __init__(self, session: BaseSession):
        super().__init__(session)
        self.metadata_schema_initializer = BigQueryMetadataSchemaInitializer(session)

    @property
    def sql_directory_name(self) -> str:
        return "bigquery"

    async def _register_sql_objects(self, items: list[dict[str, Any]]) -> None:
        items = self._sort_sql_objects(items)
        session = cast(BigQuerySession, self.session)
        for item in items:
            logger.debug(f'Registering {item["identifier"]}')
            async with aiofiles.open(item["filename"], encoding="utf-8") as f_handle:
                query = await f_handle.read()
                query = query.format(project=session.project_name, dataset=session.dataset_name)
            await self.session.execute_query(query)

    @property
    def current_working_schema_version(self) -> int:
        return 34

    async def create_schema(self) -> None:
        create_schema_query = (
            f"CREATE SCHEMA `{self.session.database_name}`.`{self.session.schema_name}`"
        )
        await self.session.execute_query(create_schema_query)

    async def drop_object(self, object_type: str, name: str) -> None:
        query = f"DROP {object_type} {self._fully_qualified(name)}"
        await self.session.execute_query(query)

    @staticmethod
    def _format_arguments_to_be_droppable(arguments_list: list[str]) -> list[str]:
        return [arguments.split(" RETURN", 1)[0] for arguments in arguments_list]

    async def drop_all_objects_in_working_schema(self) -> None:
        if not await self.schema_exists():
            return

        objects = await self.list_objects("USER FUNCTIONS")
        if objects.shape[0]:
            for func_name_with_args in self._format_arguments_to_be_droppable(
                objects["arguments"].tolist()
            ):
                await self.drop_object("FUNCTION", func_name_with_args)

        objects = await self.list_objects("USER PROCEDURES")
        if objects.shape[0]:
            for func_name_with_args in self._format_arguments_to_be_droppable(
                objects["arguments"].tolist()
            ):
                await self.drop_object("PROCEDURE", func_name_with_args)

        for name in await self.list_droppable_tables_in_working_schema():
            await self.drop_object("TABLE", name)

    def _fully_qualified(self, name: str) -> str:
        if "(" in name:
            # handle functions with arguments, e.g. MY_UDF(a INT, b INT)
            parts = name.split("(", 1)
            name = f"{quoted_identifier(parts[0])}({parts[1]}"
        else:
            name = f"{quoted_identifier(name)}"
        return f"{self._schema_qualifier}.{name}"


class BigQueryMetadataSchemaInitializer(MetadataSchemaInitializer):
    """BigQuery metadata schema initializer class"""

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
            "FEATURE_STORE_ID STRING, "
            "CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP() "
            ") AS "
            "SELECT 0 AS WORKING_SCHEMA_VERSION, "
            f"{current_migration_version} AS {InternalName.MIGRATION_VERSION}, "
            "NULL AS FEATURE_STORE_ID, "
            "CURRENT_TIMESTAMP() AS CREATED_AT;"
        )
