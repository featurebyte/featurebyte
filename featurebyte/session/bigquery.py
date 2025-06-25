"""
BigQuerySession class
"""

from __future__ import annotations

import collections
import datetime
import json
import logging
import uuid
from typing import Any, AsyncGenerator, Optional, OrderedDict, cast

import aiofiles
import pandas as pd
import pyarrow as pa
from dateutil import tz
from google.api_core.exceptions import Forbidden, NotFound
from google.api_core.gapic_v1.client_info import ClientInfo
from google.auth.exceptions import DefaultCredentialsError, MalformedError
from google.cloud import bigquery
from google.cloud.bigquery import (
    Client,
    DatasetReference,
    LoadJobConfig,
    SchemaField,
)
from google.cloud.bigquery.dbapi import Connection, DatabaseError
from google.cloud.bigquery.enums import SqlTypeNames, StandardSqlTypeNames
from google.cloud.bigquery.table import TableReference
from google.cloud.bigquery_storage_v1 import BigQueryReadClient
from google.oauth2 import service_account
from pydantic import PrivateAttr

from featurebyte.common.env_util import is_io_worker
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
    sql_to_string,
)
from featurebyte.session.base import (
    APPLICATION_NAME,
    INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    BaseSchemaInitializer,
    BaseSession,
    MetadataSchemaInitializer,
)

logger = get_logger(__name__)

logging.getLogger("connector").setLevel(logging.ERROR)

BIGQUERY_BATCH_FETCH_SIZE = 1000

db_vartype_mapping = {
    # Legacy types
    SqlTypeNames.INTEGER: DBVarType.INT,
    SqlTypeNames.INT64: DBVarType.INT,
    SqlTypeNames.BOOLEAN: DBVarType.BOOL,
    SqlTypeNames.BOOL: DBVarType.BOOL,
    SqlTypeNames.FLOAT: DBVarType.FLOAT,
    SqlTypeNames.FLOAT64: DBVarType.FLOAT,
    SqlTypeNames.STRING: DBVarType.VARCHAR,
    SqlTypeNames.BYTES: DBVarType.BINARY,
    SqlTypeNames.TIMESTAMP: DBVarType.TIMESTAMP,
    SqlTypeNames.DATETIME: DBVarType.TIMESTAMP,
    SqlTypeNames.DATE: DBVarType.DATE,
    SqlTypeNames.TIME: DBVarType.TIME,
    SqlTypeNames.NUMERIC: DBVarType.FLOAT,
    SqlTypeNames.BIGNUMERIC: DBVarType.FLOAT,
    SqlTypeNames.STRUCT: DBVarType.DICT,
    SqlTypeNames.RECORD: DBVarType.DICT,
    # Standard SQL types
    StandardSqlTypeNames.INTERVAL: DBVarType.TIMEDELTA,
    StandardSqlTypeNames.ARRAY: DBVarType.ARRAY,
    StandardSqlTypeNames.JSON: DBVarType.DICT,
    # Unsupported types
    SqlTypeNames.GEOGRAPHY: DBVarType.UNKNOWN,
    StandardSqlTypeNames.RANGE: DBVarType.UNKNOWN,
}

pa_type_mapping = {
    # Legacy types
    SqlTypeNames.INTEGER: pa.int64(),
    SqlTypeNames.INT64: pa.int64(),
    SqlTypeNames.BOOLEAN: pa.bool_(),
    SqlTypeNames.BOOL: pa.bool_(),
    SqlTypeNames.FLOAT: pa.float64(),
    SqlTypeNames.FLOAT64: pa.float64(),
    SqlTypeNames.STRING: pa.string(),
    SqlTypeNames.BYTES: pa.large_binary(),
    SqlTypeNames.TIMESTAMP: pa.timestamp("us", tz=None),
    SqlTypeNames.DATETIME: pa.timestamp("us", tz=None),
    SqlTypeNames.DATE: pa.date64(),
    SqlTypeNames.TIME: pa.time32("ms"),
    SqlTypeNames.NUMERIC: pa.decimal128(38, 18),
    SqlTypeNames.BIGNUMERIC: pa.decimal128(38, 18),
    SqlTypeNames.STRUCT: pa.string(),
    SqlTypeNames.RECORD: pa.string(),
    # Standard SQL types
    StandardSqlTypeNames.INTERVAL: pa.duration("ns"),
    StandardSqlTypeNames.ARRAY: pa.string(),
    StandardSqlTypeNames.JSON: pa.string(),
    # Unsupported types
    SqlTypeNames.GEOGRAPHY: DBVarType.UNKNOWN,
    StandardSqlTypeNames.RANGE: DBVarType.UNKNOWN,
}


def _json_serialization_handler(value: Any) -> str:
    """
    JSON serialization handler for non-serializable objects

    Parameters
    ----------
    value: Any
        Value to serialize

    Returns
    -------
    str
    """
    if isinstance(value, datetime.datetime):
        # convert to UTC and remove timezone info
        if value.tzinfo:
            return value.astimezone(tz.UTC).replace(tzinfo=None).isoformat()
        return value.isoformat()
    return str(value)


def get_pyarrow_type(
    datatype: str, precision: int = 0, scale: int = 0, mode: str | None = None
) -> pa.DataType:
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
    mode: str | None
        Mode

    Returns
    -------
    pa.DataType
    """
    if datatype == SqlTypeNames.INTERVAL:
        pyarrow_type = pa.int64()
    elif datatype in {SqlTypeNames.NUMERIC, SqlTypeNames.BIGNUMERIC}:
        _precision = precision if precision is not None else 38
        _scale = scale if scale is not None else 9
        if _scale > 0:
            pyarrow_type = pa.decimal128(_precision, _scale)
        else:
            pyarrow_type = pa.int64()
    elif mode == "REPEATED":
        pyarrow_type = pa.string()
    else:
        pyarrow_type = pa_type_mapping.get(datatype)

    if not pyarrow_type:
        # warn and fallback to string for unrecognized types
        logger.warning("Cannot infer pyarrow type", extra={"datatype": datatype})
        pyarrow_type = pa.string()
    return pyarrow_type


def convert_to_internal_variable_type(  # pylint: disable=too-many-return-statements
    bigquery_typecode: str, scale: int = 0, mode: str | None = None
) -> DBVarType:
    """
    Convert BigQuery data type to internal variable type

    Parameters
    ----------
    bigquery_typecode: str
        BigQuery data type
    scale: int
        Scale
    mode: str | None
        Mode

    Returns
    -------
    DBVarType
    """
    if bigquery_typecode in {SqlTypeNames.NUMERIC, SqlTypeNames.BIGNUMERIC}:
        if scale is None or scale > 0:
            return DBVarType.FLOAT
        return DBVarType.INT
    db_vartype = db_vartype_mapping.get(bigquery_typecode, DBVarType.UNKNOWN)
    if mode == "REPEATED":
        db_vartype = DBVarType.ARRAY
    elif db_vartype == DBVarType.UNKNOWN:
        logger.warning(f"BigQuery: Not supported data type '{bigquery_typecode}'")
    return db_vartype


def bq_to_arrow_schema(schema: list[SchemaField]) -> pa.Schema:
    """
    Convert BigQuery schema to pyarrow schema

    Parameters
    ----------
    schema: list[SchemaField]
        BigQuery schema

    Returns
    -------
    pa.Schema
    """
    fields = []
    for column in schema:
        field_name = column.name
        field_type = column.field_type
        db_var_type = convert_to_internal_variable_type(field_type, column.scale, column.mode)
        fields.append(
            pa.field(
                field_name,
                get_pyarrow_type(field_type, column.precision, column.scale, column.mode),
                metadata={ARROW_METADATA_DB_VAR_TYPE: db_var_type},
            )
        )
    return pa.schema(fields)


class BigQuerySession(BaseSession):
    """
    BigQuery session class
    """

    _no_schema_error = DatabaseError
    _client: Any = PrivateAttr(default=None)
    _location: Any = PrivateAttr(default=None)

    project_name: str
    dataset_name: str
    location: str = "US"
    source_type: SourceType = SourceType.BIGQUERY
    database_credential: GoogleCredential

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.database_name = self.project_name
        self.schema_name = self.dataset_name

    def _initialize_connection(self) -> None:
        try:
            assert isinstance(self.database_credential.service_account_info, dict)
            self._credentials = service_account.Credentials.from_service_account_info(
                self.database_credential.service_account_info
            )
            self._client = Client(
                project=self.project_name,
                location=self.location,
                credentials=self._credentials,
                client_info=ClientInfo(user_agent=APPLICATION_NAME),
            )
            connection_args: dict[str, Any]
            if is_io_worker():
                # Fetching result from cursor hangs in IO worker due to gevent if using BQ storage
                # client. Set prefer_bqstorage_client to False to avoid this issue.
                logger.info("Not using BigQueryReadClient for BigQuery session")
                connection_args = {
                    "prefer_bqstorage_client": False,
                }
            else:
                connection_args = {
                    "bqstorage_client": BigQueryReadClient(credentials=self._credentials),
                }
            self._connection = Connection(
                client=self._client,
                **connection_args,
            )
        except (MalformedError, DefaultCredentialsError) as exc:
            raise DataWarehouseConnectionError(str(exc)) from exc

    def initializer(self) -> BaseSchemaInitializer:
        return BigQuerySchemaInitializer(self)

    @classmethod
    def is_threadsafe(cls) -> bool:
        return True

    def get_additional_execute_query_kwargs(self) -> dict[str, Any]:
        job_config = bigquery.QueryJobConfig(
            default_dataset=f"{self.database_name}.{self.schema_name}"
        )
        return {"job_config": job_config}

    @property
    def dataset_ref(self) -> DatasetReference:
        return DatasetReference(project=self.project_name, dataset_id=self.dataset_name)

    def _execute_query(self, cursor: Any, query: str, **kwargs: Any) -> Any:
        job_id = str(uuid.uuid4())
        cursor.job_id = job_id
        return cursor.execute(query, job_id=job_id, **kwargs)

    @staticmethod
    def get_query_id(cursor: Any) -> str | None:
        return cursor.job_id if hasattr(cursor, "job_id") else None

    async def _cancel_query(self, cursor: Any, query: str) -> bool:
        try:
            self._client.cancel_job(
                cursor.job_id, project=self.project_name, location=self.location
            )
        except Forbidden:
            return False
        return True

    async def _list_databases(self) -> list[str]:
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

    async def _list_schemas(self, database_name: str | None = None) -> list[str]:
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

    async def _list_tables(
        self,
        database_name: str | None = None,
        schema_name: str | None = None,
        timeout: float = INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    ) -> list[TableSpec]:
        output = []
        next_page_token = None
        assert database_name is not None
        assert schema_name is not None
        while True:
            table_iterator = self._client.list_tables(
                dataset=DatasetReference(project=database_name, dataset_id=schema_name),
                page_token=next_page_token,
            )
            for table in table_iterator:
                output.append(TableSpec(name=table.table_id))
            next_page_token = table_iterator.next_page_token
            if not next_page_token:
                break
        return output

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
        rows = cursor._query_rows  # pylint: disable=protected-access
        if rows:
            schema = rows.schema
        else:
            schema = [
                SchemaField(
                    name=column.name,
                    field_type=column.type_code,
                    precision=column.precision,
                    scale=column.scale,
                    mode="NULLABLE" if column.null_ok else "REQUIRED",
                )
                for column in cursor.description
            ]

        return bq_to_arrow_schema(schema)

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

    @staticmethod
    def _format_value(value: Any) -> Any:
        """
        Format complex objects in record values from BigQuery

        Parameters
        ----------
        value: Any
            Value to format

        Returns
        -------
        Any
        """

        if isinstance(value, list):
            # NULL values are not supported in ARRAY columns, return empty array as None
            if not value:
                return None
            return json.dumps(value)
        if isinstance(value, dict):
            return json.dumps(value, default=_json_serialization_handler)
        return value

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
                table = pa.table(
                    list(
                        zip(*[
                            list(self._format_value(value) for value in i.values()) for i in records
                        ])
                    ),
                    schema=schema,
                )
                for batch in table.to_batches():
                    yield batch

    @staticmethod
    def get_columns_schema_from_dataframe(
        dataframe: pd.DataFrame,
    ) -> list[SchemaField]:
        """Get schema that can be used in CREATE TABLE statement from pandas DataFrame

        Parameters
        ----------
        dataframe : pd.DataFrame
            Input DataFrame

        Returns
        -------
        list[SchemaField]
        """

        def _detect_element_type(value: Any) -> SqlTypeNames:
            element_type = type(value)
            if element_type is int:
                return SqlTypeNames.INTEGER
            elif element_type is float:
                return SqlTypeNames.FLOAT64
            return SqlTypeNames.STRING

        def _detect_element_type_from_all(values: list[Any]) -> SqlTypeNames:
            element_type: Optional[SqlTypeNames] = None
            for value in values:
                element_type = _detect_element_type(value)
                if element_type == SqlTypeNames.STRING or element_type == SqlTypeNames.FLOAT64:
                    return element_type
            assert element_type is not None
            return element_type

        schema = []
        db_type: str
        for colname, dtype in dataframe.dtypes.to_dict().items():
            mode = "NULLABLE"
            if dataframe.shape[0] > 0 and isinstance(dataframe[colname].iloc[0], datetime.datetime):
                db_type = SqlTypeNames.TIMESTAMP
            elif dataframe.shape[0] > 0 and isinstance(dataframe[colname].iloc[0], datetime.date):
                db_type = SqlTypeNames.DATE
            elif pd.api.types.is_datetime64_any_dtype(dataframe[colname]):
                db_type = SqlTypeNames.TIMESTAMP
            elif pd.api.types.is_bool_dtype(dtype):
                db_type = SqlTypeNames.BOOLEAN
            elif pd.api.types.is_float_dtype(dtype):
                db_type = SqlTypeNames.FLOAT64
            elif pd.api.types.is_integer_dtype(dtype):
                db_type = SqlTypeNames.INTEGER
            elif (
                dataframe.shape[0] > 0
                and dataframe[colname].apply(lambda x: x is None or isinstance(x, list)).all()
            ):
                # Detect list element type
                non_empty_values = dataframe[colname][~pd.isnull(dataframe[colname])]
                all_values = []
                for arr in non_empty_values:
                    all_values.extend(arr)
                if non_empty_values.shape[0] > 0:
                    db_type = _detect_element_type_from_all(all_values)
                else:
                    db_type = SqlTypeNames.STRING
                mode = "REPEATED"
            elif (
                dataframe.shape[0] > 0
                and dataframe[colname].apply(lambda x: x is None or isinstance(x, dict)).all()
            ):
                db_type = "JSON"
            else:
                db_type = SqlTypeNames.STRING
            schema.append(
                SchemaField(name=colname, field_type=db_type, mode=mode),
            )
        return schema

    @staticmethod
    def _process_timestamp_value(ts_val: Any) -> str:
        ts_val = pd.Timestamp(ts_val)
        if ts_val.tzinfo is not None:
            ts_val = ts_val.tz_convert("UTC").tz_localize(None)
        return ts_val.isoformat()  # type: ignore[no-any-return]

    @staticmethod
    def _process_date_value(date_val: Any) -> str:
        date_val = pd.Timestamp(date_val).date()
        return date_val.isoformat()  # type: ignore[no-any-return]

    async def register_table(self, table_name: str, dataframe: pd.DataFrame) -> None:
        table_schema = self.get_columns_schema_from_dataframe(dataframe)
        types = {field.name: field.field_type for field in table_schema}
        # convert timestamps to timezone naive string, local timestamps are converted to UTC
        if dataframe.shape[0] > 0:
            dataframe = dataframe.copy()
            for colname in dataframe.columns:
                if types[colname] == SqlTypeNames.TIMESTAMP:
                    dataframe[colname] = dataframe[colname].apply(self._process_timestamp_value)
                elif types[colname] == SqlTypeNames.DATE:
                    dataframe[colname] = dataframe[colname].apply(self._process_date_value)

        table_ref = TableReference(
            dataset_ref=self.dataset_ref,
            table_id=table_name,
        )
        job_config = LoadJobConfig(
            schema=table_schema,
            write_disposition="WRITE_EMPTY",
        )

        # Load data into BigQuery table using JSON format
        # - Load via dataframe / parquet does not support JSON columns
        # - Load via CSV does not support ARRAY (REPEATED) columns
        job = self._client.load_table_from_json(
            json.loads(dataframe.to_json(orient="records")), table_ref, job_config=job_config
        )
        job.result()

    async def list_table_schema(
        self,
        table_name: str | None,
        database_name: str | None = None,
        schema_name: str | None = None,
        timeout: float = INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    ) -> OrderedDict[str, ColumnSpecWithDescription]:
        assert database_name is not None
        assert schema_name is not None
        assert table_name is not None
        table_ref = TableReference(
            dataset_ref=DatasetReference(project=database_name, dataset_id=schema_name),
            table_id=table_name,
        )
        table = self._client.get_table(table_ref)
        column_name_type_map = collections.OrderedDict()
        if table.schema is not None:
            for field in table.schema:
                dtype = convert_to_internal_variable_type(
                    bigquery_typecode=field.field_type,
                    scale=field.scale,
                    mode=field.mode,
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
        assert database_name is not None
        assert schema_name is not None

        # Use BiqQuery client to get table details instead of INFORMATION_SCHEMA tables
        # INFORMATION_SCHEMA access requires  dataset location to match client location which is not always the case
        table_ref = TableReference(
            dataset_ref=DatasetReference(project=database_name, dataset_id=schema_name),
            table_id=table_name,
        )

        try:
            table = self._client.get_table(table_ref)
        except NotFound:
            raise self.no_schema_error(f"Table {table_name} not found.")

        details = table.to_api_repr()
        table_description = details.pop("description", None)
        details.pop("schema", None)

        fully_qualified_table_name = get_fully_qualified_table_name({
            "table_name": table_name,
            "schema_name": schema_name,
            "database_name": database_name,
        })

        return TableDetails(
            details=details,
            fully_qualified_name=sql_to_string(
                fully_qualified_table_name, source_type=self.source_type
            ),
            description=table_description,
        )

    def _format_comment(self, comment: str) -> str:
        return self.sql_to_string(make_literal_value(comment))

    async def comment_table(self, table_name: str, comment: str) -> None:
        table_ref = TableReference(
            dataset_ref=self.dataset_ref,
            table_id=table_name,
        )
        table = self._client.get_table(table_ref)
        table.description = comment
        self._client.update_table(table, ["description"])

    async def comment_column(self, table_name: str, column_name: str, comment: str) -> None:
        formatted_column = self.format_quoted_identifier(column_name)
        query = f"ALTER TABLE {self.get_fully_qualified_table_name(table_name)} ALTER COLUMN {formatted_column} SET OPTIONS (description={self._format_comment(comment)})"
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
        return 4

    async def create_schema(self) -> None:
        create_schema_query = (
            f"CREATE SCHEMA `{self.session.database_name}`.`{self.session.schema_name}`"
        )
        await self.session.execute_query(create_schema_query)

    async def drop_object(self, object_type: str, name: str) -> None:
        query = f"DROP {object_type} {self._fully_qualified(name)}"
        await self.session.execute_query(query)

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
        output = []
        next_page_token = None
        assert isinstance(self.session, BigQuerySession)

        if object_type == "TABLES":
            while True:
                table_iterator = self.session._client.list_tables(  # pylint: disable=protected-access
                    dataset=DatasetReference(
                        project=self.session.project_name, dataset_id=self.session.dataset_name
                    ),
                    page_token=next_page_token,
                )
                for table in table_iterator:
                    output.append({"name": table.table_id})
                next_page_token = table_iterator.next_page_token
                if not next_page_token:
                    break

        elif object_type == "USER FUNCTIONS":
            while True:
                routine_iterator = self.session._client.list_routines(  # pylint: disable=protected-access
                    dataset=DatasetReference(
                        project=self.session.project_name, dataset_id=self.session.dataset_name
                    ),
                    page_token=next_page_token,
                )
                for routine in routine_iterator:
                    output.append({"name": routine.routine_id, "arguments": routine.arguments})
                next_page_token = routine_iterator.next_page_token
                if not next_page_token:
                    break
        return pd.DataFrame(output)

    @staticmethod
    def _format_arguments_to_be_droppable(arguments_list: list[str]) -> list[str]:
        return [arguments.split(" RETURN", 1)[0] for arguments in arguments_list]

    async def drop_all_objects_in_working_schema(self) -> None:
        if not await self.schema_exists():
            return

        functions = await self.list_objects("USER FUNCTIONS")
        if functions.shape[0]:
            for name in functions["name"].tolist():
                await self.drop_object("FUNCTION", name)

        for name in await self.list_droppable_tables_in_working_schema():
            try:
                await self.drop_object("TABLE", name)
            except DatabaseError:
                await self.drop_object("VIEW", name)

    def _fully_qualified(self, name: str) -> str:
        return f"{self._schema_qualifier}.`{name}`"


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
