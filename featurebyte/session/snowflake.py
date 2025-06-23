"""
SnowflakeSession class
"""

from __future__ import annotations

import collections
import datetime
import json
import logging
from typing import Annotated, Any, AsyncGenerator, OrderedDict, Union

import pandas as pd
import pyarrow as pa
from pydantic import Field
from snowflake import connector
from snowflake.connector.constants import FIELD_TYPES
from snowflake.connector.errors import Error as SnowflakeError
from snowflake.connector.errors import NotSupportedError, ProgrammingError
from snowflake.connector.pandas_tools import write_pandas

from featurebyte.common.utils import ARROW_METADATA_DB_VAR_TYPE
from featurebyte.enum import DBVarType, SourceType
from featurebyte.exception import CursorSchemaError, DataWarehouseConnectionError
from featurebyte.logging import get_logger
from featurebyte.models.credential import PrivateKeyCredential, UsernamePasswordCredential
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
    INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    BaseSchemaInitializer,
    BaseSession,
)
from featurebyte.session.enum import SnowflakeDataType

logger = get_logger(__name__)

logging.getLogger("snowflake.connector").setLevel(logging.ERROR)


db_vartype_mapping = {
    SnowflakeDataType.REAL: DBVarType.FLOAT,
    SnowflakeDataType.BINARY: DBVarType.BINARY,
    SnowflakeDataType.BOOLEAN: DBVarType.BOOL,
    SnowflakeDataType.DATE: DBVarType.DATE,
    SnowflakeDataType.TIME: DBVarType.TIME,
    SnowflakeDataType.ARRAY: DBVarType.ARRAY,
    SnowflakeDataType.OBJECT: DBVarType.DICT,
}

SnowflakeCredential = Annotated[
    Union[
        UsernamePasswordCredential,
        PrivateKeyCredential,
    ],
    Field(discriminator="type"),
]


class SnowflakeSession(BaseSession):
    """
    Snowflake session class


    References
    ----------
    Precision & scale:
    https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#impact-of-precision-and-scale-on-storage-size
    """

    _no_schema_error = ProgrammingError

    account: str
    warehouse: str
    database_name: str
    schema_name: str
    role_name: str
    source_type: SourceType = SourceType.SNOWFLAKE
    database_credential: SnowflakeCredential

    def _initialize_connection(self) -> None:
        try:
            connection_params = {
                "account": self.account,
                "warehouse": self.warehouse,
                "database": self.database_name,
                "schema": self.schema_name,
                "role_name": self.role_name,
                "application": APPLICATION_NAME,
                "client_session_keep_alive": True,
            }

            if isinstance(self.database_credential, PrivateKeyCredential):
                connection_params["user"] = self.database_credential.username
                connection_params["private_key"] = self.database_credential.pem_private_key
            else:
                assert isinstance(self.database_credential, UsernamePasswordCredential)
                connection_params["user"] = self.database_credential.username
                connection_params["password"] = self.database_credential.password

            self._connection = connector.connect(**connection_params)
        except SnowflakeError as exc:
            raise DataWarehouseConnectionError(exc.msg) from exc

        cursor = self._connection.cursor()
        cursor.execute(f'USE ROLE "{self.role_name}"')
        try:
            cursor.execute(f'USE SCHEMA "{self.database_name}"."{self.schema_name}"')
        except self._no_schema_error:
            # the schema may not exist yet if the feature store is being created
            pass
        # set timezone to UTC
        cursor.execute(
            "ALTER SESSION SET TIMEZONE='UTC', TIMESTAMP_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM'"
        )

    @staticmethod
    def get_query_id(cursor: Any) -> str | None:
        return str(cursor._request_id) if cursor._request_id else None

    async def _cancel_query(self, cursor: Any, query: str) -> bool:
        cursor._SnowflakeCursor__cancel_query(query)  # pylint: disable=protected-access
        return True

    def initializer(self) -> BaseSchemaInitializer:
        return SnowflakeSchemaInitializer(self)

    @classmethod
    def is_threadsafe(cls) -> bool:
        return True

    async def _list_databases(self) -> list[str]:
        """
        Execute SQL query to retrieve database names

        Returns
        -------
        list[str]
        """
        databases = await self.execute_query_interactive(
            "SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES"
        )
        output = []
        if databases is not None:
            output.extend(databases["DATABASE_NAME"].tolist())
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
        schemas = await self.execute_query_interactive(
            f'SELECT SCHEMA_NAME FROM "{database_name}".INFORMATION_SCHEMA.SCHEMATA'
        )
        output = []
        if schemas is not None:
            output.extend(schemas["SCHEMA_NAME"].tolist())
        return output

    async def _list_tables(
        self,
        database_name: str | None = None,
        schema_name: str | None = None,
        timeout: float = INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    ) -> list[TableSpec]:
        tables = await self.execute_query_interactive(
            f'SELECT TABLE_NAME, COMMENT FROM "{database_name}".INFORMATION_SCHEMA.TABLES '
            f"WHERE TABLE_SCHEMA = '{schema_name}'",
            timeout=timeout,
        )
        output = []
        if tables is not None:
            for _, (name, comment) in tables[["TABLE_NAME", "COMMENT"]].iterrows():
                output.append(TableSpec(name=name, description=comment or None))

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

        Raises
        ------
        CursorSchemaError
            When the cursor description is not as expected
        """
        fields = []
        description = cursor._description or []
        for field in description:
            if not hasattr(field, "type_code"):
                raise CursorSchemaError()
            field_type = FIELD_TYPES[field.type_code]
            if field_type.name == "FIXED" and field.scale and field.scale > 0:
                # DECIMAL type
                pa_type = pa.decimal128(field.precision, field.scale)
            else:
                pa_type = field_type.pa_type(field)

            # truncate nanoseconds to microseconds to support large date values (e.g. 9999-12-31)
            if pa_type == pa.timestamp("ns"):
                pa_type = pa.timestamp("us")

            db_var_type = self._convert_to_internal_variable_type({
                "type": field_type.name,
                "length": field.internal_size,
                "scale": field.scale,
            })
            fields.append(
                pa.field(field.name, pa_type, metadata={ARROW_METADATA_DB_VAR_TYPE: db_var_type})
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
        if cursor.description:
            try:
                result = cursor.fetch_pandas_all()
                return result
            except NotSupportedError:
                # fetch_pandas_all() raises NotSupportedError when: 1) The executed query does not
                # support it. Currently, only SELECT statements are supported; 2) pyarrow is not
                # available as a dependency.
                return super().fetch_query_result_impl(cursor)
        return None

    async def fetch_query_stream_impl(self, cursor: Any) -> AsyncGenerator[pa.RecordBatch, None]:
        # fetch results in batches and write to the stream
        schema = None
        try:
            schema = self._get_schema_from_cursor(cursor)
            for table in cursor.fetch_arrow_batches():
                for batch in table.cast(schema, safe=False).to_batches():
                    yield batch
            # return empty table to ensure correct schema is returned
            yield pa.record_batch(
                pd.DataFrame(columns=[field.name for field in schema]), schema=schema
            )
        except (NotSupportedError, CursorSchemaError):
            batches = super().fetch_query_stream_impl(cursor)
            async for batch in batches:
                if schema is None:
                    yield batch
                else:
                    yield pa.record_batch(batch.to_pandas(), schema=schema)

    async def register_table(self, table_name: str, dataframe: pd.DataFrame) -> None:
        schema = self.get_columns_schema_from_dataframe(dataframe)
        create_command = "CREATE OR REPLACE TABLE"
        await self.execute_query(
            f"""
            {create_command} "{table_name}"(
                {", ".join([f'"{colname}" {coltype}' for colname, coltype in schema])}
            )
            """
        )
        dataframe = self._prep_dataframe_before_write_pandas(dataframe, schema)
        write_pandas(self._connection, dataframe, table_name)

    @staticmethod
    def _convert_to_internal_variable_type(snowflake_var_info: dict[str, Any]) -> DBVarType:
        if snowflake_var_info["type"] in db_vartype_mapping:
            return db_vartype_mapping[snowflake_var_info["type"]]
        if snowflake_var_info["type"] == SnowflakeDataType.FIXED:
            # scale is defined as number of digits following the decimal point (see link in reference)
            return DBVarType.INT if snowflake_var_info["scale"] == 0 else DBVarType.FLOAT
        if snowflake_var_info["type"] == SnowflakeDataType.TEXT:
            return DBVarType.CHAR if snowflake_var_info["length"] == 1 else DBVarType.VARCHAR
        if snowflake_var_info["type"] in {
            SnowflakeDataType.TIMESTAMP_LTZ,
            SnowflakeDataType.TIMESTAMP_NTZ,
        }:
            return DBVarType.TIMESTAMP
        if snowflake_var_info["type"] in {
            SnowflakeDataType.TIMESTAMP_TZ,
        }:
            return DBVarType.TIMESTAMP_TZ
        logger.warning(f"Snowflake: Not supported data type '{snowflake_var_info}'")
        return DBVarType.UNKNOWN

    async def list_table_schema(
        self,
        table_name: str | None,
        database_name: str | None = None,
        schema_name: str | None = None,
        timeout: float = INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    ) -> OrderedDict[str, ColumnSpecWithDescription]:
        schema = await self.execute_query_interactive(
            f'SHOW COLUMNS IN "{database_name}"."{schema_name}"."{table_name}"',
            timeout=timeout,
        )
        column_name_type_map = collections.OrderedDict()
        if schema is not None:
            for _, (column_name, var_info, comment) in schema[
                ["column_name", "data_type", "comment"]
            ].iterrows():
                dtype = self._convert_to_internal_variable_type(json.loads(var_info))
                column_name_type_map[column_name] = ColumnSpecWithDescription(
                    name=column_name,
                    dtype=dtype,
                    description=comment or None,
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
        details_df = await self.execute_query_interactive(query)
        if details_df is None or details_df.shape[0] == 0:
            raise self.no_schema_error(f"Table {table_name} not found.")

        fully_qualified_table_name = get_fully_qualified_table_name({
            "table_name": table_name,
            "schema_name": schema_name,
            "database_name": database_name,
        })

        details = json.loads(details_df.iloc[0].to_json(orient="index"))
        return TableDetails(
            details=details,
            fully_qualified_name=sql_to_string(
                fully_qualified_table_name, source_type=self.source_type
            ),
            description=details.get("COMMENT"),
        )

    @staticmethod
    def get_columns_schema_from_dataframe(
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
            elif dataframe.shape[0] > 0 and isinstance(dataframe[colname].iloc[0], datetime.date):
                db_type = "DATE"
            elif pd.api.types.is_datetime64_any_dtype(dataframe[colname]):
                if isinstance(dataframe[colname], pd.DatetimeTZDtype):
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

    @staticmethod
    def _prep_dataframe_before_write_pandas(
        dataframe: pd.DataFrame, schema: list[tuple[str, str]]
    ) -> pd.DataFrame:
        # Ideally we should avoid making a copy, but so far the only way to get write_pandas() to
        # create DATETIME type columns in Snowflake for datetime columns in DataFrame is to specify
        # DATETIME type in the schema when creating table, and convert the dtype in DataFrame to
        # object before calling write_pandas(). A copy is made to prevent unintended side effects.
        dataframe = dataframe.copy()
        for colname, coltype in schema:
            if coltype in {"TIMESTAMP_NTZ", "TIMESTAMP_TZ"}:
                dataframe[colname] = (
                    dataframe[colname]
                    .astype(str)
                    .str.replace(r"(\+\d+):(\d+)", r" \1\2", regex=True)
                )
        return dataframe

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


class SnowflakeSchemaInitializer(BaseSchemaInitializer):
    """Snowflake schema initializer class"""

    @property
    def sql_directory_name(self) -> str:
        return "snowflake"

    @property
    def current_working_schema_version(self) -> int:
        return 35

    async def create_schema(self) -> None:
        create_schema_query = f'CREATE SCHEMA "{self.session.schema_name}"'
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
