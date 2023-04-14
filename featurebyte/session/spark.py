"""
SparkSession class
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from typing import Any, List, Optional, OrderedDict, cast

import collections

import pandas as pd
import pyarrow as pa
from pandas.core.dtypes.common import is_datetime64_dtype, is_float_dtype
from pyarrow import Schema
from pydantic import Field
from pyhive.exc import OperationalError
from pyhive.hive import Cursor

from featurebyte.common.utils import create_new_arrow_stream_writer
from featurebyte.enum import DBVarType, InternalName, SourceType
from featurebyte.logger import logger
from featurebyte.session.base import BaseSchemaInitializer, MetadataSchemaInitializer
from featurebyte.session.hive import AuthType, HiveConnection
from featurebyte.session.spark_aware import BaseSparkSchemaInitializer, BaseSparkSession


class SparkSession(BaseSparkSession):
    """
    Spark session class
    """

    _no_schema_error = OperationalError

    port: int
    use_http_transport: bool
    use_ssl: bool
    access_token: Optional[str]
    source_type: SourceType = Field(SourceType.SPARK, const=True)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._initialize_storage()

        auth = None
        scheme = None

        # determine transport scheme
        if self.use_http_transport:
            scheme = "https" if self.use_ssl else "http"

        # determine auth mechanism
        if self.access_token:
            auth = AuthType.TOKEN

        self._connection = HiveConnection(
            host=self.host,
            http_path=self.http_path,
            catalog=self.database_name,
            database=self.schema_name,
            port=self.port,
            access_token=self.access_token,
            auth=auth,
            scheme=scheme,
        )
        # Always use UTC for session timezone
        cursor = self._connection.cursor()
        cursor.execute("SET TIME ZONE 'UTC'")
        cursor.close()

    def __del__(self) -> None:
        self._connection.close()

    def initializer(self) -> BaseSchemaInitializer:
        return SparkSchemaInitializer(self)

    @classmethod
    def is_threadsafe(cls) -> bool:
        return False

    async def list_databases(self) -> list[str]:
        databases = await self.execute_query("SHOW CATALOGS")
        output = []
        if databases is not None:
            output.extend(databases["catalog"])
        return output

    async def list_schemas(self, database_name: str | None = None) -> list[str]:
        schemas = await self.execute_query(f"SHOW SCHEMAS IN `{database_name}`")
        output = []
        if schemas is not None:
            output.extend(schemas.get("namespace", schemas.get("databaseName")))
            # in DataBricks the header is databaseName instead of namespace
        return output

    async def list_tables(
        self, database_name: str | None = None, schema_name: str | None = None
    ) -> list[str]:
        tables = await self.execute_query(f"SHOW TABLES IN `{database_name}`.`{schema_name}`")
        output = []
        if tables is not None:
            output.extend(tables["tableName"])
        return output

    async def list_table_schema(
        self,
        table_name: str | None,
        database_name: str | None = None,
        schema_name: str | None = None,
    ) -> OrderedDict[str, DBVarType]:
        schema = await self.execute_query(
            f"DESCRIBE `{database_name}`.`{schema_name}`.`{table_name}`"
        )
        column_name_type_map = collections.OrderedDict()
        if schema is not None:
            for _, (column_name, var_info) in schema[["col_name", "data_type"]].iterrows():
                # Sometimes describe include metadata after column details with and empty row as a separator.
                # Skip the remaining entries once we run into an empty column name
                if column_name == "":
                    break
                column_name_type_map[column_name] = self._convert_to_internal_variable_type(
                    var_info.upper()
                )
        return column_name_type_map

    @staticmethod
    def _get_pyarrow_type(datatype: str) -> pa.types:
        """
        Get pyarrow type from Spark data type

        Parameters
        ----------
        datatype: str
            Spark data type

        Returns
        -------
        pa.types
        """
        datatype = datatype.upper()
        mapping = {
            "STRING_TYPE": pa.string(),
            "TINYINT_TYPE": pa.int8(),
            "SMALLINT_TYPE": pa.int16(),
            "INT_TYPE": pa.int32(),
            "BIGINT_TYPE": pa.int64(),
            "BINARY_TYPE": pa.large_binary(),
            "BOOLEAN_TYPE": pa.bool_(),
            "DATE_TYPE": pa.timestamp("ns", tz=None),
            "TIME_TYPE": pa.time32("ms"),
            "DOUBLE_TYPE": pa.float64(),
            "FLOAT_TYPE": pa.float32(),
            "DECIMAL_TYPE": pa.float64(),
            "INTERVAL_TYPE": pa.duration("ns"),
            "NULL_TYPE": pa.null(),
            "TIMESTAMP_TYPE": pa.timestamp("ns", tz=None),
            "ARRAY_TYPE": pa.string(),
            "MAP_TYPE": pa.string(),
            "STRUCT_TYPE": pa.string(),
        }
        if datatype.startswith("INTERVAL"):
            pyarrow_type = pa.int64()
        else:
            pyarrow_type = mapping.get(datatype)

        if not pyarrow_type:
            # warn and fallback to string for unrecognized types
            logger.warning("Cannot infer pyarrow type", extra={"datatype": datatype})
            pyarrow_type = pa.string()
        return pyarrow_type

    @staticmethod
    def _process_batch_data(data: pd.DataFrame, schema: Schema) -> pd.DataFrame:
        """
        Process batch data before converting to PyArrow record batch.

        Parameters
        ----------
        data: pd.DataFrame
            Data to process
        schema: Schema
            Schema of the data

        Returns
        -------
        pd.DataFrame
            Processed data
        """
        for i, column in enumerate(schema.names):
            # Convert decimal columns to float
            if schema.field(i).type == pa.float64() and not is_float_dtype(data[column]):
                data[column] = data[column].astype(float)
            elif isinstance(schema.field(i).type, pa.TimestampType) and not is_datetime64_dtype(
                data[column]
            ):
                data[column] = pd.to_datetime(data[column])
        return data

    @staticmethod
    def _read_batch(cursor: Cursor, schema: Schema, batch_size: int = 1000) -> pa.RecordBatch:
        """
        Fetch a batch of rows from a query result, returning them as a PyArrow record batch.

        Parameters
        ----------
        cursor: Cursor
            Cursor to fetch data from
        schema: Schema
            Schema of the data to fetch
        batch_size: int
            Number of rows to fetch at a time

        Returns
        -------
        pa.RecordBatch
            None if no more rows are available
        """
        results = cursor.fetchmany(batch_size)
        return pa.record_batch(
            SparkSession._process_batch_data(
                pd.DataFrame(results if results else None, columns=schema.names), schema
            ),
            schema=schema,
        )

    @staticmethod
    def fetchall_arrow(cursor: Cursor) -> pa.Table:
        """
        Fetch all (remaining) rows of a query result, returning them as a PyArrow table.

        Parameters
        ----------
        cursor: Cursor
            Cursor to fetch data from

        Returns
        -------
        pa.Table
        """
        schema = pa.schema(
            {
                metadata[0]: SparkSession._get_pyarrow_type(metadata[1])
                for metadata in cursor.description
            }
        )
        record_batches = []
        while True:
            record_batch = SparkSession._read_batch(cursor, schema)
            record_batches.append(record_batch)
            if record_batch.num_rows == 0:
                break
        return pa.Table.from_batches(record_batches)

    def fetch_query_result_impl(self, cursor: Any) -> pd.DataFrame | None:
        arrow_table = self.fetchall_arrow(cursor)
        return arrow_table.to_pandas()

    def fetch_query_stream_impl(self, cursor: Any, output_pipe: Any) -> None:
        # fetch results in batches and write to the stream
        schema = pa.schema(
            {
                metadata[0]: SparkSession._get_pyarrow_type(metadata[1])
                for metadata in cursor.description
            }
        )
        writer = create_new_arrow_stream_writer(output_pipe, schema)
        while True:
            record_batch = SparkSession._read_batch(cursor, schema)
            writer.write_batch(record_batch)
            if record_batch.num_rows == 0:
                break
        writer.close()


class SparkMetadataSchemaInitializer(MetadataSchemaInitializer):
    """Spark metadata initializer class"""

    def create_metadata_table_queries(self, current_migration_version: int) -> List[str]:
        """Query to create metadata table

        Parameters
        ----------
        current_migration_version: int
            Current migration version

        Returns
        -------
        List[str]
        """
        return [
            (
                f"""
                CREATE TABLE IF NOT EXISTS METADATA_SCHEMA (
                    WORKING_SCHEMA_VERSION INT,
                    {InternalName.MIGRATION_VERSION} INT,
                    FEATURE_STORE_ID STRING,
                    CREATED_AT TIMESTAMP
                )  USING DELTA
                """
            ),
            (
                f"""
                INSERT INTO METADATA_SCHEMA
                SELECT
                    0 AS WORKING_SCHEMA_VERSION,
                    {current_migration_version} AS {InternalName.MIGRATION_VERSION},
                    NULL AS FEATURE_STORE_ID,
                    CURRENT_TIMESTAMP() AS CREATED_AT
                """
            ),
        ]


class SparkSchemaInitializer(BaseSparkSchemaInitializer):
    """Spark schema initializer class"""

    def __init__(self, session: SparkSession):
        super().__init__(session=session)
        self.session = cast(SparkSession, self.session)
        self.metadata_schema_initializer = SparkMetadataSchemaInitializer(session)

    @property
    def sql_directory_name(self) -> str:
        return "spark"

    @property
    def current_working_schema_version(self) -> int:
        return 1
