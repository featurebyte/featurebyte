"""
SparkSession class
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from typing import Any, List, Optional, OrderedDict, cast

import collections
import decimal
import os

import pandas as pd
import pyarrow as pa
from bson import ObjectId
from pandas import Series
from pyarrow import Schema
from pydantic import Field, PrivateAttr
from pyhive.exc import OperationalError
from pyhive.hive import Cursor

from featurebyte.common.path_util import get_package_root
from featurebyte.common.utils import create_new_arrow_stream_writer
from featurebyte.enum import DBVarType, InternalName, SourceType, StorageType
from featurebyte.logger import logger
from featurebyte.models.credential import (
    BaseCredential,
    BaseStorageCredential,
    StorageCredentialType,
)
from featurebyte.session.base import BaseSchemaInitializer, BaseSession, MetadataSchemaInitializer
from featurebyte.session.hive import AuthType, HiveConnection
from featurebyte.session.simple_storage import (
    FileMode,
    FileSimpleStorage,
    S3SimpleStorage,
    SimpleStorage,
)


class SparkSession(BaseSession):
    """
    Spark session class
    """

    _no_schema_error = OperationalError
    _storage: SimpleStorage = PrivateAttr()

    host: str
    http_path: str
    port: int
    use_http_transport: bool
    use_ssl: bool
    access_token: Optional[str]
    storage_type: StorageType
    storage_url: str
    storage_spark_url: str
    storage_credential_type: Optional[StorageCredentialType]
    storage_credential: Optional[BaseStorageCredential]
    region_name: Optional[str]
    featurebyte_catalog: str
    featurebyte_schema: str
    source_type: SourceType = Field(SourceType.SPARK, const=True)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.storage_credential = BaseCredential(**data).storage_credential
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

    def _initialize_storage(self) -> None:
        """
        Initialize storage object

        Raises
        ------
        NotImplementedError
            Storage type not supported
        """
        # add prefix to compartmentalize assets
        self.storage_url = self.storage_url.rstrip("/")
        self.storage_spark_url = self.storage_spark_url.rstrip("/")

        if self.storage_type == StorageType.FILE:
            self._storage = FileSimpleStorage(storage_url=self.storage_url)
        elif self.storage_type == StorageType.S3:
            self._storage = S3SimpleStorage(
                storage_url=self.storage_url,
                storage_credential=self.storage_credential,
                region_name=self.region_name,
            )
        else:
            raise NotImplementedError("Unsupported remote storage type")

        # test connectivity
        self._storage.test_connection()

    def upload_file_to_storage(
        self, local_path: str, remote_path: str, is_binary: bool = True
    ) -> None:
        """
        Upload file to storage

        Parameters
        ----------
        local_path: str
            Local file path
        remote_path: str
            Remote file path
        is_binary: bool
            Upload as binary
        """
        read_mode = cast(FileMode, "rb" if is_binary else "r")
        write_mode = cast(FileMode, "wb" if is_binary else "w")
        logger.debug(
            "Upload file to storage",
            extra={"remote_path": remote_path, "is_binary": is_binary},
        )
        with open(local_path, mode=read_mode) as in_file_obj:
            with self._storage.open(
                path=remote_path,
                mode=write_mode,
            ) as out_file_obj:
                out_file_obj.write(in_file_obj.read())

    def initializer(self) -> BaseSchemaInitializer:
        return SparkSchemaInitializer(self)

    @property
    def schema_name(self) -> str:
        return self.featurebyte_schema

    @property
    def database_name(self) -> str:
        return self.featurebyte_catalog

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
    def _convert_to_internal_variable_type(spark_type: str) -> DBVarType:
        if spark_type.endswith("INT"):
            # BIGINT, INT, SMALLINT, TINYINT
            return DBVarType.INT
        if spark_type.startswith("DECIMAL"):
            # DECIMAL(10, 2)
            return DBVarType.FLOAT

        mapping = {
            "BINARY": DBVarType.BINARY,
            "BOOLEAN": DBVarType.BOOL,
            "DATE": DBVarType.DATE,
            "DECIMAL": DBVarType.FLOAT,
            "DOUBLE": DBVarType.FLOAT,
            "FLOAT": DBVarType.FLOAT,
            "INTERVAL": DBVarType.TIMEDELTA,
            "VOID": DBVarType.VOID,
            "TIMESTAMP": DBVarType.TIMESTAMP,
            "ARRAY": DBVarType.ARRAY,
            "MAP": DBVarType.MAP,
            "STRUCT": DBVarType.STRUCT,
            "STRING": DBVarType.VARCHAR,
        }
        if spark_type not in mapping:
            logger.warning(f"Spark: Not supported data type '{spark_type}'")
        return mapping.get(spark_type, DBVarType.UNKNOWN)

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
            "DATE_TYPE": pa.string(),
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
    def _convert_decimal_to_float(row: Any) -> Any:
        yield from (float(item) if isinstance(item, decimal.Decimal) else item for item in row)

    @staticmethod
    def _read_batch(cursor: Cursor, schema: Schema, batch_size=1000) -> pa.RecordBatch:
        """

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
        data = None
        if results:
            data = [SparkSession._convert_decimal_to_float(row) for row in results]
        return pa.record_batch(pd.DataFrame(data, columns=schema.names), schema=schema)

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

    async def register_table(
        self, table_name: str, dataframe: pd.DataFrame, temporary: bool = True
    ) -> None:
        # truncate timestamps to microseconds to avoid parquet and Spark issues
        if dataframe.shape[0] > 0:
            for colname in dataframe.columns:
                if pd.api.types.is_datetime64_any_dtype(
                    dataframe[colname]
                ) or pd.api.types.is_datetime64tz_dtype(dataframe[colname]):
                    dataframe[colname] = dataframe[colname].dt.floor("us")

        # write to parquet file
        temp_filename = f"temp_{ObjectId()}.parquet"
        with self._storage.open(path=temp_filename, mode="wb") as out_file_obj:
            dataframe.to_parquet(out_file_obj)
            out_file_obj.flush()

        try:
            if temporary:
                # create cached temp view
                await self.execute_query(
                    f"CREATE OR REPLACE TEMPORARY VIEW `{table_name}` USING parquet OPTIONS "
                    f"(path '{self.storage_spark_url}/{temp_filename}')"
                )
                # cache table so we can remove the temp file
                await self.execute_query(f"CACHE TABLE `{table_name}`")
            else:
                # register a permanent table from uncached temp view
                request_id = self.generate_session_unique_id()
                temp_view_name = f"__TEMP_TABLE_{request_id}"
                await self.execute_query(
                    f"CREATE OR REPLACE TEMPORARY VIEW `{temp_view_name}` USING parquet OPTIONS "
                    f"(path '{self.storage_spark_url}/{temp_filename}')"
                )
                await self.execute_query(
                    f"CREATE TABLE `{table_name}` AS SELECT * FROM `{temp_view_name}`"
                )
        finally:
            # clean up staging file
            self._storage.delete_object(path=temp_filename)

    async def register_table_with_query(
        self, table_name: str, query: str, temporary: bool = True
    ) -> None:
        if temporary:
            create_command = "CREATE OR REPLACE TEMPORARY VIEW"
        else:
            create_command = "CREATE OR REPLACE VIEW"
        await self.execute_query(f"{create_command} `{table_name}` AS {query}")


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


class SparkSchemaInitializer(BaseSchemaInitializer):
    """Spark schema initializer class"""

    def __init__(self, session: SparkSession):
        super().__init__(session=session)
        self.spark_session = cast(SparkSession, self.session)
        self.metadata_schema_initializer = SparkMetadataSchemaInitializer(session)

    async def drop_all_objects_in_working_schema(self) -> None:
        raise NotImplementedError()

    @property
    def sql_directory_name(self) -> str:
        return "spark"

    @property
    def udf_jar_local_path(self) -> str:
        """
        Get path of udf jar file

        Returns
        -------
        str

        Raises
        ------
        FileNotFoundError
            Spark hive udf jar not found
        """
        sql_directory = os.path.join(get_package_root(), "sql", self.sql_directory_name)
        for filename in os.listdir(sql_directory):
            if filename.endswith(".jar"):
                return os.path.join(sql_directory, filename)
        raise FileNotFoundError("Spark hive udf jar not found")

    @property
    def udf_jar_spark_reference_path(self) -> str:
        """
        Path to reference in Spark SQL for the remote jar file

        Returns
        -------
        str
        """
        udf_jar_file_name = os.path.basename(self.udf_jar_local_path)
        return f"{self.spark_session.storage_spark_url}/{udf_jar_file_name}"

    async def register_missing_objects(self) -> None:
        # upload jar file to storage
        local_udf_jar_path = self.udf_jar_local_path
        udf_jar_file_name = os.path.basename(local_udf_jar_path)
        self.spark_session.upload_file_to_storage(
            local_path=local_udf_jar_path, remote_path=udf_jar_file_name
        )
        await super().register_missing_objects()

    async def register_missing_functions(self, functions: list[dict[str, Any]]) -> None:
        await super().register_missing_functions(functions)
        # Note that Spark does not seem to be able to reload the same class until the spark app is restarted.
        # To ensure functionality is updated for a function we should create a new class
        # and re-register the function with the new class
        udf_functions = [
            ("OBJECT_AGG", "com.featurebyte.hive.udf.ObjectAggregate"),
            ("OBJECT_DELETE", "com.featurebyte.hive.udf.ObjectDelete"),
            ("F_TIMESTAMP_TO_INDEX", "com.featurebyte.hive.udf.TimestampToIndex"),
            (
                "F_COUNT_DICT_COSINE_SIMILARITY",
                "com.featurebyte.hive.udf.CountDictCosineSimilarity",
            ),
            ("F_COUNT_DICT_ENTROPY", "com.featurebyte.hive.udf.CountDictEntropy"),
            ("F_COUNT_DICT_MOST_FREQUENT", "com.featurebyte.hive.udf.CountDictMostFrequent"),
            (
                "F_COUNT_DICT_MOST_FREQUENT_VALUE",
                "com.featurebyte.hive.udf.CountDictMostFrequentValue",
            ),
            ("F_COUNT_DICT_NUM_UNIQUE", "com.featurebyte.hive.udf.CountDictNumUnique"),
        ]
        for (function_name, class_name) in udf_functions:
            logger.debug(
                "Register UDF",
                extra={
                    "function_name": function_name,
                    "class_name": class_name,
                    "jar_path": self.udf_jar_spark_reference_path,
                },
            )
            await self.session.execute_query(
                f"""
                CREATE OR REPLACE FUNCTION {function_name} AS '{class_name}'
                USING JAR '{self.udf_jar_spark_reference_path}';
                """
            )

    @property
    def current_working_schema_version(self) -> int:
        return 1

    async def create_schema(self) -> None:
        # allow creation of schema even if schema specified in connection does not exist
        create_schema_query = f"CREATE SCHEMA {self.session.schema_name}"
        await self.session.execute_query(create_schema_query)

    async def list_functions(self) -> list[str]:
        def _function_name_to_identifier(function_name: str) -> str:
            # function names returned from SHOW FUNCTIONS are three part fully qualified, but
            # identifiers are based on function names only
            return function_name.rsplit(".", 1)[1]

        df_result = await self.session.execute_query(
            f"SHOW USER FUNCTIONS IN {self.session.schema_name}"
        )
        out = []
        if df_result is not None:
            out.extend(df_result["function"].apply(_function_name_to_identifier))
        return out

    async def list_procedures(self) -> list[str]:
        return []
