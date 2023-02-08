"""
SparkSession class
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from typing import Any, List, Optional, OrderedDict, cast

import collections
import os

import pandas as pd
import pyarrow as pa
from impala.dbapi import connect
from impala.error import OperationalError
from impala.hiveserver2 import HiveServer2Cursor
from pydantic import Field, PrivateAttr

from featurebyte.common.path_util import get_package_root
from featurebyte.enum import DBVarType, InternalName, SourceType, StorageType
from featurebyte.logger import logger
from featurebyte.models.credential import StorageCredentialType
from featurebyte.query_graph.sql.dataframe import construct_dataframe_sql_expr
from featurebyte.session.base import BaseSchemaInitializer, BaseSession, MetadataSchemaInitializer
from featurebyte.session.simple_storage import (
    FileMode,
    FileSimpleStorage,
    S3SimpleStorage,
    SimpleStorage,
)


class SparkSession(BaseSession):
    """
    Databricks session class
    """

    _no_schema_error = OperationalError
    _storage: SimpleStorage = PrivateAttr()

    host: str
    http_path: str
    port: int
    use_http_transport: bool
    access_token: Optional[str]
    storage_type: StorageType
    storage_url: str
    storage_spark_url: str
    storage_credential: Optional[StorageCredentialType]
    region_name: Optional[str]
    featurebyte_catalog: str
    featurebyte_schema: str
    source_type: SourceType = Field(SourceType.SPARK, const=True)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

        connect_params = {}
        auth_mechanism = "PLAIN"
        if self.access_token:
            auth_mechanism = "JWT"
            connect_params["jwt"] = self.access_token

        self._initialize_storage()
        self._connection = connect(
            host=data["host"],
            http_path=data["http_path"],
            database=f"{self.featurebyte_catalog}.{self.featurebyte_schema}",
            port=data["port"],
            use_http_transport=data["use_http_transport"],
            auth_mechanism=auth_mechanism,
            **connect_params,
        )

    def _initialize_storage(self) -> None:
        """
        Initialize storage object

        Raises
        ------
        NotImplementedError
            Storage type not supported
        """
        # add prefix to compartmentalize assets
        url_prefix = "/featurebyte"
        self.storage_url = self.storage_url.rstrip("/") + url_prefix
        self.storage_spark_url = self.storage_spark_url.rstrip("/") + url_prefix

        if self.storage_type == StorageType.LOCAL:
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
        # allow listing of schemas even if schema specified in connection does not exist
        default_db = self._connection.default_db
        self._connection.default_db = None
        try:
            schemas = await self.execute_query(f"SHOW SCHEMAS IN `{database_name}`")
            output = []
            if schemas is not None:
                output.extend(schemas["namespace"])
            return output
        finally:
            self._connection.default_db = default_db

    async def list_tables(
        self, database_name: str | None = None, schema_name: str | None = None
    ) -> list[str]:
        tables = await self.execute_query(f"SHOW TABLES IN `{database_name}`.`{schema_name}`")
        views = await self.execute_query(f"SHOW VIEWS IN `{database_name}`.`{schema_name}`")
        output = []
        if tables is not None:
            output.extend(tables["tableName"])
        if views is not None:
            output.extend(views["viewName"])
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
            "TINYINT": pa.int8(),
            "SMALLINT": pa.int16(),
            "INT": pa.int32(),
            "BIGINT": pa.int64(),
            "BINARY": pa.large_binary(),
            "BOOLEAN": pa.bool_(),
            "DATE": pa.date64(),
            "TIME": pa.time32("ms"),
            "DOUBLE": pa.float64(),
            "FLOAT": pa.float32(),
            "DECIMAL": pa.decimal256(38, 0),
            "INTERVAL": pa.duration("ns"),
            "VOID": pa.null(),
            "TIMESTAMP": pa.timestamp("ns", tz=None),
            # "ARRAY": pa.large_list(),
            # "MAP": pa.map_(),
            # "STRUCT": pa.struct(),
            "STRING": pa.string(),
        }
        if datatype.startswith("DECIMAL") and "(" in datatype:
            args = datatype.split("(")[1][:-1].split(",")
            pyarrow_type = pa.decimal256(int_precision=int(args[0]), int_scale=int(args[1]))
        else:
            pyarrow_type = mapping.get(datatype)

        if not pyarrow_type:
            # warn and fallback to string for unrecognized types
            logger.warning("Cannot infer pyarrow type", extra={"datatype": datatype})
            pyarrow_type = pa.string()
        return pyarrow_type

    @staticmethod
    def fetchall_arrow(cursor: HiveServer2Cursor) -> pa.Table:
        """
        Fetch all (remaining) rows of a query result, returning them as a PyArrow table.

        Parameters
        ----------
        cursor: HiveServer2Cursor
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

        results = cursor.fetchall()
        if not results:
            return pa.Table.from_arrays([tuple() for _ in range(len(schema))], schema=schema)
        return pa.Table.from_batches(
            [
                pa.record_batch(
                    [[item] for item in row],
                    schema=schema,
                )
                for row in results
            ]
        )

    def fetch_query_result_impl(self, cursor: Any) -> pd.DataFrame | None:
        arrow_table = self.fetchall_arrow(cursor)
        return arrow_table.to_pandas()

    async def register_table(
        self, table_name: str, dataframe: pd.DataFrame, temporary: bool = True
    ) -> None:
        date_cols = dataframe.select_dtypes(include=["datetime64"]).columns.tolist()
        table_expr = construct_dataframe_sql_expr(dataframe, date_cols).sql(
            pretty=True, dialect="spark"
        )
        if temporary:
            create_command = "CREATE OR REPLACE TEMPORARY VIEW"
        else:
            create_command = "CREATE OR REPLACE VIEW"
        query = f"{create_command} {table_name} AS {table_expr}"
        await self.execute_query(query)


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
        session = cast(SparkSession, self.session)
        return f"{session.storage_spark_url}/{udf_jar_file_name}"

    async def register_missing_objects(self) -> None:
        # upload jar file to storage
        session = cast(SparkSession, self.session)
        local_udf_jar_path = self.udf_jar_local_path
        udf_jar_file_name = os.path.basename(local_udf_jar_path)
        session.upload_file_to_storage(local_path=local_udf_jar_path, remote_path=udf_jar_file_name)
        await super().register_missing_objects()

    async def register_missing_functions(self, functions: list[dict[str, Any]]) -> None:
        await super().register_missing_functions(functions)
        # register UDF functions
        udf_functions = [
            ("OBJECT_AGG", "com.featurebyte.hive.udf.ObjectAggregate"),
            ("MODE", "com.featurebyte.hive.udf.Mode"),
            ("F_TIMESTAMP_TO_INDEX", "com.featurebyte.hive.udf.TimestampToIndex"),
            ("F_COUNT_DICT_ENTROPY", "com.featurebyte.hive.udf.CountDictEntropy"),
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
        session_conn = self.session._connection  # pylint: disable=protected-access
        default_db = session_conn.default_db
        session_conn.default_db = None
        try:
            create_schema_query = f"CREATE SCHEMA {self.session.schema_name}"
            await self.session.execute_query(create_schema_query)
        finally:
            session_conn.default_db = default_db

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
