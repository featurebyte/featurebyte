"""
DatabricksSession class
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from typing import Any, List, Optional, OrderedDict, cast

import collections
import os

import pandas as pd
from botocore.exceptions import ClientError
from bson import ObjectId
from pydantic import Field, PrivateAttr, root_validator

from featurebyte import AccessTokenCredential
from featurebyte.common.path_util import get_package_root
from featurebyte.enum import DBVarType, InternalName, SourceType, StorageType
from featurebyte.logger import logger
from featurebyte.models.credential import StorageCredential
from featurebyte.session.base import BaseSchemaInitializer, BaseSession, MetadataSchemaInitializer
from featurebyte.session.simple_storage import (
    FileMode,
    FileSimpleStorage,
    S3SimpleStorage,
    SimpleStorage,
)

try:
    from databricks import sql as databricks_sql
    from databricks.sql.exc import ServerOperationError

    HAS_DATABRICKS_SQL_CONNECTOR = True
except ImportError:
    HAS_DATABRICKS_SQL_CONNECTOR = False


class DatabricksSession(BaseSession):
    """
    Databricks session class
    """

    _no_schema_error = ServerOperationError
    _storage: SimpleStorage = PrivateAttr()

    server_hostname: str
    http_path: str
    featurebyte_catalog: str
    featurebyte_schema: str
    source_type: SourceType = Field(SourceType.DATABRICKS, const=True)
    database_credential: AccessTokenCredential

    storage_type: StorageType
    storage_url: str
    storage_credential: Optional[StorageCredential]
    dbfs_dir: str

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

        if not HAS_DATABRICKS_SQL_CONNECTOR:
            raise RuntimeError("databricks-sql-connector is not available")

        self._initialize_storage()

        self._connection = databricks_sql.connect(
            server_hostname=data["server_hostname"],
            http_path=data["http_path"],
            access_token=self.database_credential.access_token,
            catalog=self.featurebyte_catalog,
            schema=self.featurebyte_schema,
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
        self.storage_url = self.storage_url.rstrip("/")

        if self.storage_type == StorageType.FILE:
            self._storage = FileSimpleStorage(storage_url=self.storage_url)
        elif self.storage_type == StorageType.S3:
            self._storage = S3SimpleStorage(
                storage_url=self.storage_url,
                storage_credential=self.storage_credential,
            )
        else:
            raise NotImplementedError("Unsupported remote storage type")

        # test connectivity
        self._storage.test_connection()

    @root_validator(pre=True)
    @classmethod
    def default_dbfs_dir(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Set default dbfs_dir

        Parameters
        ----------
        values: dict[str, Any]
            values dict from pydantic

        Returns
        -------
        values dict from pydantic
        """
        featurebyte_schema = values.get("featurebyte_schema")
        values["dbfs_dir"] = f"dbfs:/FileStore/{featurebyte_schema}"
        return values

    def initializer(self) -> BaseSchemaInitializer:
        return DatabricksSchemaInitializer(self)

    @property
    def schema_name(self) -> str:
        return self.featurebyte_schema

    @property
    def database_name(self) -> str:
        return self.featurebyte_catalog

    @classmethod
    def is_threadsafe(cls) -> bool:
        return True

    async def list_databases(self) -> list[str]:
        cursor = self._connection.cursor().catalogs()
        df_result = super().fetch_query_result_impl(cursor)
        out = []
        if df_result is not None:
            out.extend(df_result["TABLE_CAT"])
        return out

    async def list_schemas(self, database_name: str | None = None) -> list[str]:
        cursor = self._connection.cursor().schemas(catalog_name=database_name)
        df_result = self.fetch_query_result_impl(cursor)
        out = []
        if df_result is not None:
            out.extend(df_result["TABLE_SCHEM"])
        return out

    async def list_tables(
        self, database_name: str | None = None, schema_name: str | None = None
    ) -> list[str]:
        cursor = self._connection.cursor().tables(
            catalog_name=database_name, schema_name=schema_name
        )
        df_result = super().fetch_query_result_impl(cursor)
        out = []
        if df_result is not None:
            out.extend(df_result["TABLE_NAME"])
        return out

    async def list_table_schema(
        self,
        table_name: str | None,
        database_name: str | None = None,
        schema_name: str | None = None,
    ) -> OrderedDict[str, DBVarType]:
        cursor = self._connection.cursor().columns(
            table_name=table_name,
            catalog_name=database_name,
            schema_name=schema_name,
        )
        df_result = super().fetch_query_result_impl(cursor)
        column_name_type_map = collections.OrderedDict()
        if df_result is not None:
            for _, (column_name, databricks_type_name) in df_result[
                ["COLUMN_NAME", "TYPE_NAME"]
            ].iterrows():
                column_name_type_map[column_name] = self._convert_to_internal_variable_type(
                    databricks_type_name
                )
        return column_name_type_map

    @staticmethod
    def _convert_to_internal_variable_type(databricks_type: str) -> DBVarType:
        if databricks_type.endswith("INT"):
            # BIGINT, INT, SMALLINT, TINYINT
            return DBVarType.INT
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
        if databricks_type not in mapping:
            logger.warning(f"Databricks: Not supported data type '{databricks_type}'")
        return mapping.get(databricks_type, DBVarType.UNKNOWN)

    def fetch_query_result_impl(self, cursor: Any) -> pd.DataFrame | None:
        arrow_table = cursor.fetchall_arrow()
        return arrow_table.to_pandas()

    async def register_table(
        self, table_name: str, dataframe: pd.DataFrame, temporary: bool = True
    ) -> None:
        # dataframe.columns = dataframe.columns.str.replace(' ', '')

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
                    f"(path '{self.dbfs_dir}/{temp_filename}')"
                )
                # cache table so we can remove the temp file
                await self.execute_query(f"CACHE TABLE `{table_name}`")
            else:
                # register a permanent table from uncached temp view
                request_id = self.generate_session_unique_id()
                temp_view_name = f"__TEMP_TABLE_{request_id}"
                await self.execute_query(
                    f"CREATE OR REPLACE TEMPORARY VIEW `{temp_view_name}` USING parquet OPTIONS "
                    f"(path '{self.dbfs_dir}/{temp_filename}')"
                )

                await self.execute_query(
                    f"CREATE TABLE `{table_name}` USING DELTA "
                    f"TBLPROPERTIES('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5') "
                    f"AS SELECT * FROM `{temp_view_name}`"
                )
        finally:
            # clean up staging file
            try:
                self._storage.delete_object(path=temp_filename)
            except ClientError as exc:
                logger.error(f"Exception while deleting temp file {temp_filename}: {exc}")

    async def register_table_with_query(
        self, table_name: str, query: str, temporary: bool = True
    ) -> None:
        if temporary:
            create_command = "CREATE OR REPLACE TEMPORARY VIEW"
        else:
            create_command = "CREATE OR REPLACE VIEW"
        await self.execute_query(f"{create_command} `{table_name}` AS {query}")

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


class DatabricksMetadataSchemaInitializer(MetadataSchemaInitializer):
    """Databricks metadata initializer class"""

    def create_metadata_table_queries(self, current_migration_version: int) -> List[str]:
        """Queries to create metadata table

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
                "CREATE TABLE IF NOT EXISTS METADATA_SCHEMA ( "
                "WORKING_SCHEMA_VERSION INT, "
                f"{InternalName.MIGRATION_VERSION} INT, "
                "FEATURE_STORE_ID STRING, "
                "CREATED_AT TIMESTAMP "
                ");"
            ),
            (
                "INSERT INTO METADATA_SCHEMA "
                f"SELECT 0, {current_migration_version}, NULL, CURRENT_TIMESTAMP();"
            ),
        ]


class DatabricksSchemaInitializer(BaseSchemaInitializer):
    """Databricks schema initializer class"""

    def __init__(self, session: DatabricksSession):
        super().__init__(session=session)
        self.session: DatabricksSession = cast(DatabricksSession, self.session)  # type: ignore
        self.metadata_schema_initializer = DatabricksMetadataSchemaInitializer(session)

    async def drop_all_objects_in_working_schema(self) -> None:
        raise NotImplementedError()

    @property
    def sql_directory_name(self) -> str:
        return "databricks"

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
        return f"{self.session.dbfs_dir}/{udf_jar_file_name}"

    @property
    def current_working_schema_version(self) -> int:
        return 1

    async def create_schema(self) -> None:
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

    async def register_missing_objects(self) -> None:
        # upload jar file to storage
        udf_jar_file_name = os.path.basename(self.udf_jar_local_path)
        self.session.upload_file_to_storage(
            local_path=self.udf_jar_local_path, remote_path=udf_jar_file_name
        )
        await super().register_missing_objects()

        # remove jar file from storage after registering functions
        # self.session._storage.delete_object(  # pylint: disable=protected-access
        #     path=udf_jar_file_name
        # )

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
            ("F_GET_RELATIVE_FREQUENCY", "com.featurebyte.hive.udf.CountDictRelativeFrequency"),
            ("F_GET_RANK", "com.featurebyte.hive.udf.CountDictRank"),
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
