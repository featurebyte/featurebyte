"""
BaseSparkSession class
"""

from __future__ import annotations

import collections
import os
from abc import ABC, abstractmethod
from typing import Any, DefaultDict, List, Optional, OrderedDict, Tuple, cast

import pandas as pd
import pyarrow as pa
from bson import ObjectId
from pydantic import Field

from featurebyte.common.path_util import get_package_root
from featurebyte.enum import DBVarType, InternalName
from featurebyte.logging import get_logger
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.dtype import PartitionMetadata
from featurebyte.query_graph.model.table import TableDetails, TableSpec
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import get_fully_qualified_table_name, sql_to_string
from featurebyte.session.base import (
    INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    BaseSchemaInitializer,
    BaseSession,
    MetadataSchemaInitializer,
)

logger = get_logger(__name__)

db_vartype_mapping = {
    "INT": DBVarType.INT,
    "BINARY": DBVarType.BINARY,
    "BOOLEAN": DBVarType.BOOL,
    "DATE": DBVarType.DATE,
    "DECIMAL": DBVarType.FLOAT,
    "DOUBLE": DBVarType.FLOAT,
    "FLOAT": DBVarType.FLOAT,
    "INTERVAL": DBVarType.TIMEDELTA,
    "VOID": DBVarType.VOID,
    "TIMESTAMP": DBVarType.TIMESTAMP,
    "TIMESTAMP_NTZ": DBVarType.TIMESTAMP,
    "MAP": DBVarType.DICT,
    "STRUCT": DBVarType.DICT,
    "STRING": DBVarType.VARCHAR,
}


pa_type_mapping = {
    "STRING": pa.string(),
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
    # https://spark.apache.org/docs/3.5.0/api/python/reference/pyspark.sql/api/pyspark.sql.types.DecimalType.html
    "DECIMAL": pa.float64(),  # this is used when scale information is not available
    "INTERVAL": pa.duration("ns"),
    "NULL": pa.null(),
    "TIMESTAMP": pa.timestamp("us", tz=None),
    "ARRAY": pa.string(),
    "MAP": pa.string(),
    "STRUCT": pa.string(),
}


class BaseSparkSession(BaseSession, ABC):
    """
    BaseSpark session class
    """

    host: str
    http_path: str
    storage_path: str
    catalog_name: str
    schema_name: str

    region_name: Optional[str] = Field(default=None)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.database_name = self.catalog_name
        self._initialize_storage()

    def initializer(self) -> BaseSchemaInitializer:
        return BaseSparkSchemaInitializer(self)

    @abstractmethod
    def _initialize_storage(self) -> None:
        """
        Initialize storage object

        Raises
        ------
        NotImplementedError
            Storage type not supported
        """

    @abstractmethod
    def test_storage_connection(self) -> None:
        """
        Test storage connection
        """

    @abstractmethod
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

    @abstractmethod
    def upload_dataframe_to_storage(self, dataframe: pd.DataFrame, remote_path: str) -> None:
        """
        Upload file to storage

        Parameters
        ----------
        dataframe: pd.DataFrame
            Dataframe
        remote_path: str
            Remote file path
        """

    @abstractmethod
    def delete_path_from_storage(self, remote_path: str) -> None:
        """
        Delete path from storage

        Parameters
        ----------
        remote_path: str
            Remote file path
        """

    @staticmethod
    def _get_pyarrow_type(datatype: str) -> pa.DataType:
        """
        Get pyarrow type from Spark data type

        Parameters
        ----------
        datatype: str
            Spark data type

        Returns
        -------
        pa.DataType
        """
        if datatype.startswith("INTERVAL"):
            pyarrow_type = pa.int64()
        elif datatype.startswith("DECIMAL("):
            # e.g. DECIMAL(10, 2)
            precision, scale = map(int, datatype[8:-1].split(","))
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

    @staticmethod
    def _convert_to_internal_variable_type(
        spark_type: str,
    ) -> DBVarType:
        if spark_type.endswith("INT"):
            # BIGINT, INT, SMALLINT, TINYINT
            return DBVarType.INT
        if spark_type.startswith("DECIMAL("):
            # DECIMAL(10, 2)
            _, scale = map(int, spark_type[8:-1].split(","))
            if scale > 0:
                return DBVarType.FLOAT
            return DBVarType.INT
        if spark_type.startswith("ARRAY"):
            # ARRAY<BIGINT>
            return DBVarType.ARRAY
        if spark_type.startswith("STRUCT"):
            return DBVarType.DICT
        if spark_type.startswith("MAP"):
            return DBVarType.DICT
        if spark_type.startswith("VARCHAR"):
            return DBVarType.VARCHAR
        db_vartype = db_vartype_mapping.get(spark_type, DBVarType.UNKNOWN)
        if db_vartype == DBVarType.UNKNOWN:
            logger.warning(f"Spark: Not supported data type '{spark_type}'")
        return db_vartype

    async def register_table_with_query(
        self, table_name: str, query: str, temporary: bool = True
    ) -> None:
        if temporary:
            create_command = "CREATE OR REPLACE TEMPORARY VIEW"
        else:
            create_command = "CREATE OR REPLACE VIEW"
        await self.execute_query_long_running(f"{create_command} `{table_name}` AS {query}")
        await self.execute_query_long_running(f"CACHE TABLE `{table_name}`")

    async def register_table(self, table_name: str, dataframe: pd.DataFrame) -> None:
        # truncate timestamps to microseconds to avoid parquet and Spark issues
        if dataframe.shape[0] > 0:
            for colname in dataframe.columns:
                if pd.api.types.is_datetime64_any_dtype(
                    dataframe[colname]
                ) or pd.api.types.is_datetime64tz_dtype(dataframe[colname]):
                    dataframe[colname] = dataframe[colname].dt.floor("us")

        # write to parquet file
        temp_filename = f"temp_{ObjectId()}.parquet"
        self.upload_dataframe_to_storage(dataframe=dataframe, remote_path=temp_filename)

        try:
            await self.execute_query(
                f"CREATE OR REPLACE TABLE `{table_name}` USING DELTA "
                f"TBLPROPERTIES('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5') "
                f"AS SELECT * FROM PARQUET.`{self.storage_path}/{temp_filename}`"
            )
        finally:
            # clean up staging file
            try:
                self.delete_path_from_storage(remote_path=temp_filename)
            except Exception as exc:
                logger.error(f"Exception while deleting temp file {temp_filename}: {exc}")

    async def _list_databases(self) -> list[str]:
        try:
            databases = await self.execute_query_interactive("SHOW CATALOGS")
        except self._no_schema_error as exc:
            if "ParseException" in str(exc):
                # Spark 3.2 and prior don't support SHOW CATALOGS
                return ["spark_catalog"]
            raise
        output = []
        if databases is not None:
            output.extend(databases["catalog"])
        return output

    async def _list_schemas(self, database_name: str | None = None) -> list[str]:
        try:
            schemas = await self.execute_query_interactive(f"SHOW SCHEMAS IN `{database_name}`")
        except self._no_schema_error as exc:
            if "ParseException" in str(exc):
                # Spark 3.2 and prior don't support SHOW SCHEMAS with the IN clause
                schemas = await self.execute_query_interactive("SHOW SCHEMAS")
            else:
                raise
        output = []
        if schemas is not None:
            output.extend(schemas.get("namespace", schemas.get("databaseName")))
            # in DataBricks the header is databaseName instead of namespace
        return output

    async def _list_tables(
        self,
        database_name: str | None = None,
        schema_name: str | None = None,
        timeout: float = INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    ) -> list[TableSpec]:
        tables = await self.execute_query_interactive(
            f"SHOW TABLES IN `{database_name}`.`{schema_name}`", timeout=timeout
        )
        output = []
        if tables is not None:
            for _, (name,) in tables[["tableName"]].iterrows():
                output.append(TableSpec(name=name))
        return output

    async def list_table_schema(
        self,
        table_name: str | None,
        database_name: str | None = None,
        schema_name: str | None = None,
        timeout: float = INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    ) -> OrderedDict[str, ColumnSpecWithDescription]:
        schema = await self.execute_query_interactive(
            f"DESCRIBE `{database_name}`.`{schema_name}`.`{table_name}`",
            timeout=timeout,
        )
        column_name_type_map = collections.OrderedDict()
        if schema is not None:
            metadata_section = None
            metadata_data: DefaultDict[str, List[Tuple[str, str, str]]] = collections.defaultdict(
                list
            )
            for _, (column_name, var_info, comment) in schema[
                ["col_name", "data_type", "comment"]
            ].iterrows():
                # Sometimes describe include metadata after column details with and empty row as a separator.
                if metadata_data:
                    # If we have metadata lines, we should continue to collect them
                    if column_name == "":
                        # If we have an empty column name, it indicates a new section
                        metadata_section = None
                        continue
                    if metadata_section is None:
                        metadata_section = column_name
                    metadata_data[metadata_section].append((column_name, var_info, comment))
                    continue

                # Skip the remaining entries once we run into an empty column name
                if column_name == "" or column_name.startswith("# "):
                    if column_name:
                        metadata_section = column_name
                        metadata_data[metadata_section].append((column_name, var_info, comment))
                    continue

                dtype = self._convert_to_internal_variable_type(var_info.upper())
                column_name_type_map[column_name] = ColumnSpecWithDescription(
                    name=column_name,
                    dtype=dtype,
                    description=comment or None,
                )

            # add partition key information
            partition_info = metadata_data.get("# Partition Information", None)
            if partition_info and len(partition_info) > 1:
                # If we have partition information, we should update the column name type map
                for column_name, var_info, _ in partition_info[2:]:
                    if column_name in column_name_type_map:
                        column_name_type_map[column_name].partition_metadata = PartitionMetadata(
                            is_partition_key=True,
                        )

        return column_name_type_map

    async def get_table_details(
        self,
        table_name: str,
        database_name: str | None = None,
        schema_name: str | None = None,
    ) -> TableDetails:
        schema = await self.execute_query_interactive(
            f"DESCRIBE EXTENDED `{database_name}`.`{schema_name}`.`{table_name}`"
        )
        table_details_found = False
        details = {}
        if schema is not None:
            for _, (column_name, var_info, _) in schema[
                ["col_name", "data_type", "comment"]
            ].iterrows():
                # Only collect details after the table details section (# Detailed Table Information)
                if column_name.startswith("# Detailed Table Information"):
                    table_details_found = True
                elif table_details_found:
                    if column_name == "" or column_name.startswith("# "):
                        break
                    details[column_name] = var_info

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
            description=details.get("Comment"),
        )

    def _format_comment(self, comment: str) -> str:
        return self.sql_to_string(make_literal_value(comment))

    async def comment_table(self, table_name: str, comment: str) -> None:
        formatted_table = self.format_quoted_identifier(table_name)
        query = f"COMMENT ON TABLE {formatted_table} IS {self._format_comment(comment)}"
        await self.execute_query(query)

    async def comment_column(self, table_name: str, column_name: str, comment: str) -> None:
        formatted_table = self.format_quoted_identifier(table_name)
        formatted_column = self.format_quoted_identifier(column_name)
        query = f"ALTER TABLE {formatted_table} ALTER COLUMN {formatted_column} COMMENT {self._format_comment(comment)}"
        await self.execute_query(query)


class BaseSparkMetadataSchemaInitializer(MetadataSchemaInitializer):
    """BaseSpark metadata initializer class"""

    def __init__(self, session: BaseSparkSession):
        super().__init__(session)

    async def metadata_table_exists(self) -> bool:
        """
        Check if metadata table exists

        Returns
        -------
        bool
        """
        try:
            await self.session.execute_query("SELECT * FROM METADATA_SCHEMA")
        except self.session._no_schema_error:
            return False
        return True

    async def create_metadata_table_if_not_exists(self, current_migration_version: int) -> None:
        """Create metadata table if it doesn't exist

        Parameters
        ----------
        current_migration_version: int
            Current migration version
        """
        if await self.metadata_table_exists():
            return
        for query in [
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
        ]:
            await self.session.execute_query(query)


class BaseSparkSchemaInitializer(BaseSchemaInitializer):
    """BaseSpark schema initializer class"""

    def __init__(self, session: BaseSparkSession):
        super().__init__(session=session)
        self.metadata_schema_initializer = BaseSparkMetadataSchemaInitializer(session)

    @property
    def current_working_schema_version(self) -> int:
        # NOTE: Please also update the version in hive-udf/lib/build.gradle
        return 15

    @property
    def sql_directory_name(self) -> str:
        return "spark"

    async def drop_all_objects_in_working_schema(self) -> None:
        if not await self.schema_exists():
            return

        for function in await self._list_functions():
            await self.drop_object("FUNCTION", function)

        for name in await self.list_droppable_tables_in_working_schema():
            await self.drop_object("TABLE", name)

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
        return f"{self.session.storage_path}/{udf_jar_file_name}"  # type: ignore[attr-defined]

    async def create_schema(self) -> None:
        create_schema_query = f"CREATE SCHEMA `{self.session.schema_name}`"
        await self.session.execute_query(create_schema_query)

    async def drop_object(self, object_type: str, name: str) -> None:
        query = f"DROP {object_type} {name}"
        await self.session.execute_query(query)

    async def list_objects(self, object_type: str) -> pd.DataFrame:
        query = f"SHOW {object_type}"
        return await self.session.execute_query(query)

    async def _list_functions(self) -> list[str]:
        def _function_name_to_identifier(function_name: str) -> Optional[str]:
            # function names returned from SHOW FUNCTIONS are three part fully qualified, but
            # identifiers are based on function names only
            parts = function_name.rsplit(".", 1)
            if len(parts) > 1:
                return parts[1]
            return None

        df_result = await self.list_objects("USER FUNCTIONS")
        out = []
        if df_result is not None:
            out.extend([
                function_name
                for function_name in df_result["function"].apply(_function_name_to_identifier)
                if function_name is not None
            ])
        return out

    def register_jar(self) -> None:
        """
        Register jar
        """
        # check storage connection is working
        session = cast(BaseSparkSession, self.session)
        session.test_storage_connection()

        # upload jar file to storage
        udf_jar_file_name = os.path.basename(self.udf_jar_local_path)
        session.upload_file_to_storage(
            local_path=self.udf_jar_local_path, remote_path=udf_jar_file_name
        )

    async def register_missing_objects(self) -> None:
        self.register_jar()
        await super().register_missing_objects()

    async def register_functions_from_jar(self) -> None:
        """
        Register functions from jar file
        """
        # Note that Spark does not seem to be able to reload the same class until the spark app is restarted.
        # To ensure functionality is updated for a function we should create a new class
        # and re-register the function with the new class
        udf_functions = [
            ("F_VECTOR_COSINE_SIMILARITY", "com.featurebyte.hive.udf.VectorCosineSimilarityV1"),
            ("VECTOR_AGGREGATE_MAX", "com.featurebyte.hive.udf.VectorAggregateMaxV1"),
            ("VECTOR_AGGREGATE_SUM", "com.featurebyte.hive.udf.VectorAggregateSumV1"),
            ("VECTOR_AGGREGATE_AVG", "com.featurebyte.hive.udf.VectorAggregateAverageV1"),
            (
                "VECTOR_AGGREGATE_SIMPLE_AVERAGE",
                "com.featurebyte.hive.udf.VectorAggregateSimpleAverageV1",
            ),
            ("OBJECT_DELETE", "com.featurebyte.hive.udf.ObjectDeleteV1"),
            ("F_TIMESTAMP_TO_INDEX", "com.featurebyte.hive.udf.TimestampToIndexV1"),
            ("F_INDEX_TO_TIMESTAMP", "com.featurebyte.hive.udf.IndexToTimestampV1"),
            (
                "F_COUNT_DICT_COSINE_SIMILARITY",
                "com.featurebyte.hive.udf.CountDictCosineSimilarityV2",
            ),
            ("F_COUNT_DICT_ENTROPY", "com.featurebyte.hive.udf.CountDictEntropyV3"),
            ("F_COUNT_DICT_MOST_FREQUENT", "com.featurebyte.hive.udf.CountDictMostFrequentV1"),
            (
                "F_COUNT_DICT_MOST_FREQUENT_VALUE",
                "com.featurebyte.hive.udf.CountDictMostFrequentValueV1",
            ),
            ("F_COUNT_DICT_LEAST_FREQUENT", "com.featurebyte.hive.udf.CountDictLeastFrequentV1"),
            ("F_COUNT_DICT_NUM_UNIQUE", "com.featurebyte.hive.udf.CountDictNumUniqueV1"),
            ("F_GET_RELATIVE_FREQUENCY", "com.featurebyte.hive.udf.CountDictRelativeFrequencyV1"),
            ("F_GET_RANK", "com.featurebyte.hive.udf.CountDictRankV1"),
            ("F_TIMEZONE_OFFSET_TO_SECOND", "com.featurebyte.hive.udf.TimezoneOffsetToSecondV1"),
        ]
        for function_name, class_name in udf_functions:
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
                        DROP FUNCTION IF EXISTS {function_name}
                        """
            )
            await self.session.execute_query(
                f"""
                        CREATE OR REPLACE FUNCTION {function_name} AS '{class_name}'
                        USING JAR '{self.udf_jar_spark_reference_path}';
                        """
            )

    async def register_missing_functions(self, functions: list[dict[str, Any]]) -> None:
        await super().register_missing_functions(functions)
        await self.register_functions_from_jar()
