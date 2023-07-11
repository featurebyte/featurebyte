"""
SparkSession class
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from typing import Any, AsyncGenerator, Optional, OrderedDict, Union
from typing_extensions import Annotated

# pylint: disable=wrong-import-order
import collections
import os
import subprocess
import tempfile

import pandas as pd
import pyarrow as pa
from pandas.core.dtypes.common import is_datetime64_dtype, is_float_dtype
from pyarrow import Schema
from pydantic import Field, PrivateAttr
from pyhive.exc import OperationalError
from pyhive.hive import Cursor
from thrift.transport.TTransport import TTransportException

from featurebyte.enum import DBVarType, SourceType, StorageType
from featurebyte.logging import get_logger
from featurebyte.models.credential import AccessTokenCredential, KerberosKeytabCredential
from featurebyte.session.base_spark import BaseSparkSession
from featurebyte.session.hive import AuthType, HiveConnection
from featurebyte.session.simple_storage import WebHDFSStorage

logger = get_logger(__name__)


SparkDatabaseCredential = Annotated[
    Union[KerberosKeytabCredential, AccessTokenCredential],
    Field(discriminator="type"),
]


class SparkSession(BaseSparkSession):
    """
    Spark session class
    """

    _no_schema_error = OperationalError
    _connection: Optional[HiveConnection] = PrivateAttr(None)

    port: int
    use_http_transport: bool
    use_ssl: bool
    source_type: SourceType = Field(SourceType.SPARK, const=True)
    database_credential: Optional[SparkDatabaseCredential]

    def __init__(self, **data: Any) -> None:
        auth = None
        scheme = None
        access_token = None
        kerberos_service_name = None

        super().__init__(**data)

        if self.database_credential:
            if isinstance(self.database_credential, KerberosKeytabCredential):
                auth = AuthType.KERBEROS
                kerberos_service_name = "hive"
            elif isinstance(self.database_credential, AccessTokenCredential):
                auth = AuthType.TOKEN
                access_token = self.database_credential.access_token
            else:
                raise NotImplementedError(
                    f"Unsupported credential type: {self.database_credential.type}"
                )

        # determine transport scheme
        if self.use_http_transport:
            scheme = "https" if self.use_ssl else "http"

        def _create_connection() -> HiveConnection:
            return HiveConnection(
                host=self.host,
                http_path=self.http_path,
                catalog=self.database_name,
                database=self.schema_name,
                port=self.port,
                access_token=access_token,
                auth=auth,
                scheme=scheme,
                kerberos_service_name=kerberos_service_name,
            )

        # create a connection
        try:
            self._connection = _create_connection()
        except TTransportException:
            # expected to fail if kerberos ticket is expired
            if not isinstance(self.database_credential, KerberosKeytabCredential):
                raise
            # try kinit and create a connection again
            self._kinit(self.database_credential)
            self._connection = _create_connection()

        # Always use UTC for session timezone
        cursor = self._connection.cursor()
        cursor.execute("SET TIME ZONE 'UTC'")
        cursor.close()

    def __del__(self) -> None:
        if hasattr(self, "_connection") and self._connection:
            self._connection.close()

    @staticmethod
    def _kinit(credential: KerberosKeytabCredential) -> None:
        """
        Run kinit to get a kerberos ticket

        Parameters
        ----------
        credential : KerberosKeytabCredential
            Kerberos keytab credential

        Raises
        ------
        RuntimeError
            If kinit fails
        """
        # create a temporary keytab file to specify the KDC
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".keytab") as keytab_file:
            keytab_file.write(credential.keytab)
            keytab_file.flush()
            cmd = ["kinit", "-kt", keytab_file.name, credential.principal]
            process = subprocess.run(cmd, env=os.environ, check=False, stderr=subprocess.PIPE)
            if process.returncode != 0:
                raise RuntimeError(f"Failed to kinit: {process.stderr.decode('utf-8')}")

    def _initialize_storage(self) -> None:
        # support for webhdfs
        if self.storage_type == StorageType.WEBHDFS:
            self._storage = WebHDFSStorage(
                storage_url=self.storage_url,
                kerberos=(
                    self.database_credential is not None
                    and isinstance(self.database_credential, KerberosKeytabCredential)
                ),
            )
        else:
            super()._initialize_storage()

    @classmethod
    def is_threadsafe(cls) -> bool:
        return False

    async def list_databases(self) -> list[str]:
        try:
            databases = await self.execute_query("SHOW CATALOGS")
        except OperationalError as exc:
            if "ParseException" in str(exc):
                # Spark 3.2 and prior don't support SHOW CATALOGS
                return ["spark_catalog"]
            raise
        output = []
        if databases is not None:
            output.extend(databases["catalog"])
        return output

    async def list_schemas(self, database_name: str | None = None) -> list[str]:
        try:
            schemas = await self.execute_query(f"SHOW SCHEMAS IN `{database_name}`")
        except OperationalError as exc:
            if "ParseException" in str(exc):
                # Spark 3.2 and prior don't support SHOW SCHEMAS with the IN clause
                schemas = await self.execute_query("SHOW SCHEMAS")
            else:
                raise
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

    def _get_pyarrow_type(self, datatype: str) -> pa.types:
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

    def _process_batch_data(self, data: pd.DataFrame, schema: Schema) -> pd.DataFrame:
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

    def _read_batch(self, cursor: Cursor, schema: Schema, batch_size: int = 1000) -> pa.RecordBatch:
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
            self._process_batch_data(
                pd.DataFrame(results if results else None, columns=schema.names), schema
            ),
            schema=schema,
        )

    def fetchall_arrow(self, cursor: Cursor) -> pa.Table:
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
            {metadata[0]: self._get_pyarrow_type(metadata[1]) for metadata in cursor.description}
        )
        record_batches = []
        while True:
            record_batch = self._read_batch(cursor, schema)
            record_batches.append(record_batch)
            if record_batch.num_rows == 0:
                break
        return pa.Table.from_batches(record_batches)

    def fetch_query_result_impl(self, cursor: Any) -> pd.DataFrame | None:
        arrow_table = self.fetchall_arrow(cursor)
        return arrow_table.to_pandas()

    async def fetch_query_stream_impl(self, cursor: Any) -> AsyncGenerator[pa.RecordBatch, None]:
        # fetch results in batches
        schema = pa.schema(
            {metadata[0]: self._get_pyarrow_type(metadata[1]) for metadata in cursor.description}
        )
        while True:
            record_batch = self._read_batch(cursor, schema)
            yield record_batch
            if record_batch.num_rows == 0:
                break
