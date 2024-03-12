"""
SparkSession class
"""
# pylint: disable=duplicate-code
# pylint: disable=wrong-import-order
from __future__ import annotations

from typing import Any, AsyncGenerator, Optional, Union, cast
from typing_extensions import Annotated

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

from featurebyte.common.utils import literal_eval
from featurebyte.enum import SourceType, StorageType
from featurebyte.logging import get_logger
from featurebyte.models.credential import (
    AccessTokenCredential,
    AzureBlobStorageCredential,
    GCSStorageCredential,
    KerberosKeytabCredential,
    S3StorageCredential,
    StorageCredential,
)
from featurebyte.session.base_spark import BaseSparkSession
from featurebyte.session.hive import AuthType, HiveConnection
from featurebyte.session.simple_storage import (
    AzureBlobStorage,
    FileMode,
    FileSimpleStorage,
    GCSStorage,
    S3SimpleStorage,
    SimpleStorage,
    WebHDFSStorage,
)

logger = get_logger(__name__)


SparkDatabaseCredential = Annotated[
    Union[KerberosKeytabCredential, AccessTokenCredential],
    Field(discriminator="type"),
]


PYARROW_ARRAY_TYPE = pa.list_(pa.float64())


class SparkSession(BaseSparkSession):
    """
    Spark session class
    """

    _no_schema_error = OperationalError
    _connection: Optional[HiveConnection] = PrivateAttr(None)
    _storage: SimpleStorage = PrivateAttr()

    storage_type: StorageType
    storage_url: str
    port: int
    use_http_transport: bool
    use_ssl: bool
    source_type: SourceType = Field(SourceType.SPARK, const=True)
    database_credential: Optional[SparkDatabaseCredential]
    storage_credential: Optional[StorageCredential]

    def _initialize_connection(self) -> None:
        auth = None
        scheme = None
        access_token = None
        kerberos_service_name = None

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
                catalog=self.catalog_name,
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
        # add prefix to compartmentalize assets
        self.storage_url = self.storage_url.rstrip("/")
        self.storage_path = self.storage_path.rstrip("/")

        if self.storage_type == StorageType.FILE:
            self._storage = FileSimpleStorage(storage_url=self.storage_url)
        elif self.storage_type == StorageType.S3:
            if self.storage_credential is None:
                raise NotImplementedError("Storage credential is required for S3")
            if not isinstance(self.storage_credential, S3StorageCredential):
                raise NotImplementedError(
                    f"Unsupported storage credential for S3: {self.storage_credential.__class__.__name__}"
                )
            self._storage = S3SimpleStorage(
                storage_url=self.storage_url,
                storage_credential=self.storage_credential,
                region_name=self.region_name,
            )
        elif self.storage_type == StorageType.GCS:
            if self.storage_credential is None:
                raise NotImplementedError("Storage credential is required for GCS")
            if self.storage_credential is None or not isinstance(
                self.storage_credential, GCSStorageCredential
            ):
                raise NotImplementedError(
                    f"Unsupported storage credential for GCS: {self.storage_credential.__class__.__name__}"
                )
            self._storage = GCSStorage(
                storage_url=self.storage_url,
                storage_credential=self.storage_credential,
            )
        elif self.storage_type == StorageType.AZURE:
            if self.storage_credential is None:
                raise NotImplementedError("Storage credential is required for Azure Blob Storage")
            if self.storage_credential is None or not isinstance(
                self.storage_credential, AzureBlobStorageCredential
            ):
                raise NotImplementedError(
                    f"Unsupported storage credential for Azure Blob Storage: {self.storage_credential.__class__.__name__}"
                )
            self._storage = AzureBlobStorage(
                storage_url=self.storage_url,
                storage_credential=self.storage_credential,
            )
        elif self.storage_type == StorageType.WEBHDFS:
            self._storage = WebHDFSStorage(
                storage_url=self.storage_url,
                kerberos=(
                    self.database_credential is not None
                    and isinstance(self.database_credential, KerberosKeytabCredential)
                ),
            )
        else:
            raise NotImplementedError("Unsupported remote storage type")

    def test_storage_connection(self) -> None:
        # test connectivity
        self._storage.test_connection()

    def upload_file_to_storage(
        self, local_path: str, remote_path: str, is_binary: bool = True
    ) -> None:
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

    def upload_dataframe_to_storage(self, dataframe: pd.DataFrame, remote_path: str) -> None:
        with self._storage.open(path=remote_path, mode="wb") as out_file_obj:
            dataframe.to_parquet(out_file_obj, version="2.4")
            out_file_obj.flush()

    def delete_path_from_storage(self, remote_path: str) -> None:
        self._storage.delete_object(path=remote_path)

    @classmethod
    def is_threadsafe(cls) -> bool:
        return False

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
            "ARRAY_TYPE": PYARROW_ARRAY_TYPE,
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
        if data.empty:
            return data

        for i, column in enumerate(schema.names):
            current_type = schema.field(i).type
            # Convert decimal columns to float
            if current_type == pa.float64() and not is_float_dtype(data[column]):
                data[column] = data[column].astype(float)
            elif isinstance(current_type, pa.TimestampType) and not is_datetime64_dtype(
                data[column]
            ):
                data[column] = pd.to_datetime(data[column])
            elif current_type == PYARROW_ARRAY_TYPE:
                # Check if column is string. If so, convert to a list.
                is_string_series = data[column].apply(lambda x: isinstance(x, str))
                if is_string_series.any():
                    data[column] = data[column].apply(literal_eval)
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
        # Process data to update types of certain columns based on their schema type
        processed_data = self._process_batch_data(
            pd.DataFrame(results if results else None, columns=schema.names), schema
        )
        return pa.record_batch(processed_data, schema=schema)

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
            try:
                record_batch = self._read_batch(cursor, schema)
            except TypeError:
                # TypeError is raised on DDL queries in Spark 3.4 and above
                break
            yield record_batch
            if record_batch.num_rows == 0:
                break
