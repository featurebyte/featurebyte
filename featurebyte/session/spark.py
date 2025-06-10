"""
SparkSession class
"""

from __future__ import annotations

import os
import subprocess
import tempfile
import time
from typing import Any, AsyncGenerator, Optional, Union, cast
from uuid import UUID

import pandas as pd
import pyarrow as pa
from pyarrow import ArrowTypeError, Schema
from pydantic import Field, PrivateAttr
from pyhive.exc import DatabaseError, OperationalError
from TCLIService import ttypes
from TCLIService.ttypes import TOperationState
from thrift.transport.TTransport import TTransportException
from typing_extensions import Annotated

from featurebyte.common.utils import ARROW_METADATA_DB_VAR_TYPE
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
    source_type: SourceType = SourceType.SPARK
    database_credential: Optional[SparkDatabaseCredential] = Field(default=None)
    storage_credential: Optional[StorageCredential] = Field(default=None)

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
    def get_query_id(cursor: Any) -> str | None:
        return (
            str(UUID(bytes=cursor._operationHandle.operationId.guid))
            if cursor._operationHandle
            else None
        )  # pylint: disable=protected-access

    async def _cancel_query(self, cursor: Any, query: str) -> bool:
        if cursor._operationHandle:  # pylint: disable=protected-access
            if self._connection:
                # cancel without waiting for response as the thrift_transport tend to be stuck on cancellation
                req = ttypes.TCancelOperationReq(
                    operationHandle=cursor._operationHandle,
                )
                self._connection.client.send_CancelOperation(req)
                # reset the connection so that a new thrift_transport is established to support subsequent queries
                self._initialize_connection()
                # unset the operation handle to avoid cursor.close() from calling the stale thrift_transport
                cursor._operationHandle = None  # pylint: disable=protected-access
            return True
        return False

    def _execute_query(self, cursor: Any, query: str, **kwargs: Any) -> Any:
        cursor.execute(query, async_=True)
        response = cursor.poll()
        while response.operationState in (
            TOperationState.INITIALIZED_STATE,
            TOperationState.RUNNING_STATE,
        ):
            time.sleep(0.1)
            response = cursor.poll()
        if response.operationState != TOperationState.FINISHED_STATE:
            guid = str(
                cursor._operationHandle and UUID(bytes=cursor._operationHandle.operationId.guid)
            )
            if response.operationState == TOperationState.ERROR_STATE:
                raise OperationalError(response.errorMessage, {"guid": guid})
            elif response.operationState == ttypes.TOperationState.CLOSED_STATE:
                raise DatabaseError("Command unexpectedly closed server side")
            logs = cursor.fetch_logs()
            messages = "\n".join(logs)
            raise OperationalError(
                f"Failed to execute query: {response.errorMessage or messages}", {"guid": guid}
            )

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

    def _get_schema_from_cursor(self, cursor: Any) -> Schema:
        """
        Get schema from a cursor

        Parameters
        ----------
        cursor: Any
            Cursor to fetch data from

        Returns
        -------
        Schema
        """
        fields = []
        for row in cursor.description:
            field_name = row[0]
            field_type = "_".join(row[1].split("_")[:-1])
            db_var_type = self._convert_to_internal_variable_type(field_type)
            fields.append(
                pa.field(
                    field_name,
                    self._get_pyarrow_type(field_type),
                    metadata={ARROW_METADATA_DB_VAR_TYPE: db_var_type},
                )
            )
        return pa.schema(fields)

    def fetch_query_result_impl(self, cursor: Any) -> pd.DataFrame | None:
        schema = self._get_schema_from_cursor(cursor)
        records = cursor.fetchall()
        if not records:
            return pa.record_batch([[]] * len(schema), schema=schema).to_pandas()
        return (
            pa.table(list(zip(*records)), names=[field.name for field in schema])
            .cast(schema, safe=False)
            .to_pandas()
        )

    async def fetch_query_stream_impl(self, cursor: Any) -> AsyncGenerator[pa.RecordBatch, None]:
        # fetch results in batches
        schema = self._get_schema_from_cursor(cursor)
        while True:
            try:
                records = cursor.fetchmany(1000)
                if not records:
                    # return empty table to ensure correct schema is returned
                    yield pa.record_batch([[]] * len(schema), schema=schema)
                    break
                # Transpose rows -> columns
                columns = list(zip(*records))
                table = pa.table(columns, names=[field.name for field in schema])
                for batch in table.cast(schema, safe=False).to_batches():
                    yield batch
            except TypeError as exc:
                if isinstance(exc, ArrowTypeError):
                    raise
                # TypeError is raised on DDL queries in Spark 3.4 and above
                break
