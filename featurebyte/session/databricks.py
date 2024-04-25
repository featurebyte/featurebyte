"""
DatabricksSession class
"""

# pylint: disable=duplicate-code
from typing import Any, AsyncGenerator, BinaryIO, Optional

import os
from io import BytesIO

import pandas as pd
import pyarrow as pa
from bson import ObjectId
from pydantic import Field, PrivateAttr

from featurebyte import AccessTokenCredential, logging
from featurebyte.common.utils import ARROW_METADATA_DB_VAR_TYPE
from featurebyte.enum import SourceType
from featurebyte.session.base import APPLICATION_NAME
from featurebyte.session.base_spark import BaseSparkSession

try:
    from databricks import sql as databricks_sql
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.mixins.files import DbfsExt
    from databricks.sql.exc import ServerOperationError

    HAS_DATABRICKS_SQL_CONNECTOR = True
except ImportError:
    HAS_DATABRICKS_SQL_CONNECTOR = False


logger = logging.get_logger(__name__)


DATABRICKS_BATCH_FETCH_SIZE = 1000


class DatabricksSession(BaseSparkSession):
    """
    Databricks session class
    """

    _no_schema_error = ServerOperationError
    _storage_base_path: str = PrivateAttr()
    _dbfs_client: DbfsExt = PrivateAttr()

    source_type: SourceType = Field(SourceType.DATABRICKS, const=True)
    database_credential: AccessTokenCredential

    def _initialize_connection(self) -> None:
        if not HAS_DATABRICKS_SQL_CONNECTOR:
            raise RuntimeError("databricks-sql-connector is not available")

        self._connection = databricks_sql.connect(
            server_hostname=self.host,
            http_path=self.http_path,
            access_token=self.database_credential.access_token,
            catalog=self.catalog_name,
            schema=self.schema_name,
            _user_agent_entry=APPLICATION_NAME,
            _use_arrow_native_complex_types=False,
        )

    @property
    def _workspace_client(self) -> WorkspaceClient:
        # ensure google credentials not in environment variables to avoid conflict
        os.environ.pop("GOOGLE_CREDENTIALS", None)
        return WorkspaceClient(
            host=self.host,
            token=self.database_credential.access_token,
        )

    def _initialize_storage(self) -> None:
        self.storage_path = self.storage_path.rstrip("/")
        self._storage_base_path = self.storage_path.lstrip("dbfs:")
        self._dbfs_client = DbfsExt(self._workspace_client.api_client)

    def _upload_file_to_storage(self, path: str, src: BinaryIO) -> None:
        self._dbfs_client.upload(path=path, src=src, overwrite=True)

    def _delete_file_from_storage(self, path: str) -> None:
        self._dbfs_client.delete(path=path)

    def test_storage_connection(self) -> None:
        # test connectivity
        conn_test_filename = f"_conn_test_{ObjectId()}"
        path = f"{self._storage_base_path}/{conn_test_filename}"
        self._upload_file_to_storage(path, BytesIO(b"OK"))
        self._delete_file_from_storage(path)

    def upload_file_to_storage(
        self, local_path: str, remote_path: str, is_binary: bool = True
    ) -> None:
        logger.debug(
            "Upload file to storage",
            extra={"remote_path": remote_path, "is_binary": is_binary},
        )
        path = f"{self._storage_base_path}/{remote_path}"
        with open(local_path, mode="rb") as in_file_obj:
            self._upload_file_to_storage(path, in_file_obj)

    def upload_dataframe_to_storage(self, dataframe: pd.DataFrame, remote_path: str) -> None:
        path = f"{self._storage_base_path}/{remote_path}"
        with BytesIO() as buffer:
            dataframe.to_parquet(buffer, version="2.4")
            buffer.seek(0)
            self._upload_file_to_storage(path, buffer)

    def delete_path_from_storage(self, remote_path: str) -> None:
        path = f"{self._storage_base_path}/{remote_path}"
        self._delete_file_from_storage(path)

    @classmethod
    def is_threadsafe(cls) -> bool:
        return True

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
        fields = []
        for row in cursor.description:
            field_name = row[0]
            field_type = row[1].upper()
            db_var_type = self._convert_to_internal_variable_type(field_type)
            fields.append(
                pa.field(
                    field_name,
                    self._get_pyarrow_type(field_type),
                    metadata={ARROW_METADATA_DB_VAR_TYPE: db_var_type},
                )
            )
        return pa.schema(fields)

    def fetch_query_result_impl(self, cursor: Any) -> Optional[pd.DataFrame]:
        schema = None
        if cursor.description:
            schema = self._get_schema_from_cursor(cursor)

        if schema:
            return cursor.fetchall_arrow().cast(schema).to_pandas()

        return None

    async def fetch_query_stream_impl(self, cursor: Any) -> AsyncGenerator[pa.RecordBatch, None]:
        schema = None
        if cursor.description:
            schema = self._get_schema_from_cursor(cursor)

        if schema:
            # fetch results in batches
            while True:
                table = cursor.fetchmany_arrow(size=DATABRICKS_BATCH_FETCH_SIZE)
                if table.shape[0] == 0:
                    # return empty table to ensure correct schema is returned
                    yield pa.record_batch([[]] * len(schema), schema=schema)
                    break
                for batch in table.cast(schema).to_batches():
                    yield batch
