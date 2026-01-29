"""
DatabricksSession class
"""

from __future__ import annotations

import contextlib
import os
import time
from functools import cached_property
from io import BytesIO
from typing import Any, AsyncGenerator, BinaryIO, Iterator, Optional, OrderedDict, Union
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
from bson import ObjectId
from databricks.sdk.core import Config, oauth_service_principal
from databricks.sdk.errors import NotFound
from databricks.sql.thrift_api.TCLIService.ttypes import TOperationHandle
from pydantic import PrivateAttr
from TCLIService import ttypes

from featurebyte import logging
from featurebyte.common.utils import ARROW_METADATA_DB_VAR_TYPE
from featurebyte.enum import SourceType
from featurebyte.models.credential import AccessTokenCredential, OAuthCredential
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.dtype import PartitionMetadata
from featurebyte.query_graph.model.table import TableSpec
from featurebyte.session.base import APPLICATION_NAME, INTERACTIVE_QUERY_TIMEOUT_SECONDS
from featurebyte.session.base_spark import BaseSparkSession

try:
    from databricks import sql as databricks_sql
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.mixins.files import DbfsExt
    from databricks.sql.exc import DatabaseError, ServerOperationError
    from databricks.sql.thrift_backend import ThriftBackend as BaseThriftBackend

    HAS_DATABRICKS_SQL_CONNECTOR = True
except ImportError:
    HAS_DATABRICKS_SQL_CONNECTOR = False


logger = logging.get_logger(__name__)


DATABRICKS_BATCH_FETCH_SIZE = 1000


class ThriftBackend(BaseThriftBackend):
    def _poll_for_status(self, op_handle: TOperationHandle) -> ttypes.TGetOperationStatusResp:
        # introduce a sleep between polls to make query execution cancellable
        req = ttypes.TGetOperationStatusReq(
            operationHandle=op_handle,
            getProgressUpdate=False,
        )
        resp = self.make_request(self._client.GetOperationStatus, req)
        if resp.operationState in [
            ttypes.TOperationState.RUNNING_STATE,
            ttypes.TOperationState.PENDING_STATE,
        ]:
            time.sleep(0.1)
        return resp


class DatabricksSession(BaseSparkSession):
    """
    Databricks session class
    """

    _no_schema_error = ServerOperationError
    _storage_base_path: str = PrivateAttr()
    _dbfs_client: DbfsExt = PrivateAttr()

    source_type: SourceType = SourceType.DATABRICKS
    database_credential: Union[AccessTokenCredential, OAuthCredential]

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

    async def _list_databases(self) -> list[str]:
        output = []
        try:
            for catalog_info in self._workspace_client.catalogs.list(include_browse=False):
                if catalog_info.name:
                    output.append(catalog_info.name)
            return output
        except NotFound as exc:
            raise self._no_schema_error(str(exc)) from exc

    async def _list_schemas(self, database_name: str | None = None) -> list[str]:
        if database_name is None:
            return []
        output = []
        try:
            for schema_info in self._workspace_client.schemas.list(
                catalog_name=database_name, include_browse=False
            ):
                if schema_info.name:
                    output.append(schema_info.name)
            return output
        except NotFound as exc:
            raise self._no_schema_error(str(exc)) from exc

    async def _list_tables(
        self,
        database_name: str | None = None,
        schema_name: str | None = None,
        timeout: float = INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    ) -> list[TableSpec]:
        if database_name is None or schema_name is None:
            return []
        output = []
        try:
            for table_info in self._workspace_client.tables.list(
                catalog_name=database_name, schema_name=schema_name, include_browse=False
            ):
                if table_info.name:
                    output.append(TableSpec(name=table_info.name, description=table_info.comment))
            return output
        except NotFound as exc:
            raise self._no_schema_error(str(exc)) from exc

    async def list_table_schema(
        self,
        table_name: str | None,
        database_name: str | None = None,
        schema_name: str | None = None,
        timeout: float = INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    ) -> OrderedDict[str, ColumnSpecWithDescription]:
        try:
            table_info = self._workspace_client.tables.get(
                f"{database_name}.{schema_name}.{table_name}"
            )
            column_name_type_map: OrderedDict[str, ColumnSpecWithDescription] = OrderedDict()
            if table_info.columns is None:
                return column_name_type_map
            for column_info in table_info.columns:
                if column_info.name is None or column_info.type_text is None:
                    continue
                self._populate_column_spec(
                    column_info.name,
                    column_info.type_text.lower(),
                    column_info.comment,
                    column_name_type_map,
                )
                # add partition key information
                if column_info.partition_index is not None:
                    column_name_type_map[column_info.name].partition_metadata = PartitionMetadata(
                        is_partition_key_candidate=True,
                    )

            return column_name_type_map
        except NotFound as exc:
            raise self._no_schema_error(str(exc)) from exc

    @staticmethod
    @contextlib.contextmanager
    def exclude_env_credentials() -> Iterator[None]:
        """
        Context manager to ignore environment variables for credentials.
        This is useful to prevent accidental usage of environment variables
        when creating a session with explicit credentials.

        Yields
        -------
        Iterator[None]
            The context manager
        """
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GOOGLE_CREDENTIALS", None)
            os.environ.pop("DATABRICKS_CLIENT_ID", None)
            os.environ.pop("DATABRICKS_CLIENT_SECRET", None)
            yield

    @classmethod
    def get_workspace_client(
        cls, host: str, database_credential: Union[AccessTokenCredential, OAuthCredential]
    ) -> WorkspaceClient:
        """
        Get a WorkspaceClient instance based on the provided database credential.

        Parameters
        ----------
        host: str
            The host URL of the Databricks workspace.
        database_credential: Union[AccessTokenCredential, OAuthCredential]
            The credential to use for authentication with the Databricks workspace.

        Returns
        -------
        WorkspaceClient
        """
        if isinstance(database_credential, AccessTokenCredential):
            return WorkspaceClient(
                host=host,
                token=database_credential.access_token,
            )
        else:
            assert isinstance(database_credential, OAuthCredential)
            return WorkspaceClient(
                host=host,
                client_id=database_credential.client_id,
                client_secret=database_credential.client_secret,
            )

    def _execute_query(self, cursor: Any, query: str, **kwargs: Any) -> Any:
        try:
            return super()._execute_query(cursor, query, **kwargs)
        except DatabaseError as exc:
            if "Invalid SessionHandle" in str(exc):
                # This error is raised when the session is closed
                # Re-initialize the connection and try again
                logger.warning("Session closed, re-initializing connection")
                self._initialize_connection()
                # create a new cursor and replace all attributes of the old cursor with those from the new one
                new_cursor = self._connection.cursor()
                cursor.__dict__.clear()
                cursor.__dict__.update(new_cursor.__dict__)
                return super()._execute_query(cursor, query, **kwargs)
            else:
                raise

    def _initialize_connection(self) -> None:
        if not HAS_DATABRICKS_SQL_CONNECTOR:
            raise RuntimeError("databricks-sql-connector is not available")

        additional_connection_params: dict[str, Any] = {}
        # This patch is necessary for the query execution to be cancellable while the cursor is polling for results
        with self.exclude_env_credentials():
            with patch("databricks.sql.client.ThriftBackend", ThriftBackend):
                if isinstance(self.database_credential, AccessTokenCredential):
                    additional_connection_params["access_token"] = (
                        self.database_credential.access_token
                    )
                else:

                    def credentials_provider() -> Any:
                        assert isinstance(self.database_credential, OAuthCredential)
                        config = Config(
                            host=f"https://{self.host}",
                            client_id=self.database_credential.client_id,
                            client_secret=self.database_credential.client_secret,
                        )
                        return oauth_service_principal(config)

                    additional_connection_params["credentials_provider"] = credentials_provider
                self._connection = databricks_sql.connect(
                    server_hostname=self.host,
                    http_path=self.http_path,
                    catalog=self.catalog_name,
                    schema=self.schema_name,
                    _user_agent_entry=APPLICATION_NAME,
                    _use_arrow_native_complex_types=False,
                    **additional_connection_params,
                )

        # Always use UTC for session timezone
        cursor = self._connection.cursor()
        cursor.execute("SET TIME ZONE 'UTC'")
        cursor.close()

    @staticmethod
    def get_query_id(cursor: Any) -> str | None:
        return cursor.query_id if cursor.query_id else None

    async def _cancel_query(self, cursor: Any, query: str) -> bool:
        if cursor.active_op_handle:
            cursor.cancel()
            cursor.active_op_handle = None
            return True
        return False

    @cached_property
    def _workspace_client(self) -> WorkspaceClient:
        with self.exclude_env_credentials():
            return self.get_workspace_client(
                host=self.host, database_credential=self.database_credential
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
            return cursor.fetchall_arrow().cast(schema, safe=False).to_pandas()

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
                for batch in table.cast(schema, safe=False).to_batches():
                    yield batch
