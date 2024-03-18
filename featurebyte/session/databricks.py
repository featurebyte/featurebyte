"""
DatabricksSession class
"""
# pylint: disable=duplicate-code
from typing import Any, AsyncGenerator, BinaryIO, Dict, Optional

import json
import os
from io import BytesIO

import pandas as pd
import pyarrow as pa
from bson import ObjectId
from pydantic import Field, PrivateAttr

from featurebyte import AccessTokenCredential, logging
from featurebyte.common.utils import pa_table_to_record_batches
from featurebyte.enum import SourceType
from featurebyte.session.base import APPLICATION_NAME
from featurebyte.session.base_spark import BaseSparkSession

try:
    from databricks import sql as databricks_sql
    from databricks.sdk import DbfsExt, WorkspaceClient
    from databricks.sql.exc import ServerOperationError

    HAS_DATABRICKS_SQL_CONNECTOR = True
except ImportError:
    HAS_DATABRICKS_SQL_CONNECTOR = False


logger = logging.get_logger(__name__)


class ArrowTablePostProcessor:
    """
    Post processor for Arrow table to fix databricks return format
    """

    def __init__(self, schema: Dict[str, str]):
        self._map_columns = []
        for col_name, var_type in schema.items():
            if var_type.upper() == "MAP":
                self._map_columns.append(col_name)

    def to_dataframe(self, arrow_table: pa.Table) -> pd.DataFrame:
        """
        Convert Arrow table to Pandas dataframe

        Parameters
        ----------
        arrow_table: pa.Table
            Arrow table to convert

        Returns
        -------
        pd.DataFrame:
            Pandas dataframe
        """
        # handle map type. Databricks returns map as list of tuples
        # https://docs.databricks.com/sql/language-manual/sql-ref-datatypes.html#map
        # which is not supported by pyarrow. Below converts the tuple list to json string
        dataframe = arrow_table.to_pandas()
        for col_name in self._map_columns:
            dataframe[col_name] = dataframe[col_name].apply(
                lambda x: json.dumps(dict(x)) if x is not None else None
            )
        return dataframe


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

    def fetch_query_result_impl(self, cursor: Any) -> Optional[pd.DataFrame]:
        schema = None
        if cursor.description:
            schema = {row[0]: row[1] for row in cursor.description}

        if schema:
            post_processor = ArrowTablePostProcessor(schema=schema)
            arrow_table = cursor.fetchall_arrow()
            return post_processor.to_dataframe(arrow_table)

        return None

    async def fetch_query_stream_impl(self, cursor: Any) -> AsyncGenerator[pa.RecordBatch, None]:
        schema = None
        if cursor.description:
            schema = {row[0]: row[1] for row in cursor.description}

        if schema:
            post_processor = ArrowTablePostProcessor(schema=schema)
            # fetch results in batches
            counter = 0
            while True:
                dataframe = post_processor.to_dataframe(cursor.fetchmany_arrow(size=1000))
                arrow_table = pa.Table.from_pandas(dataframe)
                if arrow_table.num_rows == 0:
                    if counter == 0:
                        # return empty dataframe with correct schema
                        yield pa_table_to_record_batches(arrow_table)[0]
                    break
                for record_batch in arrow_table.to_batches():
                    counter += 1
                    yield record_batch
