"""
Databricks unity
"""

from __future__ import annotations

from typing import Any, BinaryIO

from pydantic import PrivateAttr

from featurebyte import SourceType
from featurebyte.session.base import BaseSchemaInitializer
from featurebyte.session.base_spark import (
    BaseSparkMetadataSchemaInitializer,
    BaseSparkSchemaInitializer,
)
from featurebyte.session.databricks import DatabricksSession

try:
    from databricks.sdk.service.files import FilesAPI
except ImportError:
    pass


class DataBricksMetadataSchemaInitializer(BaseSparkMetadataSchemaInitializer):
    """
    Databricks metadata schema initializer
    """

    async def create_metadata_table_if_not_exists(self, current_migration_version: int) -> None:
        await super().create_metadata_table_if_not_exists(current_migration_version)
        # grant permissions on metadata table to the group
        assert isinstance(self.session, DatabricksUnitySession)


class DatabricksUnitySchemaInitializer(BaseSparkSchemaInitializer):
    """
    Databricks unity schema initializer
    """

    session: DatabricksUnitySession

    def __init__(self, session: DatabricksUnitySession):
        super().__init__(session=session)
        self.metadata_schema_initializer = DataBricksMetadataSchemaInitializer(session)

    @property
    def current_working_schema_version(self) -> int:
        return 21

    async def create_schema(self) -> None:
        await super().create_schema()
        # grant permissions on schema to the group
        assert isinstance(self.session, DatabricksUnitySession)

    async def register_missing_objects(self) -> None:
        # create staging volume if not exists
        assert isinstance(self.session, DatabricksUnitySession)
        await self.session.execute_query(f"CREATE VOLUME IF NOT EXISTS {self.session.volume_name}")

        # register missing other common objects by calling the super method
        await super().register_missing_objects()

    @property
    def sql_directory_name(self) -> str:
        return "databricks_unity"

    def register_jar(self) -> None:
        """
        Override since we don't need to register any jar.
        """

    async def register_functions_from_jar(self) -> None:
        """
        Override to not do anything, and just do the default registration.
        """


class DatabricksUnitySession(DatabricksSession):
    """
    Databricks Unity session class
    """

    _files_client: FilesAPI = PrivateAttr()

    source_type: SourceType = SourceType.DATABRICKS_UNITY

    def __init__(self, **data: Any) -> None:
        # set storage path based on catalog and schema name
        catalog_name = data.get("catalog_name")
        schema_name = data.get("schema_name")
        data["storage_path"] = f"/Volumes/{catalog_name}/{schema_name}/staging"
        super().__init__(**data)

    def initializer(self) -> BaseSchemaInitializer:
        return DatabricksUnitySchemaInitializer(self)

    @property
    def volume_name(self) -> str:
        """
        Volume name for storage

        Returns
        -------
        str:
            Volume name
        """
        return f"`{self.catalog_name}`.`{self.schema_name}`.`staging`"

    def _initialize_storage(self) -> None:
        self.storage_path = self.storage_path.rstrip("/")
        self._storage_base_path = self.storage_path
        self._files_client = FilesAPI(self._workspace_client.api_client)

    def _upload_file_to_storage(self, path: str, src: BinaryIO) -> None:
        self._files_client.upload(file_path=path, contents=src, overwrite=True)

    def _delete_file_from_storage(self, path: str) -> None:
        self._files_client.delete(file_path=path)
