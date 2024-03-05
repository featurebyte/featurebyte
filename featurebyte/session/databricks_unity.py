"""
Databricks unity
"""
from typing import Any, BinaryIO

from pydantic import Field, PrivateAttr

from featurebyte import SourceType
from featurebyte.session.base import BaseSchemaInitializer
from featurebyte.session.base_spark import BaseSparkSchemaInitializer
from featurebyte.session.databricks import DatabricksSession

try:
    from databricks.sdk import FilesAPI
except ImportError:
    pass


class DatabricksUnitySchemaInitializer(BaseSparkSchemaInitializer):
    """
    Databricks unity schema initializer
    """

    @property
    def current_working_schema_version(self) -> int:
        return 13

    async def create_schema(self) -> None:
        await super().create_schema()
        # grant permissions on schema to the group
        assert isinstance(self.session, DatabricksUnitySession)
        grant_permissions_query = f"GRANT ALL PRIVILEGES ON SCHEMA `{self.session.schema_name}` TO `{self.session.group_name}`"
        await self.session.execute_query(grant_permissions_query)
        # create staging volume if not exists
        await self.session.execute_query(f"CREATE VOLUME IF NOT EXISTS {self.session.volume_name}")

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

    source_type: SourceType = Field(SourceType.DATABRICKS_UNITY, const=True)
    group_name: str

    def __init__(self, **data: Any) -> None:
        # set storage path based on catalog and schema name
        catalog_name = data.get("catalog_name")
        schema_name = data.get("schema_name")
        data["storage_path"] = f"dbfs:/Volumes/{catalog_name}/{schema_name}/staging"
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
        super()._initialize_storage()
        self._files_client = FilesAPI(self._workspace_client.api_client)

    def _upload_file_to_storage(self, path: str, src: BinaryIO) -> None:
        self._files_client.upload(file_path=path, contents=src, overwrite=True)

    def _delete_file_from_storage(self, path: str) -> None:
        self._files_client.delete(file_path=path)
