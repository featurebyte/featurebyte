"""
Databricks unity
"""

from __future__ import annotations

from typing import Any, BinaryIO, Literal

import pandas as pd
from pydantic import Field, PrivateAttr
from sqlglot.expressions import Select

from featurebyte import SourceType
from featurebyte.query_graph.model.table import TableSpec
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import get_fully_qualified_table_name, sql_to_string
from featurebyte.session.base import INTERACTIVE_SESSION_TIMEOUT_SECONDS, BaseSchemaInitializer
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
        await self.session.set_owner("TABLE", "METADATA_SCHEMA")


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
        return 18

    async def create_schema(self) -> None:
        await super().create_schema()
        # grant permissions on schema to the group
        assert isinstance(self.session, DatabricksUnitySession)
        await self.session.set_owner("SCHEMA", self.session.schema_name)

    async def _register_sql_objects(self, items: list[dict[str, Any]]) -> None:
        await super()._register_sql_objects(items)
        for item in items:
            item_type = item["type"].upper()
            item_identifier = item["identifier"]
            await self.session.set_owner(item_type, item_identifier)

    async def register_missing_objects(self) -> None:
        # create staging volume if not exists
        assert isinstance(self.session, DatabricksUnitySession)
        await self.session.execute_query(f"CREATE VOLUME IF NOT EXISTS {self.session.volume_name}")
        await self.session.set_owner("VOLUME", self.session.volume_name)

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

    source_type: SourceType = Field(SourceType.DATABRICKS_UNITY, const=True)
    group_name: str

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

    async def set_owner(
        self, kind: Literal["SCHEMA", "TABLE", "VIEW", "VOLUME", "FUNCTION"], name: str
    ) -> None:
        """
        Set owner of the object

        Parameters
        ----------
        kind: Literal["SCHEMA", "TABLE", "VIEW", "VOLUME", "FUNCTION"]
            Object type
        name: str
            Object name
        """
        await self.execute_query(f"ALTER {kind} {name} OWNER TO `{self.group_name}`")

    async def register_table(self, table_name: str, dataframe: pd.DataFrame) -> None:
        await super().register_table(table_name, dataframe)
        # grant ownership of the table or view to the group
        await self.set_owner("TABLE", table_name)

    async def list_schemas(self, database_name: str | None = None) -> list[str]:
        schemas = await self.execute_query_interactive(
            f"SELECT SCHEMA_NAME FROM `{database_name}`.INFORMATION_SCHEMA.SCHEMATA"
        )
        output = []
        if schemas is not None:
            output.extend(schemas["SCHEMA_NAME"].tolist())
        return output

    async def list_tables(
        self,
        database_name: str | None = None,
        schema_name: str | None = None,
        timeout: float = INTERACTIVE_SESSION_TIMEOUT_SECONDS,
    ) -> list[TableSpec]:
        tables = await self.execute_query_interactive(
            f"SELECT TABLE_NAME FROM `{database_name}`.INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = '{schema_name}'",
            timeout=timeout,
        )
        output = []
        if tables is not None:
            for _, (name,) in tables[["TABLE_NAME"]].iterrows():
                output.append(TableSpec(name=name))
        return output

    async def create_table_as(
        self,
        table_details: TableDetails | str,
        select_expr: Select | str,
        kind: Literal["TABLE", "VIEW"] = "TABLE",
        partition_keys: list[str] | None = None,
        replace: bool = False,
        retry: bool = False,
        retry_num: int = 10,
        sleep_interval: int = 5,
    ) -> pd.DataFrame | None:
        result = await super().create_table_as(
            table_details,
            select_expr,
            kind,
            partition_keys,
            replace,
            retry,
            retry_num,
            sleep_interval,
        )

        # grant ownership of the table to the group
        if isinstance(table_details, str):
            table_details = TableDetails(
                database_name=None, schema_name=None, table_name=table_details
            )
        fully_qualified_table_name = sql_to_string(
            get_fully_qualified_table_name(table_details.dict()), source_type=self.source_type
        )
        await self.set_owner(kind, fully_qualified_table_name)
        return result
