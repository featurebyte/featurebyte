"""
Databricks unity
"""

from pydantic import Field

from featurebyte import SourceType
from featurebyte.session.base import BaseSchemaInitializer
from featurebyte.session.base_spark import BaseSparkSchemaInitializer
from featurebyte.session.databricks import DatabricksSession


class DatabricksUnitySchemaInitializer(BaseSparkSchemaInitializer):
    """
    Databricks unity schema initializer
    """

    async def initialize(self) -> None:
        await super().initialize()
        assert isinstance(self.session, DatabricksUnitySession)
        grant_permissions_query = f"GRANT ALL PRIVILEGES ON SCHEMA `{self.session.schema_name}` TO `{self.session.group_name}`"
        await self.session.execute_query(grant_permissions_query)

    @property
    def current_working_schema_version(self) -> int:
        return 12

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

    source_type: SourceType = Field(SourceType.DATABRICKS_UNITY, const=True)
    group_name: str

    def initializer(self) -> BaseSchemaInitializer:
        return DatabricksUnitySchemaInitializer(self)
