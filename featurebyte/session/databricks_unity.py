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

    @property
    def sql_directory_name(self) -> str:
        return "databricks_unity"

    async def register_functions_from_jar(self) -> None:
        """
        Override to not do anything, and just do the default registration.
        """
        pass


class DatabricksUnitySession(DatabricksSession):
    """
    Databricks Unity session class
    """

    source_type: SourceType = Field(SourceType.DATABRICKS_UNITY, const=True)

    def initializer(self) -> BaseSchemaInitializer:
        return DatabricksUnitySchemaInitializer(self)
