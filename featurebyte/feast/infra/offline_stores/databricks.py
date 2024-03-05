"""
DataBricks offline store
"""
from typing import Literal, Optional

from featurebyte import AccessTokenCredential
from featurebyte.feast.infra.offline_stores.spark_thrift import (
    BaseSparkThriftOfflineStoreConfig,
    SparkThriftOfflineStore,
)
from featurebyte.session.base_spark import BaseSparkSession
from featurebyte.session.databricks import DatabricksSession
from featurebyte.session.databricks_unity import DatabricksUnitySession


class DataBricksOfflineStoreConfig(BaseSparkThriftOfflineStoreConfig):
    """Offline store config for DataBricks"""

    type: Literal["databricks.offline"] = "databricks.offline"
    """ Offline store type selector"""

    database_credential: AccessTokenCredential

    def get_db_session(self) -> BaseSparkSession:
        return DatabricksSession(**self.dict())


class DataBricksUnityOfflineStoreConfig(BaseSparkThriftOfflineStoreConfig):
    """Offline store config for DataBricks Unity"""

    type: Literal["databricks_unity.offline"] = "databricks_unity.offline"
    """ Offline store type selector"""

    storage_path: Optional[str]  # type: ignore
    group_name: str
    database_credential: AccessTokenCredential

    def get_db_session(self) -> BaseSparkSession:
        return DatabricksUnitySession(**self.dict())


class DataBricksOfflineStore(SparkThriftOfflineStore):
    """Offline store for DataBricks"""


class DataBricksUnityOfflineStore(SparkThriftOfflineStore):
    """Offline store for DataBricks Unity"""
