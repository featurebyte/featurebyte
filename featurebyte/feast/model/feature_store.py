"""
This module contains feature store details used to construct feast data source & offline store config
"""

from typing import Any, Optional, Union, cast

from abc import ABC, abstractmethod

from feast import SnowflakeSource
from feast.data_source import DataSource
from feast.infra.offline_stores.snowflake import SnowflakeOfflineStoreConfig

from featurebyte import SourceType
from featurebyte.feast.infra.offline_stores.databricks import (
    DataBricksOfflineStoreConfig,
    DataBricksUnityOfflineStoreConfig,
)
from featurebyte.feast.infra.offline_stores.spark_thrift import SparkThriftOfflineStoreConfig
from featurebyte.feast.infra.offline_stores.spark_thrift_source import SparkThriftSource
from featurebyte.models.credential import (
    BaseDatabaseCredential,
    BaseStorageCredential,
    UsernamePasswordCredential,
)
from featurebyte.query_graph.node.schema import (
    BaseDatabaseDetails,
    DatabricksDetails,
    DatabricksUnityDetails,
)
from featurebyte.query_graph.node.schema import FeatureStoreDetails as BaseFeatureStoreDetails
from featurebyte.query_graph.node.schema import SnowflakeDetails as BaseSnowflakeDetails
from featurebyte.query_graph.node.schema import SparkDetails


class AbstractDatabaseDetailsForFeast(BaseDatabaseDetails, ABC):
    """
    Abstract base class for database details.
    """

    @abstractmethod
    def create_feast_data_source(
        self,
        name: str,
        table_name: str,
        timestamp_field: str,
        created_timestamp_column: Optional[str] = None,
    ) -> DataSource:
        """
        Create a Feast DataSource from the details in this class

        Parameters
        ----------
        name: str
            Name of the DataSource
        table_name: str
            Name of the table to create a DataSource for
        timestamp_field: str
            Event timestamp field used for point in time joins of feature values
        created_timestamp_column: Optional[str]
            Timestamp column indicating when the row was created, used for de-duplicating rows.

        Returns
        -------
        DataSource
            Feast DataSource object
        """

    @abstractmethod
    def get_offline_store_config(
        self,
        database_credential: Optional[BaseDatabaseCredential],
        storage_credential: Optional[BaseStorageCredential],
    ) -> Any:
        """
        Get Feast offline store config based on the feature store details

        Parameters
        ----------
        database_credential: Optional[BaseDatabaseCredential]
            Credential to use to connect to the database
        storage_credential: Optional[BaseStorageCredential]
            Credential to use to connect to the storage

        Returns
        -------
        Any
            Feast offline store config
        """


class FeastSnowflakeDetails(AbstractDatabaseDetailsForFeast, BaseSnowflakeDetails):
    """
    Snowflake database details.
    """

    def create_feast_data_source(
        self,
        name: str,
        table_name: str,
        timestamp_field: str,
        created_timestamp_column: Optional[str] = None,
    ) -> DataSource:
        """
        Create a Feast DataSource from the details in this class

        Parameters
        ----------
        name: str
            Name of the DataSource
        table_name: str
            Name of the table to create a DataSource for
        timestamp_field: str
            Event timestamp field used for point in time joins of feature values
        created_timestamp_column: Optional[str]
            Timestamp column indicating when the row was created, used for de-duplicating rows.

        Returns
        -------
        DataSource
            Feast DataSource object
        """
        return SnowflakeSource(
            name=name,
            timestamp_field=timestamp_field,
            database=self.database_name,
            warehouse=self.warehouse,
            schema=self.schema_name,
            table=table_name,
            created_timestamp_column=created_timestamp_column,
        )

    def get_offline_store_config(
        self,
        database_credential: Optional[BaseDatabaseCredential],
        storage_credential: Optional[BaseStorageCredential],
    ) -> Any:
        username, password = None, None
        if database_credential:
            assert isinstance(database_credential, UsernamePasswordCredential)
            username = database_credential.username
            password = database_credential.password

        return SnowflakeOfflineStoreConfig(
            account=self.account,
            user=username,
            password=password,
            role=self.role_name,
            warehouse=self.warehouse,
            database=self.database_name,
            schema_=self.schema_name,
        )


class FeastSparkDetails(AbstractDatabaseDetailsForFeast, SparkDetails):
    """
    Spark database details.
    """

    def create_feast_data_source(
        self,
        name: str,
        table_name: str,
        timestamp_field: str,
        created_timestamp_column: Optional[str] = None,
    ) -> DataSource:
        """
        Create a Feast DataSource from the details in this class

        Parameters
        ----------
        name: str
            Name of the DataSource
        table_name: str
            Name of the table to create a DataSource for
        timestamp_field: str
            Event timestamp field used for point in time joins of feature values
        created_timestamp_column: Optional[str]
            Timestamp column indicating when the row was created, used for de-duplicating rows.

        Returns
        -------
        DataSource
            Feast DataSource object
        """
        return cast(
            DataSource,
            SparkThriftSource(
                name=name,
                timestamp_field=timestamp_field,
                catalog=self.catalog_name,
                schema=self.schema_name,
                table=table_name,
                created_timestamp_column=created_timestamp_column,
            ),
        )

    def get_offline_store_config(
        self,
        database_credential: Optional[BaseDatabaseCredential],
        storage_credential: Optional[BaseStorageCredential],
    ) -> Any:
        return SparkThriftOfflineStoreConfig(
            **self.dict(),
            database_credential=database_credential,
            storage_credential=storage_credential,
        )


class FeastDataBricksDetails(AbstractDatabaseDetailsForFeast, DatabricksDetails):
    """
    Databricks details.
    """

    def create_feast_data_source(
        self,
        name: str,
        table_name: str,
        timestamp_field: str,
        created_timestamp_column: Optional[str] = None,
    ) -> DataSource:
        """
        Create a Feast DataSource from the details in this class

        Parameters
        ----------
        name: str
            Name of the DataSource
        table_name: str
            Name of the table to create a DataSource for
        timestamp_field: str
            Event timestamp field used for point in time joins of feature values
        created_timestamp_column: Optional[str]
            Timestamp column indicating when the row was created, used for de-duplicating rows.

        Returns
        -------
        DataSource
            Feast DataSource object
        """
        return cast(
            DataSource,
            SparkThriftSource(
                name=name,
                timestamp_field=timestamp_field,
                catalog=self.catalog_name,
                schema=self.schema_name,
                table=table_name,
                created_timestamp_column=created_timestamp_column,
            ),
        )

    def get_offline_store_config(
        self,
        database_credential: Optional[BaseDatabaseCredential],
        storage_credential: Optional[BaseStorageCredential],
    ) -> Any:
        return DataBricksOfflineStoreConfig(**self.dict(), database_credential=database_credential)


class FeastDataBricksUnityDetails(AbstractDatabaseDetailsForFeast, DatabricksUnityDetails):
    """
    Databricks Unity details.
    """

    def create_feast_data_source(
        self,
        name: str,
        table_name: str,
        timestamp_field: str,
        created_timestamp_column: Optional[str] = None,
    ) -> DataSource:
        """
        Create a Feast DataSource from the details in this class

        Parameters
        ----------
        name: str
            Name of the DataSource
        table_name: str
            Name of the table to create a DataSource for
        timestamp_field: str
            Event timestamp field used for point in time joins of feature values
        created_timestamp_column: Optional[str]
            Timestamp column indicating when the row was created, used for de-duplicating rows.

        Returns
        -------
        DataSource
            Feast DataSource object
        """
        return cast(
            DataSource,
            SparkThriftSource(
                name=name,
                timestamp_field=timestamp_field,
                catalog=self.catalog_name,
                schema=self.schema_name,
                table=table_name,
                created_timestamp_column=created_timestamp_column,
            ),
        )

    def get_offline_store_config(
        self,
        database_credential: Optional[BaseDatabaseCredential],
        storage_credential: Optional[BaseStorageCredential],
    ) -> Any:
        return DataBricksUnityOfflineStoreConfig(
            **self.dict(), database_credential=database_credential
        )


FeastDatabaseDetails = Union[
    FeastSnowflakeDetails, FeastSparkDetails, FeastDataBricksDetails, FeastDataBricksUnityDetails
]


class FeatureStoreDetailsWithFeastConfiguration(BaseFeatureStoreDetails):
    """
    Feature store details

    name: str
        Feature store name
    database_details: DatabaseDetails
        Database details
    """

    type: SourceType
    details: FeastDatabaseDetails
