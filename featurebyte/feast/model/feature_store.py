"""
This module contains feature store details used to construct feast data source & offline store config
"""
from typing import Any, Optional, Union

from abc import ABC, abstractmethod

from feast import SnowflakeSource
from feast.data_source import DataSource
from feast.infra.offline_stores.snowflake import SnowflakeOfflineStoreConfig

from featurebyte import SourceType
from featurebyte.models.credential import BaseDatabaseCredential, UsernamePasswordCredential
from featurebyte.query_graph.node.schema import BaseDatabaseDetails
from featurebyte.query_graph.node.schema import FeatureStoreDetails as BaseFeatureStoreDetails
from featurebyte.query_graph.node.schema import SnowflakeDetails as BaseSnowflakeDetails


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
    def get_offline_store_config(self, credential: Optional[BaseDatabaseCredential]) -> Any:
        """
        Get Feast offline store config based on the feature store details

        Parameters
        ----------
        credential: Optional[BaseDatabaseCredential]
            Credential to use to connect to the database

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
            database=self.database,
            warehouse=self.warehouse,
            schema=self.sf_schema,
            table=table_name,
            created_timestamp_column=created_timestamp_column,
        )

    def get_offline_store_config(self, credential: Optional[BaseDatabaseCredential]) -> Any:
        username, password = None, None
        if credential:
            assert isinstance(credential, UsernamePasswordCredential)
            username = credential.username
            password = credential.password

        return SnowflakeOfflineStoreConfig(
            account=self.account,
            user=username,
            password=password,
            role=None,
            warehouse=self.warehouse,
            database=self.database,
            schema_=self.sf_schema,
        )


FeastDatabaseDetails = Union[FeastSnowflakeDetails]


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
