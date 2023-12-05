"""
This module contains feature store & table schemas that are used in node parameters.
"""
from __future__ import annotations

from typing import Any, ClassVar, Optional, Union

from feast import SnowflakeSource
from feast.data_source import DataSource
from feast.infra.offline_stores.snowflake import SnowflakeOfflineStoreConfig
from pydantic import Field, StrictStr

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import DBVarType, SourceType, StorageType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.credential import BaseDatabaseCredential, UsernamePasswordCredential


class BaseDatabaseDetails(FeatureByteBaseModel):
    """Model for data source information"""

    is_local_source: ClassVar[bool] = False

    def create_feast_data_source(
        self,
        name: str,
        table_name: str,
        timestamp_field: Optional[str] = None,
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
        timestamp_field: Optional[str]
            Event timestamp field used for point in time joins of feature values
        created_timestamp_column: Optional[str]
            Timestamp column indicating when the row was created, used for de-duplicating rows.

        Returns
        -------
        DataSource
            Feast DataSource object
        # noqa: DAR202

        Raises
        ------
        NotImplementedError
            If the method is not implemented by the subclass
        """
        raise NotImplementedError()

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
        # noqa: DAR202

        Raises
        ------
        NotImplementedError
            If the method is not implemented by the subclass
        """
        raise NotImplementedError()


class SnowflakeDetails(BaseDatabaseDetails):
    """
    Model for details used to connect to a Snowflake data source.

    Examples
    --------
    >>> details= fb.SnowflakeDetails(
    ...   account="<account>",
    ...   warehouse="snowflake",
    ...   database="<database_name>",
    ...   sf_schema="<schema_name>",
    ... )
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.SnowflakeDetails")

    account: StrictStr = Field(
        description="The host account name which can be found using one of the following "
        "formats:\n"
        "- For Amazon Web Services US West, use`<account>.snowflakecomputing.com`\n"
        "- For all other regions on Amazon Web Services, use `<account>.<region>.snowflakecomputing.com`\n"
        "- For all regions on Microsoft Azure, use `<account>.<region>.azure.snowflakecomputing.com`"
    )
    warehouse: StrictStr = Field(
        description="The name of the warehouse containing the schema tables and columns."
    )
    database: StrictStr = Field(
        description="The name of the database containing the schema tables and columns."
    )
    sf_schema: StrictStr = Field(
        description="The name of the schema containing the database, tables and columns."
    )

    def create_feast_data_source(
        self,
        name: str,
        table_name: str,
        timestamp_field: Optional[str] = None,
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
        timestamp_field: Optional[str]
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


class SQLiteDetails(BaseDatabaseDetails):  # pylint: disable=abstract-method
    """Model for SQLite data source information"""

    filename: StrictStr
    is_local_source: ClassVar[bool] = True


class DatabricksDetails(BaseDatabaseDetails):  # pylint: disable=abstract-method
    """
    Model for details used to connect to a Databricks data source.

    Examples
    --------
    >>> details = fb.DatabricksDetails(
    ...   host="<host_name>",
    ...   http_path="<http_path>",
    ...   featurebyte_catalog="hive_metastore",
    ...   featurebyte_schema="<schema_name>",
    ...   storage_spark_url="dbfs:/FileStore/<schema_name>",
    ... )
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.DatabricksDetails")

    host: StrictStr = Field(
        description="Databricks host. This is typically the URL you use to go to to access your databricks environment."
    )
    http_path: StrictStr = Field(description="Databricks compute resource URL.")
    featurebyte_catalog: StrictStr = Field(
        description="Name of the database that holds metadata about the actual data. This is commonly filled as "
        "`hive_metastore`."
    )
    featurebyte_schema: StrictStr = Field(
        description="The name of the schema containing the tables and columns."
    )
    storage_spark_url: StrictStr = Field(
        description="DBFS path where we will be reading our data from."
    )


class SparkDetails(BaseDatabaseDetails):  # pylint: disable=abstract-method
    """
    Model for details used to connect to a Spark data source.

    Examples
    --------
    >>> details = fb.SparkDetails(
    ...   host="<host>",
    ...   port=10003,
    ...   featurebyte_catalog="spark_catalog",
    ...   featurebyte_schema="<schema_name>",
    ...   storage_type=fb.StorageType.S3,
    ...   storage_url="<storage_url>",
    ...   storage_spark_url="gs://dataproc-cluster-staging/{<schema_name>}"
    ... )
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.SparkDetails")

    host: StrictStr = Field(
        default="127.0.0.1", description="The server where your spark cluster is hosted."
    )
    port: int = Field(default=10000, description="The port your spark cluster is hosted on.")
    http_path: StrictStr = Field(default="cliservice", description="Spark compute resource URL.")
    use_http_transport: bool = Field(
        default=False,
        description="Configuration on whether to use HTTP as our transport layer. Defaults to Thrift",
    )
    use_ssl: bool = Field(
        default=False,
        description="Configuration on whether to use SSL. Only applicable if use_http_transport is set to True.",
    )
    storage_type: StorageType = Field(
        description="Storage type of where we will be persisting the feature store to."
    )
    storage_url: str = Field(description="URL of where we will be uploading our custom UDFs to.")
    storage_spark_url: StrictStr = Field(
        description="URL of where we will be reading our data from. Note that this technically points to the same "
        "location as the storage_url. However, the format that the warehouse accepts differs between the read and "
        "write path, and as such, we require two fields."
    )
    featurebyte_catalog: StrictStr = Field(
        description="Name of the database that holds metadata about the actual data. This is commonly filled as "
        "`hive_metastore`."
    )
    featurebyte_schema: StrictStr = Field(
        description="The name of the schema containing the tables and columns."
    )


class TestDatabaseDetails(BaseDatabaseDetails):  # pylint: disable=abstract-method
    """Model for a no-op mock database details for use in tests"""


DatabaseDetails = Union[
    SnowflakeDetails, SparkDetails, SQLiteDetails, DatabricksDetails, TestDatabaseDetails
]


class FeatureStoreDetails(FeatureByteBaseModel):
    """FeatureStoreDetail"""

    type: SourceType = Field(
        description="Returns the type of the feature store (Spark, Snowflake, DataBricks,...) "
        "represented by the FeatureStore object."
    )
    details: DatabaseDetails = Field(
        description="Returns the details of the database used for the FeatureStore object."
    )


class TableDetails(FeatureByteBaseModel):
    """Table details"""

    database_name: Optional[StrictStr]
    schema_name: Optional[StrictStr]
    table_name: StrictStr


class ColumnSpec(FeatureByteBaseModel):
    """
    Schema for columns retrieval
    """

    name: StrictStr
    dtype: DBVarType
