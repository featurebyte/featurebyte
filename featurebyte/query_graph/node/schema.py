"""
This module contains feature store & table schemas that are used in node parameters.
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, Optional, Union

from pydantic import Field, StrictStr, root_validator

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import DBVarType, SourceType, StorageType
from featurebyte.models.base import FeatureByteBaseModel, NameStr


class BaseDatabaseDetails(FeatureByteBaseModel):
    """Model for data source information"""

    is_local_source: ClassVar[bool] = False

    @property
    def updatable_fields(self) -> set[str]:
        """
        Returns the fields that can be updated in the database details.

        Returns
        -------
        set[str]
            Set of fields that can be updated
        """
        return set()


class SnowflakeDetails(BaseDatabaseDetails):
    """
    Model for details used to connect to a Snowflake data source.

    Examples
    --------
    >>> details= fb.SnowflakeDetails(
    ...   account="<account>",
    ...   warehouse="snowflake",
    ...   database_name="<database_name>",
    ...   schema_name="<schema_name>",
    ...   role_name="<role_name>",
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.SnowflakeDetails")

    # instance variables
    account: StrictStr = Field(
        description="The host account name which can be found using one of the following "
        "formats:\n"
        "- For Amazon Web Services US West, use`<account>.snowflakecomputing.com`\n"
        "- For all other regions on Amazon Web Services, use `<account>.<region>.snowflakecomputing.com`\n"
        "- For all regions on Microsoft Azure, use `<account>.<region>.azure.snowflakecomputing.com`"
    )
    warehouse: StrictStr = Field(
        description="The name of default warehouse to use for computation."
    )
    database_name: StrictStr = Field(
        description="The name of the database to use for creation of output tables."
    )
    schema_name: StrictStr = Field(
        description="The name of the schema to use for creation of output tables."
    )
    role_name: StrictStr = Field(
        description="The name of the role to use for creation of output tables.",
        default="PUBLIC",
    )

    @root_validator(pre=True)
    @classmethod
    def _support_old_parameters(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # support old parameters
        database = values.get("database")
        if database:
            values["database_name"] = database
        sf_schema = values.get("sf_schema")
        if sf_schema:
            values["schema_name"] = sf_schema
        return values

    @property
    def updatable_fields(self) -> set[str]:
        return {"warehouse"}


class SQLiteDetails(BaseDatabaseDetails):  # pylint: disable=abstract-method
    """Model for SQLite data source information"""

    filename: StrictStr
    is_local_source: ClassVar[bool] = True


class BaseDatabricksDetails(BaseDatabaseDetails):  # pylint: disable=abstract-method
    """
    Base model for details used to connect to a Databricks data source.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.DatabricksDetails")

    # instance variables
    host: StrictStr = Field(
        description="Databricks host. This is typically the URL you use to go to to access your databricks environment."
    )
    http_path: StrictStr = Field(description="Databricks compute resource URL.")
    catalog_name: StrictStr = Field(
        description="The name of the catalog to use for creation of output tables."
    )
    schema_name: StrictStr = Field(
        description="The name of the schema to use for creation of output tables."
    )

    @property
    def updatable_fields(self) -> set[str]:
        return {"http_path"}


class DatabricksDetails(BaseDatabricksDetails):  # pylint: disable=abstract-method
    """
    Model for details used to connect to a Databricks data source.

    Examples
    --------
    >>> details = fb.DatabricksDetails(
    ...   host="<host_name>",
    ...   http_path="<http_path>",
    ...   catalog_name="hive_metastore",
    ...   schema_name="<schema_name>",
    ...   storage_path="dbfs:/FileStore/<schema_name>",
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.DatabricksDetails")

    # instance variables
    host: StrictStr = Field(
        description="Databricks host. This is typically the URL you use to go to to access your databricks environment."
    )
    http_path: StrictStr = Field(description="Databricks compute resource URL.")
    catalog_name: StrictStr = Field(
        description="The name of the catalog to use for creation of output tables."
    )
    schema_name: StrictStr = Field(
        description="The name of the schema to use for creation of output tables."
    )
    storage_path: StrictStr = Field(description="DBFS path to use for file storage.")

    @root_validator(pre=True)
    @classmethod
    def _support_old_parameters(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # support old parameters
        featurebyte_catalog = values.get("featurebyte_catalog")
        if featurebyte_catalog:
            values["catalog_name"] = featurebyte_catalog
        featurebyte_schema = values.get("featurebyte_schema")
        if featurebyte_schema:
            values["schema_name"] = featurebyte_schema
        storage_spark_url = values.get("storage_spark_url")
        if storage_spark_url:
            values["storage_path"] = storage_spark_url
        return values


class DatabricksUnityDetails(BaseDatabricksDetails):  # pylint: disable=abstract-method
    """
    Model for details used to connect to a Databricks Unity data source.

    Examples
    --------
    >>> details = fb.DatabricksUnityDetails(
    ...   host="<host_name>",
    ...   http_path="<http_path>",
    ...   catalog_name="hive_metastore",
    ...   schema_name="<schema_name>",
    ...   group_name="<group_name>",
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.DatabricksUnityDetails")

    # instance variables
    host: StrictStr = Field(
        description="Databricks host. This is typically the URL you use to go to to access your databricks environment."
    )
    http_path: StrictStr = Field(description="Databricks compute resource URL.")
    catalog_name: StrictStr = Field(
        description="The name of the catalog to use for creation of output tables."
    )
    schema_name: StrictStr = Field(
        description="The name of the schema to use for creation of output tables."
    )
    group_name: StrictStr = Field(
        description="The name of the group to use for creation of output tables."
    )


class SparkDetails(BaseDatabaseDetails):  # pylint: disable=abstract-method
    """
    Model for details used to connect to a Spark data source.

    Examples
    --------
    >>> details = fb.SparkDetails(
    ...   host="<host>",
    ...   port=10003,
    ...   catalog_name="spark_catalog",
    ...   schema_name="<schema_name>",
    ...   storage_type=fb.StorageType.S3,
    ...   storage_url="<storage_url>",
    ...   storage_path="gs://dataproc-cluster-staging/{<schema_name>}"
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.SparkDetails")

    # instance variables
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
    storage_path: StrictStr = Field(
        description="Path where we will be reading our data from. Note that this technically points to the same "
        "location as the storage_url. However, the format that the warehouse accepts differs between the read and "
        "write path, and as such, we require two fields."
    )
    catalog_name: StrictStr = Field(
        description="The name of the catalog to use for creation of output tables."
    )
    schema_name: StrictStr = Field(
        description="The name of the schema to use for creation of output tables."
    )

    @root_validator(pre=True)
    @classmethod
    def _support_old_parameters(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # support old parameters
        featurebyte_catalog = values.get("featurebyte_catalog")
        if featurebyte_catalog:
            values["catalog_name"] = featurebyte_catalog
        featurebyte_schema = values.get("featurebyte_schema")
        if featurebyte_schema:
            values["schema_name"] = featurebyte_schema
        storage_spark_url = values.get("storage_spark_url")
        if storage_spark_url:
            values["storage_path"] = storage_spark_url
        return values


class TestDatabaseDetails(BaseDatabaseDetails):  # pylint: disable=abstract-method
    """Model for a no-op mock database details for use in tests"""


DatabaseDetails = Union[
    SnowflakeDetails,
    SparkDetails,
    SQLiteDetails,
    DatabricksDetails,
    DatabricksUnityDetails,
    TestDatabaseDetails,
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


class InputNodeFeatureStoreDetails(FeatureByteBaseModel):
    """FeatureStoreDetails for input node"""

    type: SourceType
    details: Optional[DatabaseDetails]


class TableDetails(FeatureByteBaseModel):
    """Table details"""

    database_name: Optional[NameStr]
    schema_name: Optional[NameStr]
    table_name: NameStr


class ColumnSpec(FeatureByteBaseModel):
    """
    Schema for columns retrieval
    """

    name: NameStr
    dtype: DBVarType
