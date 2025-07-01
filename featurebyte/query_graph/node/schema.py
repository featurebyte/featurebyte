"""
This module contains feature store & table schemas that are used in node parameters.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, ClassVar, Optional, Union

from pydantic import BaseModel, Field, StrictStr, model_validator

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import DBVarType, SourceType, StorageType
from featurebyte.models.base import FeatureByteBaseModel, NameStr
from featurebyte.query_graph.model.dtype import DBVarTypeInfo, DBVarTypeMetadata, PartitionMetadata
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.sql.source_info import SourceInfo


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

    @abstractmethod
    def get_source_info(self) -> SourceInfo:
        """
        Returns a SourceInfo object corresponding to the feature store

        Returns
        -------
        """


class SnowflakeDetails(BaseDatabaseDetails):
    """
    Model for details used to connect to a Snowflake data source.

    Examples
    --------
    >>> details = fb.SnowflakeDetails(
    ...     account="<account>",
    ...     warehouse="snowflake",
    ...     database_name="<database_name>",
    ...     schema_name="<schema_name>",
    ...     role_name="<role_name>",
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

    @model_validator(mode="before")
    @classmethod
    def _support_old_parameters(cls, values: Any) -> Any:
        # support old parameters
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

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

    def get_source_info(self) -> SourceInfo:
        return SourceInfo(
            database_name=self.database_name,
            schema_name=self.schema_name,
            source_type=SourceType.SNOWFLAKE,
        )


class SQLiteDetails(BaseDatabaseDetails):
    """Model for SQLite data source information"""

    filename: StrictStr
    is_local_source: ClassVar[bool] = True

    def get_source_info(self) -> SourceInfo:
        return SourceInfo(
            database_name="",
            schema_name="",
            source_type=SourceType.SQLITE,
        )


class BaseDatabricksDetails(BaseDatabaseDetails):
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


class DatabricksDetails(BaseDatabricksDetails):
    """
    Model for details used to connect to a Databricks data source.

    Examples
    --------
    >>> details = fb.DatabricksDetails(
    ...     host="<host_name>",
    ...     http_path="<http_path>",
    ...     catalog_name="hive_metastore",
    ...     schema_name="<schema_name>",
    ...     storage_path="dbfs:/FileStore/<schema_name>",
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

    @model_validator(mode="before")
    @classmethod
    def _support_old_parameters(cls, values: Any) -> Any:
        # support old parameters
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)
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

    def get_source_info(self) -> SourceInfo:
        return SourceInfo(
            database_name=self.catalog_name,
            schema_name=self.schema_name,
            source_type=SourceType.DATABRICKS,
        )


class DatabricksUnityDetails(BaseDatabricksDetails):
    """
    Model for details used to connect to a Databricks Unity data source.

    Examples
    --------
    >>> details = fb.DatabricksUnityDetails(
    ...     host="<host_name>",
    ...     http_path="<http_path>",
    ...     catalog_name="hive_metastore",
    ...     schema_name="<schema_name>",
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

    def get_source_info(self) -> SourceInfo:
        return SourceInfo(
            database_name=self.catalog_name,
            schema_name=self.schema_name,
            source_type=SourceType.DATABRICKS_UNITY,
        )


class SparkDetails(BaseDatabaseDetails):
    """
    Model for details used to connect to a Spark data source.

    Examples
    --------
    >>> details = fb.SparkDetails(
    ...     host="<host>",
    ...     port=10003,
    ...     catalog_name="spark_catalog",
    ...     schema_name="<schema_name>",
    ...     storage_type=fb.StorageType.S3,
    ...     storage_url="<storage_url>",
    ...     storage_path="gs://dataproc-cluster-staging/{<schema_name>}",
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

    @model_validator(mode="before")
    @classmethod
    def _support_old_parameters(cls, values: Any) -> Any:
        # support old parameters
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)
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

    def get_source_info(self) -> SourceInfo:
        return SourceInfo(
            database_name=self.catalog_name,
            schema_name=self.schema_name,
            source_type=SourceType.SPARK,
        )


class BigQueryDetails(BaseDatabaseDetails):  # pylint: disable=abstract-method
    """
    Model for details used to connect to a BigQuery data source.

    Examples
    --------
    >>> details = fb.BigQueryDetails(  # doctest: +SKIP
    ...     project_name="<project_name>",
    ...     dataset_name="<dataset_name>",
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.BigQueryDetails")

    project_name: StrictStr = Field(
        description="The name of the project to use for creation of output tables."
    )
    dataset_name: StrictStr = Field(
        description="The name of the dataset to use for creation of output tables."
    )
    location: Optional[StrictStr] = Field(
        default="US",
        description="The location of the dataset to use for creation of output tables.",
    )

    def get_source_info(self) -> SourceInfo:
        return SourceInfo(
            database_name=self.project_name,
            schema_name=self.dataset_name,
            source_type=SourceType.BIGQUERY,
        )


class TestDatabaseDetails(BaseDatabaseDetails):
    """Model for a no-op mock database details for use in tests"""

    def get_source_info(self) -> SourceInfo:
        return SourceInfo(
            database_name="",
            schema_name="",
            source_type=SourceType.TEST,
        )


DatabaseDetails = Union[
    SnowflakeDetails,
    SparkDetails,
    SQLiteDetails,
    DatabricksDetails,
    DatabricksUnityDetails,
    BigQueryDetails,
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
    details: Optional[DatabaseDetails] = Field(default=None)


class TableDetails(FeatureByteBaseModel):
    """Table details"""

    database_name: Optional[NameStr] = Field(default=None)
    schema_name: Optional[NameStr] = Field(default=None)
    table_name: NameStr

    def __hash__(self) -> int:
        return hash((self.database_name, self.schema_name, self.table_name))


class ColumnSpec(FeatureByteBaseModel):
    """
    Schema for columns retrieval
    """

    name: NameStr
    dtype: DBVarType
    dtype_metadata: Optional[DBVarTypeMetadata] = None
    partition_metadata: Optional[PartitionMetadata] = None

    def _validate_timestamp_settings(self) -> None:
        assert self.dtype_metadata is not None
        assert self.dtype_metadata.timestamp_schema is not None

        # invalid setting: unsupported datetime types
        if self.dtype not in DBVarType.supported_ts_datetime_types():
            expected_dtypes = sorted(DBVarType.supported_ts_datetime_types())
            raise ValueError(
                f"Column {self.name} is of type {self.dtype} (expected: {expected_dtypes})"
            )

        if self.dtype == DBVarType.VARCHAR:
            # invalid setting: missing format_string for VARCHAR timestamp
            if self.dtype_metadata.timestamp_schema.format_string is None:
                raise ValueError(
                    f"format_string is required in the timestamp_schema for column {self.name}"
                )
        else:
            # invalid setting: format_string provided but column is not string
            if self.dtype_metadata.timestamp_schema.format_string:
                raise ValueError(
                    f"format_string is not supported in the timestamp_schema for non-string column {self.name}"
                )

    @model_validator(mode="after")
    def _validate_string_timestamp_format_string(self) -> "ColumnSpec":
        if self.dtype_metadata and self.dtype_metadata.timestamp_schema:
            self._validate_timestamp_settings()
        return self

    @property
    def timestamp_schema(self) -> Optional[TimestampSchema]:
        """
        Returns the timestamp schema object if available

        Returns
        -------
        Optional[TimestampSchema]
            Timestamp schema
        """
        if self.dtype_metadata is not None:
            return self.dtype_metadata.timestamp_schema
        return None

    @property
    def dtype_info(self) -> DBVarTypeInfo:
        """
        Returns the database var type info object

        Returns
        -------
        DBVarTypeInfo
            Var type info
        """
        return DBVarTypeInfo(dtype=self.dtype, metadata=self.dtype_metadata)

    def is_compatible_with(self, other: ColumnSpec) -> bool:
        """
        Compares the column spec with another column spec

        Parameters
        ----------
        other: ColumnSpec
            The other column spec to compare with

        Returns
        -------
        bool
            True if the column specs are the same, False otherwise
        """
        if self.name != other.name:
            return False
        if not DBVarType.are_compatible_types(self.dtype, other.dtype):
            return False
        if self.dtype_metadata != other.dtype_metadata:
            return False
        return True
