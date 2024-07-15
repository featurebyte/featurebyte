"""
Spark Thrift Server Data Source
"""

import json
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, cast

from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.type_map import spark_to_feast_value_type
from feast.value_type import ValueType
from typeguard import typechecked


@typechecked
class SparkThriftSource(DataSource):
    """
    Feast DataSource for Spark Thrift Server
    """

    def __init__(
        self,
        *,
        name: str,
        catalog: str,
        schema: str,
        table: str,
        timestamp_field: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        date_partition_column: Optional[str] = None,
    ):
        super().__init__(
            name=name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
            date_partition_column=date_partition_column,
        )

        self.spark_thrift_options = SparkThriftOptions(
            catalog=catalog,
            schema=schema,
            table=table,
        )

    @property
    def catalog(self) -> str:
        """
        Returns the catalog of this feature data source

        Returns
        -------
        str
        """
        return self.spark_thrift_options.catalog

    @property
    def schema(self) -> str:
        """
        Returns the schema of this feature data source

        Returns
        -------
        str
        """
        return self.spark_thrift_options.schema

    @property
    def table(self) -> str:
        """
        Returns the table of this feature data source

        Returns
        -------
        str
        """
        return self.spark_thrift_options.table

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> Any:
        assert data_source.HasField("custom_options")
        spark_thrift_options = SparkThriftOptions.from_proto(data_source.custom_options)

        return SparkThriftSource(
            name=data_source.name,
            catalog=spark_thrift_options.catalog,
            schema=spark_thrift_options.schema,
            table=spark_thrift_options.table,
            field_mapping=dict(data_source.field_mapping),
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="featurebyte.feast.infra.offline_stores.spark_thrift_source.SparkThriftSource",
            field_mapping=self.field_mapping,
            custom_options=self.spark_thrift_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column

        return data_source_proto

    def validate(self, config: RepoConfig) -> None:
        self.get_table_column_names_and_types(config)

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return spark_to_feast_value_type

    def get_table_column_names_and_types(self, config: RepoConfig) -> Iterable[Tuple[str, str]]:
        return cast(
            Iterable[Tuple[str, str]],
            config.offline_store.get_table_column_names_and_types(self.get_table_query_string()),
        )

    def get_table_query_string(self) -> str:
        """
        Returns a string that can directly be used to reference this table in SQL

        Returns
        -------
        str
        """
        return f"`{self.catalog}`.`{self.schema}`.`{self.table}`"


class SparkThriftOptions:
    """
    DataSource options for Spark Thrift Server
    """

    def __init__(
        self,
        catalog: str,
        schema: str,
        table: str,
    ):
        self.catalog = catalog
        self.schema = schema
        self.table = table

    @classmethod
    def from_proto(
        cls, spark_thrift_options_proto: DataSourceProto.CustomSourceOptions
    ) -> "SparkThriftOptions":
        """
        Creates a SparkThriftOptions from a protobuf representation of a SparkThriftOptions

        Parameters
        ----------
        spark_thrift_options_proto: DataSourceProto.CustomSourceOptions
            A protobuf representation of a SparkThriftOptions

        Returns
        -------
        SparkThriftOptions
        """
        config = json.loads(spark_thrift_options_proto.configuration.decode("utf8"))
        spark_thrift_options = cls(
            catalog=config["catalog"], schema=config["schema"], table=config["table"]
        )
        return spark_thrift_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        """
        Converts a SparkThriftOptions to its protobuf representation

        Returns
        -------
        DataSourceProto.CustomSourceOptions
        """
        spark_thrift_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=json.dumps({
                "catalog": self.catalog,
                "schema": self.schema,
                "table": self.table,
            }).encode()
        )
        return spark_thrift_options_proto
