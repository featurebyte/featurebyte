"""
Spark Thrift Server Offline Store
"""

import json
from abc import abstractmethod
from datetime import datetime
from typing import Any, Callable, Iterable, List, Literal, Optional, Tuple, Union

import numpy as np
import pandas as pd
import pyarrow
from feast import FeatureView, OnDemandFeatureView
from feast.data_source import DataSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob, RetrievalMetadata
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from pytz import utc
from sqlglot import expressions

from featurebyte.enum import SourceType, StorageType
from featurebyte.feast.infra.offline_stores.spark_thrift_source import SparkThriftSource
from featurebyte.logging import get_logger
from featurebyte.models.credential import StorageCredential
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.session.base_spark import BaseSparkSession
from featurebyte.session.spark import SparkDatabaseCredential, SparkSession

logger = get_logger(__name__)


class BaseSparkThriftOfflineStoreConfig(FeastConfigBaseModel):
    """Base Offline store config for Spark Thrift Server"""

    host: str
    http_path: str
    catalog_name: str
    schema_name: str

    @abstractmethod
    def get_db_session(self) -> BaseSparkSession:
        """
        Get a Spark Thrift Session

        Returns
        -------
        BaseSparkSession
            Spark Thrift Session

        Raises
        ------
        ValueError
            If the store_config is not a valid Spark Thrift Offline Store Config
        """

    def get_table_column_names_and_types(
        self,
        table_name: str,
    ) -> Iterable[Tuple[str, str]]:
        """
        Get the column names and types of a table

        Parameters
        ----------
        table_name: str
            Table name

        Returns
        -------
        Iterable[Tuple[str, str]]
        """
        db_session = self.get_db_session()
        result = db_session.execute_query_blocking(f"DESCRIBE {table_name}")
        assert result is not None
        return tuple((row.col_name, row.data_type) for _, row in result.iterrows())


class SparkThriftOfflineStoreConfig(BaseSparkThriftOfflineStoreConfig):
    """Offline store config for Spark Thrift Server"""

    type: Literal["spark_thrift.offline"] = "spark_thrift.offline"
    """ Offline store type selector"""

    port: int
    use_http_transport: bool
    use_ssl: bool
    storage_path: str
    storage_type: StorageType
    storage_url: str
    database_credential: Optional[SparkDatabaseCredential]
    storage_credential: Optional[StorageCredential]

    def get_db_session(self) -> BaseSparkSession:
        return SparkSession(**self.model_dump())


class SparkThriftOfflineStore(OfflineStore):
    """
    Offline store for Spark Thrift Server
    """

    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(data_source, SparkThriftSource)
        db_session = config.offline_store.get_db_session()

        logger.info(f"Pulling latest features from {config.offline_store.type.split('.')[0]} store")

        timestamp_cols = [quoted_identifier(timestamp_field)]
        if created_timestamp_column:
            timestamp_cols.append(quoted_identifier(created_timestamp_column))
        join_key_cols = [quoted_identifier(col) for col in join_key_columns]
        feature_name_cols = [quoted_identifier(col) for col in feature_name_columns]
        partition_by_cols = [quoted_identifier(col) for col in join_key_columns]

        # query expression to get feast_row_ column
        start_date_str = _datetime_to_string(start_date)
        end_date_str = _datetime_to_string(end_date)
        feast_row_expr = expressions.Window(
            this=expressions.RowNumber(),
            partition_by=partition_by_cols,
            order=expressions.Order(
                expressions=[
                    expressions.Ordered(this=timestamp_col, desc=True)
                    for timestamp_col in timestamp_cols
                ]
            ),
        )

        # inner query expression
        select_fields = join_key_cols + feature_name_cols + timestamp_cols
        inner_expr = (
            expressions.Select(
                expressions=select_fields
                + [expressions.alias_(feast_row_expr, "feast_row_", quoted=True)],
            )
            .from_(
                expressions.Table(
                    catalog=quoted_identifier(data_source.catalog),
                    db=quoted_identifier(data_source.schema),
                    this=quoted_identifier(data_source.table),
                )
            )
            .where(
                expressions.Between(
                    this=quoted_identifier(timestamp_field),
                    low=expressions.Anonymous(
                        this="TIMESTAMP",
                        expressions=[expressions.Literal(this=start_date_str, is_string=True)],
                    ),
                    high=expressions.Anonymous(
                        this="TIMESTAMP",
                        expressions=[expressions.Literal(this=end_date_str, is_string=True)],
                    ),
                )
            )
        )

        # outer query expression
        if not join_key_columns:
            select_fields.append(
                expressions.alias_(
                    expression=expressions.Literal(this=DUMMY_ENTITY_VAL, is_string=True),
                    alias=DUMMY_ENTITY_ID,
                    quoted=True,
                )
            )
        expr = (
            expressions.Select(expressions=select_fields)
            .from_(inner_expr.subquery())
            .where(
                expressions.EQ(
                    this=quoted_identifier("feast_row_"),
                    expression=make_literal_value(1),
                )
            )
        )
        query = sql_to_string(expr, source_type=SourceType.SPARK)

        return SparkThriftRetrievalJob(
            db_session=db_session,
            query=query,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
        )

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        raise NotImplementedError()

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        raise NotImplementedError()

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        raise NotImplementedError()


class SparkThriftRetrievalJob(RetrievalJob):
    """
    Spark Thrift Server retrieval job
    """

    def __init__(
        self,
        db_session: BaseSparkSession,
        query: str,
        full_feature_names: bool,
        config: RepoConfig,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        super().__init__()
        self.db_session = db_session
        self.query = query
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata
        self._config = config

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        """
        Return dataset as Pandas DataFrame synchronously

        Parameters
        ----------
        timeout: Optional[int]
            Timeout in seconds

        Returns
        -------
        pd.DataFrame
        """
        result = self.db_session.execute_query_blocking(self.query)
        assert isinstance(result, pd.DataFrame)

        # skip if result is empty
        if result.shape[0] == 0:
            return result

        # convert arrays to string
        for column in result.columns:
            if result[column].dtype == "object":
                if isinstance(result[column].iloc[0], np.ndarray):
                    result[column] = result[column].apply(
                        lambda x: json.dumps(x.tolist()) if x is not None else None
                    )
        return result

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pyarrow.Table:
        """
        Return dataset as pyarrow Table synchronously

        Parameters
        ----------
        timeout: Optional[int]
            Timeout in seconds

        Returns
        -------
        pyarrow.Table
        """
        return pyarrow.Table.from_pandas(self._to_df_internal(timeout=timeout))

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: Optional[bool] = False,
        timeout: Optional[int] = None,
    ) -> None:
        raise NotImplementedError()

    def supports_remote_storage_export(self) -> bool:
        """
        Returns True if the RetrievalJob supports `to_remote_storage`.

        Returns
        -------
        bool
        """
        return False

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        """
        Return metadata information about retrieval.
        Should be available even before materializing the dataset itself.

        Returns
        -------
        RetrievalMetadata
        """
        return self._metadata


def _datetime_to_string(dt_object: datetime) -> str:
    """
    Convert datetime object to string

    Parameters
    ----------
    dt_object: datetime
        Datetime object

    Returns
    -------
    str
    """
    if dt_object.tzinfo:
        dt_object = dt_object.astimezone(tz=utc)
    return dt_object.strftime("%Y-%m-%d %H:%M:%S.%f")
