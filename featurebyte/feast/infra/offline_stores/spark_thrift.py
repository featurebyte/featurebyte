from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

import json
import uuid
import warnings
from datetime import datetime

import numpy as np
import pandas
import pandas as pd
import pyarrow
from feast import FeatureView, OnDemandFeatureView
from feast.data_source import DataSource
from feast.errors import EntitySQLEmptyResults, InvalidEntityType
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob, RetrievalMetadata
from feast.infra.registry.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.usage import log_exceptions_and_usage
from pytz import utc

# Make sure spark warning doesn't raise more than once.
from featurebyte import StorageType, get_logger
from featurebyte.feast.infra.offline_stores.spark_thrift_source import (
    SavedDatasetSparkThriftStorage,
    SparkThriftSource,
    get_table_column_names_and_types,
)
from featurebyte.models.credential import StorageCredential
from featurebyte.session.base_spark import BaseSparkSession
from featurebyte.session.databricks import DatabricksSession
from featurebyte.session.databricks_unity import DatabricksUnitySession
from featurebyte.session.spark import SparkDatabaseCredential, SparkSession

logger = get_logger(__name__)


SparkThriftSession = Union[SparkSession, DatabricksSession, DatabricksUnitySession]


class BaseSparkThriftOfflineStoreConfig(FeastConfigBaseModel):
    """Base Offline store config for Spark Thrift Server"""

    host: str
    http_path: str
    storage_path: str
    catalog_name: str
    schema_name: str


class SparkThriftOfflineStoreConfig(BaseSparkThriftOfflineStoreConfig):
    """Offline store config for Spark Thrift Server"""

    type: Literal["spark_thrift.offline"] = "spark_thrift.offline"
    """ Offline store type selector"""

    port: int
    use_http_transport: bool
    use_ssl: bool
    storage_type: StorageType
    storage_url: str
    database_credential: Optional[SparkDatabaseCredential]
    storage_credential: Optional[StorageCredential]


def get_db_session(store_config: BaseSparkThriftOfflineStoreConfig) -> BaseSparkSession:
    """
    Get a Spark Thrift Session

    Parameters
    ----------
    store_config: BaseSparkThriftOfflineStoreConfig
        Spark Thrift Offline Store Config

    Returns
    -------
    BaseSparkSession
        Spark Thrift Session
    """
    if isinstance(store_config, SparkThriftOfflineStoreConfig):
        return SparkSession(**store_config.dict())
    raise ValueError(f"Unknown offline store config type: {type(store_config)}")


class SparkThriftOfflineStore(OfflineStore):
    @staticmethod
    @log_exceptions_and_usage(offline_store="spark_thrift")
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
        db_session = get_db_session(config.offline_store)
        assert isinstance(config.offline_store, BaseSparkThriftOfflineStoreConfig)
        assert isinstance(data_source, SparkThriftSource)

        logger.info("Pulling latest features from spark thrift offline store")

        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = "`, `".join(join_key_columns)
        if partition_by_join_key_string != "":
            partition_by_join_key_string = f"PARTITION BY `{partition_by_join_key_string}`"
        timestamps = [timestamp_field]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamps) + " DESC"
        field_string = "`, `".join(join_key_columns + feature_name_columns + timestamps)

        start_date_str = _format_datetime(start_date)
        end_date_str = _format_datetime(end_date)
        query = f"""
                SELECT
                    `{field_string}`
                    {f", {repr(DUMMY_ENTITY_VAL)} AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""}
                FROM (
                    SELECT `{field_string}`,
                    ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS feast_row_
                    FROM {from_expression} t1
                    WHERE {timestamp_field} BETWEEN TIMESTAMP('{start_date_str}') AND TIMESTAMP('{end_date_str}')
                ) t2
                WHERE feast_row_ = 1
                """

        return SparkThriftRetrievalJob(
            db_session=db_session,
            query=query,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
        )

    @staticmethod
    @log_exceptions_and_usage(offline_store="spark_thrift")
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pandas.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, SparkThriftOfflineStoreConfig)
        for fv in feature_views:
            assert isinstance(fv.batch_source, SparkThriftSource)

        db_session = get_db_session(config.offline_store)
        tmp_entity_df_table_name = offline_utils.get_temp_entity_table_name()

        entity_schema = _get_entity_schema(
            db_session=db_session,
            entity_df=entity_df,
        )
        event_timestamp_col = offline_utils.infer_event_timestamp_from_entity_df(
            entity_schema=entity_schema,
        )
        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df,
            event_timestamp_col,
            db_session,
        )
        _upload_entity_df(
            db_session=db_session,
            table_name=tmp_entity_df_table_name,
            entity_df=entity_df,
            event_timestamp_col=event_timestamp_col,
        )

        expected_join_keys = offline_utils.get_expected_join_keys(
            project=project, feature_views=feature_views, registry=registry
        )
        offline_utils.assert_expected_columns_in_entity_df(
            entity_schema=entity_schema,
            join_keys=expected_join_keys,
            entity_df_event_timestamp_col=event_timestamp_col,
        )

        query_context = offline_utils.get_feature_view_query_context(
            feature_refs,
            feature_views,
            registry,
            project,
            entity_df_event_timestamp_range,
        )

        query = offline_utils.build_point_in_time_query(
            feature_view_query_contexts=query_context,
            left_table_query_string=tmp_entity_df_table_name,
            entity_df_event_timestamp_col=event_timestamp_col,
            entity_df_columns=entity_schema.keys(),
            query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
            full_feature_names=full_feature_names,
        )

        return SparkThriftRetrievalJob(
            db_session=db_session,
            query=query,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(set(entity_schema.keys()) - {event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
            config=config,
        )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        assert isinstance(config.offline_store, SparkThriftOfflineStoreConfig)
        assert isinstance(feature_view.batch_source, SparkThriftSource)

        db_session = get_db_session(config.offline_store)
        source_table_name = feature_view.batch_source.get_table_query_string()
        source_schema = list(get_table_column_names_and_types(db_session, source_table_name))
        source_column_names = [colname for colname, _ in source_schema]
        if source_column_names != table.column_names:
            raise ValueError(
                f"The input pyarrow table has incorrect columns {table.column_names}. "
                f"Expected columns are: {source_column_names}."
            )

        temp_table_name = "feast_batch_source_" + uuid.uuid4().hex
        db_session.register_table(
            table_name=temp_table_name, dataframe=table.to_pandas(), temporary=True
        )
        table_schema = list(get_table_column_names_and_types(db_session, temp_table_name))

        select_expression = []
        for i, (source_column_name, source_column_type) in enumerate(source_schema):
            target_column_name, target_column_type = table_schema[i]
            assert (
                source_column_name == target_column_name
            ), f"Column names do not match: {source_column_name} != {target_column_name}"
            if target_column_type != source_column_type:
                select_expression.append(
                    f"cast({target_column_name} as {source_column_type}) as {target_column_name}"
                )
            else:
                select_expression.append(source_column_name)

        db_session.execute_query_blocking(
            f"INSERT INTO {source_table_name} SELECT {','.join(select_expression)} FROM {temp_table_name}"
        )

    @staticmethod
    @log_exceptions_and_usage(offline_store="spark_thrift")
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        """
        Note that join_key_columns, feature_name_columns, timestamp_field, and
        created_timestamp_column have all already been mapped to column names of the
        source table and those column names are the values passed into this function.

        Parameters
        ----------
        config: RepoConfig
            Offline store configuration
        data_source: DataSource
            Source of the data
        join_key_columns: List[str]
            List of join key columns
        feature_name_columns: List[str]
            List of feature name columns
        timestamp_field: str
            Timestamp column
        start_date: datetime
            Start date of the data to pull
        end_date: datetime
            End date of the data to pull

        Returns
        -------
        RetrievalJob
        """
        assert isinstance(config.offline_store, SparkThriftOfflineStoreConfig)
        assert isinstance(data_source, SparkThriftSource)
        warnings.warn(
            "The spark offline store is an experimental feature in alpha development. "
            "This API is unstable and it could and most probably will be changed in the future.",
            RuntimeWarning,
        )

        db_session = get_db_session(config.offline_store)

        fields = ", ".join(join_key_columns + feature_name_columns + [timestamp_field])
        from_expression = data_source.get_table_query_string()
        start_date = start_date.astimezone(tz=utc)
        end_date = end_date.astimezone(tz=utc)

        query = f"""
            SELECT {fields}
            FROM {from_expression}
            WHERE {timestamp_field} BETWEEN TIMESTAMP '{start_date}' AND TIMESTAMP '{end_date}'
        """

        return SparkThriftRetrievalJob(
            db_session=db_session,
            query=query,
            full_feature_names=False,
            config=config,
        )


class SparkThriftRetrievalJob(RetrievalJob):
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
        statements = self.query.split("---EOS---")
        for statement in statements[:-1]:
            self.db_session.execute_query_blocking(statement)
        result = self.db_session.execute_query_blocking(statements[-1])

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
    ):
        """
        Run the retrieval and persist the results in the same offline store used for read.

        Parameters
        ----------
        storage: SavedDatasetStorage
            Storage to persist the dataset
        allow_overwrite: Optional[bool]
            Allow overwriting existing dataset
        timeout: Optional[int]
            Timeout in seconds
        """
        assert isinstance(storage, SavedDatasetSparkThriftStorage)
        table_name = storage.spark_thrift_options.table
        statements = self.query.split("---EOS---")
        for statement in statements[:-1]:
            self.db_session.execute_query_blocking(statement)
        self.db_session.register_table_with_query(table_name, self.query)

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


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    db_session: BaseSparkSession,
) -> Tuple[datetime, datetime]:
    if isinstance(entity_df, pd.DataFrame):
        entity_df_event_timestamp = entity_df.loc[:, entity_df_event_timestamp_col].infer_objects()
        if pd.api.types.is_string_dtype(entity_df_event_timestamp):
            entity_df_event_timestamp = pd.to_datetime(entity_df_event_timestamp, utc=True)
        entity_df_event_timestamp_range = (
            entity_df_event_timestamp.min().to_pydatetime(),
            entity_df_event_timestamp.max().to_pydatetime(),
        )
    elif isinstance(entity_df, str):
        # If the entity_df is a string (SQL query), determine range
        # from table
        df = db_session.execute_query_blocking(
            f"SELECT MIN(`{entity_df_event_timestamp_col}`) AS `min_ts`, "
            f"MAX(`{entity_df_event_timestamp_col}`) AS `max_ts` "
            "FROM ({entity_df})"
        )

        # Checks if executing entity sql resulted in any data
        if not df:
            raise EntitySQLEmptyResults(entity_df)

        entity_df_event_timestamp_range = (
            df["min_ts"].tz_convert("UTC").tz_localize(None).iloc[0],
            df["max_ts"].tz_convert("UTC").tz_localize(None).iloc[0],
        )
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_df_event_timestamp_range


def _get_entity_schema(
    db_session: BaseSparkSession, entity_df: Union[pandas.DataFrame, str]
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, str):
        entity_df = db_session.execute_query_blocking(entity_df)
    if isinstance(entity_df, pd.DataFrame):
        return dict(zip(entity_df.columns, entity_df.dtypes))
    raise InvalidEntityType(type(entity_df))


def _upload_entity_df(
    db_session: BaseSparkSession,
    table_name: str,
    entity_df: Union[pandas.DataFrame, str],
    event_timestamp_col: str,
) -> None:
    if isinstance(entity_df, pd.DataFrame):
        entity_df[event_timestamp_col] = pd.to_datetime(entity_df[event_timestamp_col], utc=True)
        db_session.register_table(table_name=table_name, dataframe=entity_df, temporary=True)
        return
    elif isinstance(entity_df, str):
        db_session.register_table_with_query(table_name=table_name, query=entity_df, temporary=True)
        return
    else:
        raise InvalidEntityType(type(entity_df))


def _format_datetime(t: datetime) -> str:
    # Since Hive does not support timezone, need to transform to utc.
    if t.tzinfo:
        t = t.astimezone(tz=utc)
    dt = t.strftime("%Y-%m-%d %H:%M:%S.%f")
    return dt


MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
CREATE OR REPLACE TEMPORARY VIEW entity_dataframe AS (
    SELECT *,
        {{entity_df_event_timestamp_col}} AS entity_timestamp
        {% for featureview in featureviews %}
            ,CONCAT(
                {% for entity in featureview.entities %}
                    CAST({{entity}} AS STRING),
                {% endfor %}
                CAST({{entity_df_event_timestamp_col}} AS STRING)
            ) AS {{featureview.name}}__entity_row_unique_id
        {% endfor %}
    FROM {{ left_table_query_string }}
);

---EOS---

{% for featureview in featureviews %}

CREATE OR REPLACE TEMPORARY VIEW {{ featureview.name }}__cleaned AS (

    WITH {{ featureview.name }}__entity_dataframe AS (
        SELECT
            {{ featureview.entities | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
            entity_timestamp,
            {{featureview.name}}__entity_row_unique_id
        FROM entity_dataframe
        GROUP BY
            {{ featureview.entities | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
            entity_timestamp,
            {{featureview.name}}__entity_row_unique_id
    ),

    /*
     This query template performs the point-in-time correctness join for a single feature set table
     to the provided entity table.

     1. We first join the current feature_view to the entity dataframe that has been passed.
     This JOIN has the following logic:
        - For each row of the entity dataframe, only keep the rows where the `timestamp_field`
        is less than the one provided in the entity dataframe
        - If there a TTL for the current feature_view, also keep the rows where the `timestamp_field`
        is higher the the one provided minus the TTL
        - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
        computed previously

     The output of this CTE will contain all the necessary information and already filtered out most
     of the data that is not relevant.
    */

    {{ featureview.name }}__subquery AS (
        SELECT
            {{ featureview.timestamp_field }} as event_timestamp,
            {{ featureview.created_timestamp_column ~ ' as created_timestamp,' if featureview.created_timestamp_column else '' }}
            {{ featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
            {% for feature in featureview.features %}
                {{ feature }} as {% if full_feature_names %}{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}{% if loop.last %}{% else %}, {% endif %}
            {% endfor %}
        FROM {{ featureview.table_subquery }}
        WHERE {{ featureview.timestamp_field }} <= '{{ featureview.max_event_timestamp }}'
        {% if featureview.ttl == 0 %}{% else %}
        AND {{ featureview.timestamp_field }} >= '{{ featureview.min_event_timestamp }}'
        {% endif %}
    ),

    {{ featureview.name }}__base AS (
        SELECT
            subquery.*,
            entity_dataframe.entity_timestamp,
            entity_dataframe.{{featureview.name}}__entity_row_unique_id
        FROM {{ featureview.name }}__subquery AS subquery
        INNER JOIN {{ featureview.name }}__entity_dataframe AS entity_dataframe
        ON TRUE
            AND subquery.event_timestamp <= entity_dataframe.entity_timestamp

            {% if featureview.ttl == 0 %}{% else %}
            AND subquery.event_timestamp >= entity_dataframe.entity_timestamp - {{ featureview.ttl }} * interval '1' second
            {% endif %}

            {% for entity in featureview.entities %}
            AND subquery.{{ entity }} = entity_dataframe.{{ entity }}
            {% endfor %}
    ),

    /*
     2. If the `created_timestamp_column` has been set, we need to
     deduplicate the data first. This is done by calculating the
     `MAX(created_at_timestamp)` for each event_timestamp.
     We then join the data on the next CTE
    */
    {% if featureview.created_timestamp_column %}
    {{ featureview.name }}__dedup AS (
        SELECT
            {{featureview.name}}__entity_row_unique_id,
            event_timestamp,
            MAX(created_timestamp) as created_timestamp
        FROM {{ featureview.name }}__base
        GROUP BY {{featureview.name}}__entity_row_unique_id, event_timestamp
    ),
    {% endif %}

    /*
     3. The data has been filtered during the first CTE "*__base"
     Thus we only need to compute the latest timestamp of each feature.
    */
    {{ featureview.name }}__latest AS (
        SELECT
            event_timestamp,
            {% if featureview.created_timestamp_column %}created_timestamp,{% endif %}
            {{featureview.name}}__entity_row_unique_id
        FROM
        (
            SELECT *,
                ROW_NUMBER() OVER(
                    PARTITION BY {{featureview.name}}__entity_row_unique_id
                    ORDER BY event_timestamp DESC{% if featureview.created_timestamp_column %},created_timestamp DESC{% endif %}
                ) AS row_number
            FROM {{ featureview.name }}__base
            {% if featureview.created_timestamp_column %}
                INNER JOIN {{ featureview.name }}__dedup
                USING ({{featureview.name}}__entity_row_unique_id, event_timestamp, created_timestamp)
            {% endif %}
        )
        WHERE row_number = 1
    )

    /*
     4. Once we know the latest value of each feature for a given timestamp,
     we can join again the data back to the original "base" dataset
    */
    SELECT base.*
    FROM {{ featureview.name }}__base as base
    INNER JOIN {{ featureview.name }}__latest
    USING(
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp
        {% if featureview.created_timestamp_column %}
            ,created_timestamp
        {% endif %}
    )
)

---EOS---

{% endfor %}

/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */

SELECT {{ final_output_feature_names | join(', ')}}
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        {{featureview.name}}__entity_row_unique_id
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}
        {% endfor %}
    FROM {{ featureview.name }}__cleaned
) USING ({{featureview.name}}__entity_row_unique_id)
{% endfor %}
"""
