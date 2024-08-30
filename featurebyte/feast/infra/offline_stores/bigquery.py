"""
Offline store implementations for GCP BigQuery
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional
from unittest.mock import patch

import pyarrow
from feast.data_source import DataSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL
from feast.infra.offline_stores.bigquery import (
    BigQueryOfflineStore,
    BigQueryOfflineStoreConfig,
    BigQueryRetrievalJob,
    _get_bigquery_client,
)
from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.repo_config import RepoConfig
from feast.usage import log_exceptions_and_usage

from featurebyte.session.bigquery import bq_to_arrow_schema


class FeatureByteBigQueryOfflineStoreConfig(BigQueryOfflineStoreConfig):
    """Offline store config for GCP BigQuery"""


class FeatureByteBigQueryOfflineStore(BigQueryOfflineStore):
    """Offline store for GCP BigQuery"""

    @staticmethod
    @log_exceptions_and_usage(offline_store="bigquery")
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
        assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)
        assert isinstance(data_source, BigQuerySource)
        from_expression = data_source.get_table_query_string()

        quoted_feature_name_columns = [f"`{column}`" for column in feature_name_columns]
        quoted_join_key_columns = [f"`{column}`" for column in join_key_columns]
        partition_by_join_key_string = ", ".join(quoted_join_key_columns)
        if partition_by_join_key_string != "":
            partition_by_join_key_string = "PARTITION BY " + partition_by_join_key_string
        timestamps = [timestamp_field]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamps) + " DESC"
        field_string = ", ".join(quoted_join_key_columns + quoted_feature_name_columns + timestamps)
        project_id = config.offline_store.billing_project_id or config.offline_store.project_id
        client = _get_bigquery_client(
            project=project_id,
            location=config.offline_store.location,
        )
        query = f"""
            SELECT
                {field_string}
                {f", {repr(DUMMY_ENTITY_VAL)} AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""}
            FROM (
                SELECT {field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression}
                WHERE CAST({timestamp_field} AS TIMESTAMP) BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')
            )
            WHERE _feast_row = 1
            """

        # When materializing a single feature view, we don't need full feature names. On demand transforms aren't materialized
        return FeatureByteBigQueryRetrievalJob(
            query=query,
            client=client,
            config=config,
            full_feature_names=False,
        )


class FeatureByteBigQueryRetrievalJob(BigQueryRetrievalJob):
    def to_arrow(
        self,
        validation_reference: Optional["ValidationReference"] = None,
        timeout: Optional[int] = None,
    ) -> pyarrow.Table:
        with patch(
            "google.cloud.bigquery._pandas_helpers.bq_to_arrow_schema"
        ) as patched_bq_to_arrow_schema:
            patched_bq_to_arrow_schema.side_effect = bq_to_arrow_schema
            return super().to_arrow(validation_reference=validation_reference, timeout=timeout)
