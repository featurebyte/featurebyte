"""
Offline store implementations for GCP BigQuery
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional
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

from featurebyte.models.credential import GoogleCredential
from featurebyte.session.bigquery import bq_to_arrow_schema

if TYPE_CHECKING:
    from feast.saved_dataset import ValidationReference


class FeatureByteBigQueryOfflineStoreConfig(BigQueryOfflineStoreConfig):
    """Offline store config for GCP BigQuery"""

    database_credential: GoogleCredential


class FeatureByteBigQueryOfflineStore(BigQueryOfflineStore):
    """Offline store for GCP BigQuery

    Implementation of pull_latest_from_table_or_query is overridden to support special characters in
    column names.
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
    """
    BigQuery retrieval job for FeatureByte

    Implementation of to_arrow is overridden to handle JSON data type on empty table.
    """

    @staticmethod
    def _apply_json_encoding(result: pyarrow.Table) -> pyarrow.Table:
        schema = result.schema
        new_fields = []
        new_columns = []
        for i, field in enumerate(schema):
            column = result.column(i)
            if pyarrow.types.is_list(field.type):
                # Apply JSON encoding transformation
                pd_series = column.to_pandas()
                encoded_column = pyarrow.Array.from_pandas(
                    pd_series.apply(lambda x: json.dumps(list(x)) if x is not None else None)
                )
                new_field = pyarrow.field(field.name, pyarrow.string())
                new_fields.append(new_field)
                new_columns.append(encoded_column)
            else:
                # Keep the field unchanged if not array type
                new_fields.append(field)
                new_columns.append(column)

        # Create a new table with the transformed columns
        new_schema = pyarrow.schema(new_fields)
        result_encoded = pyarrow.Table.from_arrays(new_columns, schema=new_schema)

        return result_encoded

    def to_arrow(
        self,
        validation_reference: Optional["ValidationReference"] = None,
        timeout: Optional[int] = None,
    ) -> pyarrow.Table:
        with patch(
            "google.cloud.bigquery._pandas_helpers.bq_to_arrow_schema"
        ) as patched_bq_to_arrow_schema:
            patched_bq_to_arrow_schema.side_effect = bq_to_arrow_schema
            arrow_result = super().to_arrow(
                validation_reference=validation_reference, timeout=timeout
            )
        return self._apply_json_encoding(arrow_result)
