"""
SnapshotsTableValidationService class
"""

from __future__ import annotations

from sqlglot import expressions

from featurebyte.exception import TableValidationError
from featurebyte.models.snapshots_table import SnapshotsTableModel
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.materialisation import get_source_expr
from featurebyte.query_graph.sql.timestamp_helper import apply_snapshot_adjustment
from featurebyte.query_graph.sql.validation_helper import get_duplicate_rows_per_keys
from featurebyte.schema.snapshots_table import SnapshotsTableCreate, SnapshotsTableServiceUpdate
from featurebyte.service.base_table_validation import BaseTableValidationService
from featurebyte.session.base import BaseSession

DUPLICATE_COUNT_RESULT_NAME = "COUNT_PER_SNAPSHOT_ID_AND_DATETIME"
POINT_IN_TIME_RECENCY_DAYS = 7
POINT_IN_TIME_RECENCY_SECONDS = POINT_IN_TIME_RECENCY_DAYS * 24 * 3600


class SnapshotsTableValidationService(
    BaseTableValidationService[
        SnapshotsTableModel, SnapshotsTableCreate, SnapshotsTableServiceUpdate
    ]
):
    """
    SnapshotsTableValidationService class
    """

    @classmethod
    def _get_compute_column_statistics_columns(cls, table_model: SnapshotsTableModel) -> list[str]:
        return [table_model.snapshot_datetime_column]

    async def _validate_table(
        self,
        session: BaseSession,
        table_model: SnapshotsTableModel,
        num_records: int = 10,
    ) -> None:
        # Validate table is a valid snapshots table: check uniqueness for a given snapshot id and
        # snapshot datetime
        recent_point_in_time = session.adapter.subtract_seconds(
            session.adapter.current_timestamp(), POINT_IN_TIME_RECENCY_SECONDS
        )
        adjusted_snapshot_date = apply_snapshot_adjustment(
            datetime_expr=recent_point_in_time,
            time_interval=table_model.time_interval,
            feature_job_setting=table_model.default_feature_job_setting,
            format_string=table_model.snapshot_datetime_schema.format_string,
            offset_size=0,
            adapter=session.adapter,
        )
        source_expr = get_source_expr(source=table_model.tabular_source.table_details).where(
            expressions.EQ(
                this=quoted_identifier(table_model.snapshot_datetime_column),
                expression=adjusted_snapshot_date,
            )
        )
        invalid_rows_expr = get_duplicate_rows_per_keys(
            source_expr=source_expr,
            key_columns=[
                table_model.series_id_column,
                table_model.snapshot_datetime_column,
            ],
            exclude_null_column=table_model.series_id_column,
            count_output_column_name=DUPLICATE_COUNT_RESULT_NAME,
            num_records_to_retrieve=num_records,
        )
        df_invalid_rows = await session.execute_query_long_running(
            sql_to_string(invalid_rows_expr, session.source_type)
        )
        if df_invalid_rows is not None and not df_invalid_rows.empty:
            raise TableValidationError(
                f"Table {table_model.name} is not a valid snapshots table. "
                "The following snapshot ID column and snapshot datetime column pairs are not unique: "
                f"{df_invalid_rows.to_dict(orient='records')}"
            )
