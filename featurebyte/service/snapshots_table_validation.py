"""
SnapshotsTableValidationService class
"""

from __future__ import annotations

from featurebyte.exception import TableValidationError
from featurebyte.models.snapshots_table import SnapshotsTableModel
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.query_graph.sql.validation_helper import get_duplicate_rows_per_keys
from featurebyte.schema.snapshots_table import SnapshotsTableCreate, SnapshotsTableServiceUpdate
from featurebyte.service.base_table_validation import BaseTableValidationService
from featurebyte.session.base import BaseSession

DUPLICATE_COUNT_RESULT_NAME = "COUNT_PER_SNAPSHOT_ID_AND_DATETIME"


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
        invalid_rows_expr = get_duplicate_rows_per_keys(
            key_columns=[
                table_model.snapshot_id_column,
                table_model.snapshot_datetime_column,
            ],
            exclude_null_column=table_model.snapshot_id_column,
            table_details=table_model.tabular_source.table_details,
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
