"""
SnapshotsTableValidationService class
"""

from __future__ import annotations

from featurebyte.models.snapshots_table import SnapshotsTableModel
from featurebyte.schema.snapshots_table import SnapshotsTableCreate, SnapshotsTableServiceUpdate
from featurebyte.service.base_table_validation import BaseTableValidationService


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
