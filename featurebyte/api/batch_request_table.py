"""
BatchRequestTable class
"""
from __future__ import annotations

from featurebyte.api.api_object import ApiObject, ForeignKeyMapping
from featurebyte.api.feature_store import FeatureStore
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.schema.batch_request_table import BatchRequestTableListRecord


class BatchRequestTable(BatchRequestTableModel, ApiObject):
    """
    BatchRequestTable class
    """

    _route = "/batch_request_table"
    _list_schema = BatchRequestTableListRecord
    _get_schema = BatchRequestTableModel
    _list_fields = [
        "name",
        "type",
        "feature_store_name",
        "created_at",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("feature_store_id", FeatureStore, "feature_store_name"),
    ]
