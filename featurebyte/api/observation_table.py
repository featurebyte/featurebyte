"""
ObservationTable class
"""
from __future__ import annotations

from featurebyte.api.api_object import ApiObject, ForeignKeyMapping
from featurebyte.api.feature_store import FeatureStore
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.schema.observation_table import ObservationTableListRecord


class ObservationTable(ObservationTableModel, ApiObject):
    """
    ObservationTable class
    """

    _route = "/observation_table"
    _list_schema = ObservationTableListRecord
    _get_schema = ObservationTableModel
    _list_fields = [
        "id",
        "created_at",
        "name",
        "feature_store_name",
        "type",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("feature_store_id", FeatureStore, "feature_store_name"),
    ]
