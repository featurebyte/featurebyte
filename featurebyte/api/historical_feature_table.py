"""
ModelingTable class
"""
from __future__ import annotations

from featurebyte.api.api_object import ApiObject, ForeignKeyMapping
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.observation_table import ObservationTable
from featurebyte.models.modeling_table import HistoricalFeatureTableModel
from featurebyte.schema.modeling_table import ModelingTableListRecord


class HistoricalFeatureTable(HistoricalFeatureTableModel, ApiObject):
    """
    ModelingTable class
    """

    _route = "/modeling_table"
    _list_schema = ModelingTableListRecord
    _get_schema = HistoricalFeatureTableModel
    _list_fields = [
        "name",
        "feature_store_name",
        "observation_table_name",
        "created_at",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("feature_store_id", FeatureStore, "feature_store_name"),
        ForeignKeyMapping("observation_table_id", ObservationTable, "observation_table_name"),
    ]
