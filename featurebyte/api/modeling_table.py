"""
ModelingTable class
"""
from __future__ import annotations

from featurebyte.api.api_object import ApiObject, ForeignKeyMapping
from featurebyte.api.feature_store import FeatureStore
from featurebyte.models.modeling_table import ModelingTableModel
from featurebyte.schema.modeling_table import ModelingTableListRecord


class ModelingTable(ModelingTableModel, ApiObject):
    """
    ModelingTable class
    """

    _route = "/modeling_table"
    _list_schema = ModelingTableListRecord
    _get_schema = ModelingTableModel
    _list_fields = [
        "id",
        "created_at",
        "name",
        "feature_store_name",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("feature_store_id", FeatureStore, "feature_store_name"),
    ]
