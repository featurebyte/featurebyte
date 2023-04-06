"""
ObservationTable class
"""
from __future__ import annotations

from featurebyte.api.api_object import ApiObject
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
        "database_name",
        "schema_name",
        "table_name",
    ]
