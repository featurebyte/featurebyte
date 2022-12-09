"""
DimensionData class
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from bson.objectid import ObjectId
from typeguard import typechecked

from featurebyte.api.data import DataApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TableDataType
from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.schema.dimension_data import DimensionDataCreate, DimensionDataUpdate


class DimensionData(DimensionDataModel, DataApiObject):
    """
    DimensionData class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Data"], proxy_class="featurebyte.DimensionData")

    # class variables
    _route = "/dimension_data"
    _update_schema_class = DimensionDataUpdate
    _create_schema_class = DimensionDataCreate

    def _get_create_payload(self) -> dict[str, Any]:
        data = DimensionDataCreate(**self.json_dict())
        return data.json_dict()

    @classmethod
    def _get_other_input_node_parameters(cls, values: dict[str, Any]) -> dict[str, Any]:
        # the key `_id` is used during deserialization, the key `id` is used during setattr
        return {
            "type": TableDataType.DIMENSION_DATA,
            "id": values.get("_id", values.get("id")),
        }

    @classmethod
    @typechecked
    def from_tabular_source(
        cls,
        tabular_source: DatabaseTable,
        name: str,
        dimension_data_id_column: str,
        record_creation_date_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> DimensionData:
        """
        Create DimensionData object from tabular source

        Parameters
        ----------
        tabular_source: DatabaseTable
            DatabaseTable object constructed from FeatureStore
        name: str
            Event data name
        dimension_data_id_column: str
            Dimension data ID column from the given tabular source
        record_creation_date_column: str
            Record creation datetime column from the given tabular source
        _id: Optional[ObjectId]
            Identity value for constructed object

        Returns
        -------
        DimensionData
        """
        return super().create(
            tabular_source=tabular_source,
            name=name,
            record_creation_date_column=record_creation_date_column,
            _id=_id,
            dimension_data_id_column=dimension_data_id_column,
        )

    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Provide baisc info for the dimension data.

        Parameters
        ----------
        verbose: bool
            This is a no-op for now. This will be used when we add more functionality to this funciton.

        Returns
        -------
        Dict[str, Any]
        """
        return {
            "name": self.name,
            "record_creation_date_column": self.record_creation_date_column,
            "updated_at": self.updated_at,
            "status": self.status,
            "entities": self.entity_ids,
            "tabular_source": self.tabular_source,
            "dimension_data_id_column": self.dimension_data_id_column,
        }
